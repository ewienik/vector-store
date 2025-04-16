/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::Connectivity;
use crate::Dimensions;
use crate::Embeddings;
use crate::ExpansionAdd;
use crate::ExpansionSearch;
use crate::IndexId;
use crate::Limit;
use crate::PrimaryKey;
use crate::index::actor::AnnR;
use crate::index::actor::Index;
use crate::index::actor::SizeR;
use anyhow::anyhow;
use bimap::BiMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;
use tracing::error;
use tracing::trace;
use usearch::IndexOptions;
use usearch::ScalarKind;

// Initial and incremental number for the index vectors reservation.
// The value was taken for initial benchmarks (size similar to benchmark size)
const RESERVE_INCREMENT: usize = 1000000;

// When free space for index vectors drops below this, will reserve more space
// The ratio was taken for initial benchmarks
const RESERVE_THRESHOLD: usize = RESERVE_INCREMENT / 3;

#[derive(
    Copy,
    Clone,
    Debug,
    PartialEq,
    Eq,
    Hash,
    derive_more::From,
    derive_more::AsRef,
    derive_more::Display,
)]
/// Key for index embeddings
struct Key(u64);

pub(crate) fn new(
    id: IndexId,
    dimensions: Dimensions,
    connectivity: Connectivity,
    expansion_add: ExpansionAdd,
    expansion_search: ExpansionSearch,
) -> anyhow::Result<mpsc::Sender<Index>> {
    let options = IndexOptions {
        dimensions: dimensions.0.get(),
        connectivity: connectivity.0,
        expansion_add: expansion_add.0,
        expansion_search: expansion_search.0,
        quantization: ScalarKind::F32,
        ..Default::default()
    };

    let idx = Arc::new(usearch::Index::new(&options)?);
    idx.reserve(RESERVE_INCREMENT)?;

    // TODO: The value of channel size was taken from initial benchmarks. Needs more testing
    const CHANNEL_SIZE: usize = 10;
    let (tx, mut rx) = mpsc::channel(CHANNEL_SIZE);

    tokio::spawn(
        async move {
            debug!("starting");
            let keys = Arc::new(RwLock::new(BiMap::new()));
            let atomic_key = Arc::new(AtomicU64::new(0));

            let idx_lock = Arc::new(RwLock::new(()));

            let semaphore = Arc::new(Semaphore::new(rayon::current_num_threads() * 2));

            while let Some(msg) = rx.recv().await {
                let permit = Arc::clone(&semaphore).acquire_owned().await.unwrap();
                tokio::spawn({
                    let idx = Arc::clone(&idx);
                    let idx_lock = Arc::clone(&idx_lock);
                    let keys = Arc::clone(&keys);
                    let atomic_key = Arc::clone(&atomic_key);
                    async move {
                        process(msg, dimensions, idx, idx_lock, keys, atomic_key).await;
                        drop(permit);
                    }
                });
            }
            debug!("finished");
        }
        .instrument(debug_span!("usearch", "{id}")),
    );

    Ok(tx)
}

async fn process(
    msg: Index,
    dimensions: Dimensions,
    idx: Arc<usearch::Index>,
    idx_lock: Arc<RwLock<()>>,
    keys: Arc<RwLock<BiMap<PrimaryKey, Key>>>,
    atomic_key: Arc<AtomicU64>,
) {
    match msg {
        Index::Add {
            primary_key,
            embeddings,
        } => {
            add(idx, idx_lock, keys, atomic_key, primary_key, embeddings).await;
        }

        Index::Ann {
            embeddings,
            limit,
            tx,
        } => {
            ann(idx, tx, keys, embeddings, dimensions, limit).await;
        }

        Index::Size { tx } => {
            size(idx, idx_lock, tx);
        }
    }
}

async fn add(
    idx: Arc<usearch::Index>,
    idx_lock: Arc<RwLock<()>>,
    keys: Arc<RwLock<BiMap<PrimaryKey, Key>>>,
    atomic_key: Arc<AtomicU64>,
    primary_key: PrimaryKey,
    embeddings: Embeddings,
) {
    let key = atomic_key.fetch_add(1, Ordering::Relaxed).into();

    if keys
        .write()
        .unwrap()
        .insert_no_overwrite(primary_key.clone(), key)
        .is_err()
    {
        debug!("add: primary_key already exists: {primary_key:?}");
        return;
    }

    let (tx, rx) = oneshot::channel();
    rayon::spawn(move || {
        let capacity = idx.capacity();
        if capacity - idx.size() < RESERVE_THRESHOLD {
            // free space below threshold, reserve more space
            let _lock = idx_lock.write().unwrap();
            let capacity = capacity + RESERVE_INCREMENT;
            debug!("index::add: trying to reserve {capacity}");
            if let Err(err) = idx.reserve(capacity) {
                error!("unable to reserve index capacity for {capacity} in usearch: {err}");
                _ = tx.send(false);
                return;
            }
            debug!("add: finished reserve {capacity}");
        }

        let _lock = idx_lock.read().unwrap();
        if let Err(err) = idx.add(key.0, &embeddings.0) {
            debug!("add: unable to add embeddings for key {key}: {err}");
            _ = tx.send(false);
            return;
        };
        _ = tx.send(true);
    });
    if let Ok(false) = rx.await {
        keys.write().unwrap().remove_by_right(&key);
    }
}

async fn ann(
    idx: Arc<usearch::Index>,
    tx_ann: oneshot::Sender<AnnR>,
    keys: Arc<RwLock<BiMap<PrimaryKey, Key>>>,
    embeddings: Embeddings,
    dimensions: Dimensions,
    limit: Limit,
) {
    let Some(embeddings_len) = NonZeroUsize::new(embeddings.0.len()) else {
        tx_ann
            .send(Err(anyhow!("index::ann: embeddings dimensions == 0")))
            .unwrap_or_else(|_| trace!("ann: unable to send error response (zero dimensions)"));
        return;
    };
    if embeddings_len != dimensions.0 {
        tx_ann
            .send(Err(anyhow!(
                "ann: wrong embeddings dimensions: {embeddings_len} != {dimensions}",
            )))
            .unwrap_or_else(|_| trace!("ann: unable to send error response (wrong dimensions)"));
        return;
    }

    let (tx, rx) = oneshot::channel();
    rayon::spawn(move || {
        _ = tx.send(idx.search(&embeddings.0, limit.0.get()));
    });

    tx_ann
        .send(
            rx.await
                .map_err(|err| anyhow!("ann: unable to recv matches: {err}"))
                .and_then(|matches| matches.map_err(|err| anyhow!("ann: search failed: {err}")))
                .and_then(|matches| {
                    let primary_keys = {
                        let keys = keys.read().unwrap();
                        matches
                            .keys
                            .into_iter()
                            .map(|key| {
                                keys.get_by_right(&key.into())
                                    .cloned()
                                    .ok_or(anyhow!("not defined primary key column {key}"))
                            })
                            .collect::<anyhow::Result<_>>()?
                    };
                    let distances = matches
                        .distances
                        .into_iter()
                        .map(|value| value.into())
                        .collect();
                    Ok((primary_keys, distances))
                }),
        )
        .unwrap_or_else(|_| trace!("ann: unable to send response"));
}

fn size(idx: Arc<usearch::Index>, idx_lock: Arc<RwLock<()>>, tx: oneshot::Sender<SizeR>) {
    tx.send(Ok({
        let _lock = idx_lock.read().unwrap();
        idx.size()
    }))
    .unwrap_or_else(|_| trace!("size: unable to send response"));
}
