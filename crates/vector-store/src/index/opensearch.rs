/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::Connectivity;
use crate::Dimensions;
use crate::Distance;
use crate::ExpansionAdd;
use crate::ExpansionSearch;
use crate::IndexFactory;
use crate::IndexId;
use crate::Limit;
use crate::SpaceType;
use crate::Vector;
use crate::index::actor::Index;
use crate::index::factory::IndexConfiguration;
use crate::index::validator;
use crate::memory::Memory;
use crate::table::RowId;
use crate::table::Table;
use crate::table::TableSearch;
use anyhow::anyhow;
use opensearch::DeleteParts;
use opensearch::IndexParts;
use opensearch::OpenSearch;
use opensearch::http::Url;
use opensearch::http::transport::SingleNodeConnectionPool;
use opensearch::http::transport::TransportBuilder;
use opensearch::indices::IndicesCreateParts;
use serde_json::Value;
use serde_json::json;
use std::fmt::Display;
use std::sync::Arc;
use std::sync::RwLock;
use tokio::sync::Notify;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;
use tracing::error;
use tracing::info;
use tracing::trace;
use tracing::warn;

use super::actor::AnnR;
use super::actor::CountR;

pub struct OpenSearchIndexFactory {
    client: Arc<OpenSearch>,
    shutdown_notify: Arc<Notify>,
}

impl Drop for OpenSearchIndexFactory {
    fn drop(&mut self) {
        self.shutdown_notify.notify_one();
    }
}

impl OpenSearchIndexFactory {
    fn create_opensearch_client(addr: &str) -> anyhow::Result<OpenSearch> {
        let address = Url::parse(addr)?;
        let conn_pool = SingleNodeConnectionPool::new(address);
        let transport = TransportBuilder::new(conn_pool).disable_proxy().build()?;
        let client = OpenSearch::new(transport);
        Ok(client)
    }
}

impl Display for SpaceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Euclidean => write!(f, "l2"),
            Self::Cosine => write!(f, "cosinesimil"),
            Self::DotProduct => write!(f, "innerproduct"),
            Self::Hamming => unimplemented!("Hamming distance is not supported"),
        }
    }
}

impl IndexFactory for OpenSearchIndexFactory {
    fn create_index(
        &self,
        index: IndexConfiguration,
        table: Arc<RwLock<Table>>,
        _: mpsc::Sender<Memory>,
    ) -> anyhow::Result<mpsc::Sender<Index>> {
        new(
            index.id,
            index.dimensions,
            index.connectivity,
            index.expansion_add,
            index.expansion_search,
            index.space_type,
            table,
            self.client.clone(),
        )
    }

    fn index_engine_version(&self) -> String {
        "opensearch".into()
    }
}

pub fn new_opensearch(
    addr: &str,
    config_rx: watch::Receiver<Arc<crate::Config>>,
) -> Result<OpenSearchIndexFactory, anyhow::Error> {
    let initial_addr = addr.to_string();
    let shutdown_notify = Arc::new(Notify::new());
    let factory = OpenSearchIndexFactory {
        client: Arc::new(OpenSearchIndexFactory::create_opensearch_client(addr)?),
        shutdown_notify: shutdown_notify.clone(),
    };

    // Spawn monitoring task
    tokio::spawn(async move {
        let mut rx = config_rx;
        loop {
            tokio::select! {
                result = rx.changed() => {
                    if result.is_err() {
                        break;
                    }
                    let new_config = rx.borrow();
                    let new_addr = new_config.opensearch_addr.as_deref();

                    if Some(initial_addr.as_str()) != new_addr {
                        let new_display = new_addr.unwrap_or("None (using Usearch)");
                        warn!(
                            "OpenSearch address changed: {initial_addr} -> {new_display}. Restart required."
                        );
                    }
                }
                _ = shutdown_notify.notified() => {
                    break;
                }
            }
        }
    });

    Ok(factory)
}

async fn create_index(
    id: &IndexId,
    dimensions: Dimensions,
    connectivity: Connectivity,
    expansion_add: ExpansionAdd,
    expansion_search: ExpansionSearch,
    space_type: SpaceType,
    client: Arc<OpenSearch>,
) -> Result<opensearch::http::response::Response, ()> {
    let response: Result<opensearch::http::response::Response, ()> = client
        .indices()
        .create(IndicesCreateParts::Index(&id.0))
        .body(json!({
            "settings": {
                "index.knn": true
            },
            "mappings": {
                "properties": {
                    "vector": {
                        "type": "knn_vector",
                        "dimension": dimensions.0.get(),
                        "method": {
                            "name": "hnsw",
                            "space_type": space_type.to_string(),
                            "parameters": {
                                "ef_search": if expansion_search.0 > 0 {
                                    expansion_search.0
                                } else {
                                    100
                                },
                                "ef_construction": if expansion_add.0 > 0 {
                                    expansion_add.0
                                } else {
                                    100
                                },
                                "m": if connectivity.0 > 0 {
                                    connectivity.0
                                } else {
                                    16
                                },
                            }
                        }
                    },
                }
            }
        }))
        .send()
        .await
        .map_or_else(
            Err,
            opensearch::http::response::Response::error_for_status_code,
        )
        .map_err(|err| {
            error!("engine::new: unable to create index with id {id}: {err}");
        });

    response
}

// TODO: remove allow
#[allow(clippy::too_many_arguments)]
pub fn new(
    id: IndexId,
    dimensions: Dimensions,
    connectivity: Connectivity,
    expansion_add: ExpansionAdd,
    expansion_search: ExpansionSearch,
    space_type: SpaceType,
    table: Arc<RwLock<impl TableSearch + Send + Sync + 'static>>,
    client: Arc<OpenSearch>,
) -> anyhow::Result<mpsc::Sender<Index>> {
    info!("Creating new index with id: {id}");
    // TODO: The value of channel size was taken from initial benchmarks. Needs more testing
    const CHANNEL_SIZE: usize = 10;
    let (tx, mut rx) = mpsc::channel(CHANNEL_SIZE);

    tokio::spawn({
        let cloned_id = id.clone();
        async move {
            let response = create_index(
                &id,
                dimensions,
                connectivity,
                expansion_add,
                expansion_search,
                space_type,
                client.clone(),
            )
            .await;

            if response.is_err() {
                error!("engine::new: unable to create index with id {id}");
                return;
            }

            debug!("starting");

            // This semaphore decides how many tasks are queued for an opensearch process.
            // We are currently using SingleNodeConnectionPool, so we can only have one
            // connection to the server. This means that we can only have one task at a time,
            // so we set the semaphore to 2, so we always have something in queue.
            let semaphore = Arc::new(Semaphore::new(2));

            let id = Arc::new(id);

            while let Some(msg) = rx.recv().await {
                let permit = Arc::clone(&semaphore).acquire_owned().await.unwrap();
                tokio::spawn({
                    let id = Arc::clone(&id);
                    let table = Arc::clone(&table);
                    let client = Arc::clone(&client);
                    async move {
                        process(msg, dimensions, space_type, id, table, client).await;
                        drop(permit);
                    }
                });
            }

            debug!("finished");
        }
        .instrument(debug_span!("opensearch", "{cloned_id}"))
    });

    Ok(tx)
}

async fn process(
    msg: Index,
    dimensions: Dimensions,
    space_type: SpaceType,
    index_id: Arc<IndexId>,
    table: Arc<RwLock<impl TableSearch>>,
    client: Arc<OpenSearch>,
) {
    match msg {
        Index::Add {
            row_id,
            embedding,
            in_progress: _in_progress,
        } => add(index_id, row_id, embedding, client).await,
        Index::Remove {
            row_id,
            in_progress: _in_progress,
        } => remove(index_id, row_id, client).await,
        Index::Ann {
            embedding,
            limit,
            tx,
        } => {
            ann(
                index_id, tx, embedding, dimensions, limit, space_type, table, client,
            )
            .await
        }
        Index::FilteredAnn { tx, .. } => filtered_ann(tx).await,
        Index::Count { tx } => count(index_id, tx, client).await,
    }
}

async fn add(id: Arc<IndexId>, row_id: RowId, embeddings: Vector, client: Arc<OpenSearch>) {
    _ = client
        .index(IndexParts::IndexId(&id.0, &row_id.as_ref().to_string()))
        .body(json!({
            "vector": embeddings.0,
        }))
        .send()
        .await
        .map_or_else(
            Err,
            opensearch::http::response::Response::error_for_status_code,
        )
        .map_err(|err| {
            error!("add: unable to add embedding for row_id {row_id:?}: {err}");
        });
}

async fn remove(id: Arc<IndexId>, row_id: RowId, client: Arc<OpenSearch>) {
    _ = client
        .delete(DeleteParts::IndexId(&id.0, &row_id.as_ref().to_string()))
        .send()
        .await
        .map_or_else(
            Err,
            opensearch::http::response::Response::error_for_status_code,
        )
        .map_err(|err| {
            error!("remove: unable to remove embedding for row_id {row_id:?}: {err}");
        });
}

#[allow(clippy::too_many_arguments)]
async fn ann(
    id: Arc<IndexId>,
    tx_ann: oneshot::Sender<AnnR>,
    embedding: Vector,
    dimensions: Dimensions,
    limit: Limit,
    space_type: SpaceType,
    table: Arc<RwLock<impl TableSearch>>,
    client: Arc<OpenSearch>,
) {
    if let Err(err) = validator::embedding_dimensions(&embedding, dimensions) {
        return tx_ann
            .send(Err(err))
            .unwrap_or_else(|_| trace!("ann: unable to send response"));
    }

    let response = client
        .search(opensearch::SearchParts::Index(&[&id.0]))
        .body(json!({
            "query": {
                "knn": {
                    "vector": {
                        "vector": embedding.0,
                        "k": limit.0,
                    }
                }
            }
        }))
        .send()
        .await
        .map_or_else(
            Err,
            opensearch::http::response::Response::error_for_status_code,
        )
        .map_err(|err| {
            error!("ann: unable to search for embedding: {err}");
        });

    if response.is_err() {
        _ = tx_ann.send(Err(anyhow!("ann: unable to search for embedding")));
        return;
    }

    let response_body = response.unwrap().json::<Value>().await;

    if response_body.is_err() {
        _ = tx_ann.send(Err(anyhow!("ann: unable to search for embedding")));
        return;
    }
    let response_body = response_body.unwrap();

    let hits = response_body
        .get("hits")
        .and_then(|hits| hits.get("hits"))
        .and_then(|hits| hits.as_array());

    if hits.is_none() {
        _ = tx_ann.send(Err(anyhow!("ann: unable to search for embedding")));
        return;
    }
    let hits = {
        let table = table.read().unwrap();
        hits.unwrap()
            .iter()
            .map(|hit| {
                let id = hit["_id"].as_str().unwrap();
                let score = hit["_score"].as_f64().unwrap();
                let row_id = RowId::from(id.parse::<u64>().unwrap());
                let row_id = table.primary_key(row_id).unwrap();
                (row_id.clone(), score)
            })
            .collect::<Vec<_>>()
    };

    let (keys, scores): (Vec<_>, Vec<_>) = hits.iter().cloned().unzip();
    let distances: anyhow::Result<Vec<_>> = scores
        .iter()
        .map(|score| Distance::try_from((*score as f32, space_type, Some(dimensions))))
        .collect();
    let distances = match distances {
        Ok(distances) => distances,
        Err(err) => {
            _ = tx_ann.send(Err(err));
            return;
        }
    };

    tx_ann
        .send(Ok((keys, distances)))
        .unwrap_or_else(|_| trace!("ann: unable to send response"));
}

async fn filtered_ann(tx_ann: oneshot::Sender<AnnR>) {
    _ = tx_ann.send(Err(anyhow!("Filtering not supported")));
}

async fn count(id: Arc<IndexId>, tx: oneshot::Sender<CountR>, client: Arc<OpenSearch>) {
    let response = client
        .count(opensearch::CountParts::Index(&[&id.0]))
        .send()
        .await
        .map_or_else(
            Err,
            opensearch::http::response::Response::error_for_status_code,
        )
        .map_err(|err| {
            error!("count: unable to count embeddings: {err}");
        });

    if response.is_err() {
        _ = tx.send(Ok(0));
        return;
    }

    let response_body = response.unwrap().json::<Value>().await;

    if response_body.is_err() {
        _ = tx.send(Ok(0));
        return;
    }
    let response_body = response_body.unwrap();

    let count = response_body.get("count").and_then(|count| count.as_u64());

    if count.is_none() {
        _ = tx.send(Ok(0));
        return;
    }
    let count = count.unwrap();

    _ = tx.send(Ok(count as usize));
}
