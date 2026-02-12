/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::AsyncInProgress;
use crate::Distance;
use crate::Filter;
use crate::Limit;
use crate::PrimaryKey;
use crate::Vector;
use crate::table::RowId;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

pub(crate) type AnnR = anyhow::Result<(Vec<PrimaryKey>, Vec<Distance>)>;
pub(crate) type CountR = anyhow::Result<usize>;

pub enum Index {
    Add {
        row_id: RowId,
        embedding: Vector,
        in_progress: Option<AsyncInProgress>,
    },
    Remove {
        row_id: RowId,
        in_progress: Option<AsyncInProgress>,
    },
    Ann {
        embedding: Vector,
        limit: Limit,
        tx: oneshot::Sender<AnnR>,
    },
    FilteredAnn {
        embedding: Vector,
        filter: Filter,
        limit: Limit,
        tx: oneshot::Sender<AnnR>,
    },
    Count {
        tx: oneshot::Sender<CountR>,
    },
}

pub(crate) trait IndexExt {
    async fn add(&self, row_id: RowId, embedding: Vector, in_progress: Option<AsyncInProgress>);
    async fn remove(&self, row_id: RowId, in_progress: Option<AsyncInProgress>);
    async fn ann(&self, embedding: Vector, limit: Limit) -> AnnR;
    async fn filtered_ann(&self, embedding: Vector, filter: Filter, limit: Limit) -> AnnR;
    async fn count(&self) -> CountR;
}

impl IndexExt for mpsc::Sender<Index> {
    async fn add(&self, row_id: RowId, embedding: Vector, in_progress: Option<AsyncInProgress>) {
        self.send(Index::Add {
            row_id,
            embedding,
            in_progress,
        })
        .await
        .expect("internal actor should receive request");
    }

    async fn remove(&self, row_id: RowId, in_progress: Option<AsyncInProgress>) {
        self.send(Index::Remove {
            row_id,
            in_progress,
        })
        .await
        .expect("internal actor should receive request");
    }

    async fn ann(&self, embedding: Vector, limit: Limit) -> AnnR {
        let (tx, rx) = oneshot::channel();
        self.send(Index::Ann {
            embedding,
            limit,
            tx,
        })
        .await?;
        rx.await?
    }

    async fn filtered_ann(&self, embedding: Vector, filter: Filter, limit: Limit) -> AnnR {
        let (tx, rx) = oneshot::channel();
        self.send(Index::FilteredAnn {
            embedding,
            filter,
            limit,
            tx,
        })
        .await?;
        rx.await?
    }

    async fn count(&self) -> CountR {
        let (tx, rx) = oneshot::channel();
        self.send(Index::Count { tx }).await?;
        rx.await?
    }
}
