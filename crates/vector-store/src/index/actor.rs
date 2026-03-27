/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::AsyncInProgress;
use crate::Distance;
use crate::Filter;
use crate::IndexKey;
use crate::Limit;
use crate::PrimaryKey;
use crate::Vector;
use crate::table::PartitionId;
use crate::table::PrimaryId;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

pub(crate) type AnnR = anyhow::Result<(Vec<PrimaryKey>, Vec<Distance>)>;
pub(crate) type CountR = anyhow::Result<usize>;

pub enum IndexModify {
    AddVector {
        partition_id: PartitionId,
        primary_id: PrimaryId,
        embedding: Vector,
        in_progress: Option<AsyncInProgress>,
    },
    RemoveVector {
        partition_id: PartitionId,
        primary_id: PrimaryId,
        in_progress: Option<AsyncInProgress>,
    },
    RemovePartition {
        partition_id: PartitionId,
    },
}

pub enum IndexSearch {
    Ann {
        index_key: IndexKey,
        embedding: Vector,
        limit: Limit,
        tx: oneshot::Sender<AnnR>,
    },
    FilteredAnn {
        index_key: IndexKey,
        embedding: Vector,
        filter: Filter,
        limit: Limit,
        tx: oneshot::Sender<AnnR>,
    },
    Count {
        index_key: IndexKey,
        tx: oneshot::Sender<CountR>,
    },
}

pub(crate) enum Index {
    Modify(IndexModify),
    Search(IndexSearch),
}

pub(crate) trait IndexModifyExt {
    async fn add_vector(
        &self,
        partition_id: PartitionId,
        primary_id: PrimaryId,
        embedding: Vector,
        in_progress: Option<AsyncInProgress>,
    );
    async fn remove_vector(
        &self,
        partition_id: PartitionId,
        primary_id: PrimaryId,
        in_progress: Option<AsyncInProgress>,
    );
    async fn remove_partition(&self, partition_id: PartitionId);
}

pub(crate) trait IndexSearchExt {
    async fn ann(&self, index_key: IndexKey, embedding: Vector, limit: Limit) -> AnnR;
    async fn filtered_ann(
        &self,
        index_key: IndexKey,
        embedding: Vector,
        filter: Filter,
        limit: Limit,
    ) -> AnnR;
    async fn count(&self, index_key: IndexKey) -> CountR;
}

impl IndexModifyExt for mpsc::Sender<IndexModify> {
    async fn add_vector(
        &self,
        partition_id: PartitionId,
        primary_id: PrimaryId,
        embedding: Vector,
        in_progress: Option<AsyncInProgress>,
    ) {
        self.send(IndexModify::AddVector {
            partition_id,
            primary_id,
            embedding,
            in_progress,
        })
        .await
        .expect("internal actor should receive request");
    }

    async fn remove_vector(
        &self,
        partition_id: PartitionId,
        primary_id: PrimaryId,
        in_progress: Option<AsyncInProgress>,
    ) {
        self.send(IndexModify::RemoveVector {
            partition_id,
            primary_id,
            in_progress,
        })
        .await
        .expect("internal actor should receive request");
    }

    async fn remove_partition(&self, partition_id: PartitionId) {
        self.send(IndexModify::RemovePartition { partition_id })
            .await
            .expect("internal actor should receive request");
    }
}

impl IndexSearchExt for mpsc::Sender<IndexSearch> {
    async fn ann(&self, index_key: IndexKey, embedding: Vector, limit: Limit) -> AnnR {
        let (tx, rx) = oneshot::channel();
        self.send(IndexSearch::Ann {
            index_key,
            embedding,
            limit,
            tx,
        })
        .await?;
        rx.await?
    }

    async fn filtered_ann(
        &self,
        index_key: IndexKey,
        embedding: Vector,
        filter: Filter,
        limit: Limit,
    ) -> AnnR {
        let (tx, rx) = oneshot::channel();
        self.send(IndexSearch::FilteredAnn {
            index_key,
            embedding,
            filter,
            limit,
            tx,
        })
        .await?;
        rx.await?
    }

    async fn count(&self, index_key: IndexKey) -> CountR {
        let (tx, rx) = oneshot::channel();
        self.send(IndexSearch::Count { index_key, tx }).await?;
        rx.await?
    }
}
