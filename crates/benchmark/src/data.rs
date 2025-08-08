/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_array::types::Int64Type;
use futures::Stream;
use futures::StreamExt;
use futures::stream;
use itertools::Itertools;
use parquet::arrow::ProjectionMask;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use std::collections::HashMap;
use std::path::Path;
use tokio::fs::File;
use tracing::info;

const PATH_SHUFFLE_TRAIN: &str = "shuffle_train.parquet";
const PATH_TEST: &str = "test.parquet";
const PATH_NEIGHBORS: &str = "neighbors.parquet";
const ID: &str = "id";
const EMB: &str = "emb";
const NEIGHBORS_ID: &str = "neighbors_id";

pub(crate) async fn dimension(path: &Path) -> usize {
    let builder =
        ParquetRecordBatchStreamBuilder::new(File::open(path.join(PATH_TEST)).await.unwrap())
            .await
            .unwrap();
    let mask = ProjectionMask::columns(builder.parquet_schema(), [EMB].into_iter());
    let mut stream = builder.with_projection(mask).build().unwrap();
    let batch = stream.next().await.unwrap().unwrap();
    let data = batch.column_by_name(EMB).unwrap().as_list::<i64>().value(0);
    let dim = data.as_primitive::<Float64Type>().len();
    info!("Found dimension {dim} for dataset at {path:?}");
    dim
}

pub(crate) async fn count(path: &Path) -> usize {
    let builder = ParquetRecordBatchStreamBuilder::new(
        File::open(path.join(PATH_SHUFFLE_TRAIN)).await.unwrap(),
    )
    .await
    .unwrap();
    let mask = ProjectionMask::columns(builder.parquet_schema(), [ID].into_iter());
    let stream = builder.with_projection(mask).build().unwrap();
    let size = stream
        .map(move |batch| batch.unwrap())
        .map(move |batch| {
            batch
                .column_by_name(ID)
                .unwrap()
                .as_primitive::<Int64Type>()
                .len()
        })
        .fold(0usize, |acc, len| async move { acc + len })
        .await;
    info!("Found size {size} for dataset at {path:?}");
    size
}

pub(crate) async fn vector_stream(path: &Path) -> impl Stream<Item = (i64, Vec<f32>)> {
    let stream = ParquetRecordBatchStreamBuilder::new(
        File::open(path.join(PATH_SHUFFLE_TRAIN)).await.unwrap(),
    )
    .await
    .unwrap()
    .build()
    .unwrap();
    stream
        .map(move |batch| batch.unwrap())
        .map(move |batch| {
            let ids = batch
                .column_by_name(ID)
                .unwrap()
                .as_primitive::<Int64Type>();
            let ids = ids.iter().map(|id| id.unwrap());

            let embs = batch.column_by_name(EMB).unwrap().as_list::<i32>();
            let embs = embs.iter().map(|emb| emb.unwrap()).map(|emb| {
                let emb = emb.as_primitive::<Float64Type>();
                emb.iter().map(|v| v.unwrap() as f32).collect_vec()
            });

            let ids_embs = ids.zip(embs).collect_vec();
            stream::iter(ids_embs)
        })
        .flatten()
}

pub(crate) struct Query {
    pub(crate) query: Vec<f32>,
    pub(crate) neighbors: Vec<i64>,
}

pub(crate) async fn queries(path: &Path, limit: usize) -> Vec<Query> {
    let stream =
        ParquetRecordBatchStreamBuilder::new(File::open(path.join(PATH_TEST)).await.unwrap())
            .await
            .unwrap()
            .build()
            .unwrap();
    let ids_queries = stream
        .map(move |batch| batch.unwrap())
        .map(move |batch| {
            let ids = batch
                .column_by_name(ID)
                .unwrap()
                .as_primitive::<Int64Type>();
            let ids = ids.iter().map(|id| id.unwrap());

            let queries = batch.column_by_name(EMB).unwrap().as_list::<i64>();
            let queries = queries.iter().map(|query| query.unwrap()).map(|query| {
                let query = query.as_primitive::<Float64Type>();
                query.iter().map(|v| v.unwrap() as f32).collect_vec()
            });

            let ids_queries = ids.zip(queries).collect_vec();
            stream::iter(ids_queries)
        })
        .flatten()
        .collect::<HashMap<_, _>>()
        .await;

    let stream =
        ParquetRecordBatchStreamBuilder::new(File::open(path.join(PATH_NEIGHBORS)).await.unwrap())
            .await
            .unwrap()
            .build()
            .unwrap();
    let ids_neighbors = stream
        .map(move |batch| batch.unwrap())
        .map(move |batch| {
            let ids = batch
                .column_by_name(ID)
                .unwrap()
                .as_primitive::<Int64Type>();
            let ids = ids.iter().map(|id| id.unwrap());

            let neighbors = batch.column_by_name(NEIGHBORS_ID).unwrap().as_list::<i64>();
            let neighbors = neighbors
                .iter()
                .map(|neighbor| neighbor.unwrap())
                .map(|neighbor| {
                    let neighbor = neighbor.as_primitive::<Int64Type>();
                    neighbor
                        .iter()
                        .take(limit)
                        .map(|v| v.unwrap())
                        .collect_vec()
                });

            let ids_neighbors = ids.zip(neighbors).collect_vec();
            stream::iter(ids_neighbors)
        })
        .flatten()
        .collect::<HashMap<_, _>>()
        .await;

    ids_queries
        .into_iter()
        .filter(|(id, _)| ids_neighbors.contains_key(id))
        .map(|(id, query)| {
            let neighbors = ids_neighbors.get(&id).unwrap().clone();
            Query { query, neighbors }
        })
        .collect()
}
