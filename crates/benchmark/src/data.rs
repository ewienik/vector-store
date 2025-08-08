/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use arrow_array::cast::AsArray;
use arrow_array::types::Float32Type;
use arrow_array::types::Float64Type;
use arrow_array::types::Int64Type;
use futures::Stream;
use futures::StreamExt;
use futures::stream;
use itertools::Itertools;
use parquet::arrow::ProjectionMask;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use std::path::Path;
use std::path::PathBuf;
use tokio::fs;
use tokio::fs::File;
use tokio_stream::wrappers::ReadDirStream;
use tracing::info;

const PATH_PARQUET: &str = "parquet";
const PATH_TRAIN: &str = "train";
const PATH_TEST: &str = "test.parquet";
const ID: &str = "id";
const EMB: &str = "emb";

pub(crate) async fn dimension(path: &Path) -> usize {
    let builder =
        ParquetRecordBatchStreamBuilder::new(File::open(path.join(PATH_TEST)).await.unwrap())
            .await
            .unwrap();
    let mask = ProjectionMask::columns(builder.parquet_schema(), [EMB].into_iter());
    let mut stream = builder.with_projection(mask).build().unwrap();
    let batch = stream.next().await.unwrap().unwrap();
    let data = batch.column_by_name(EMB).unwrap().as_list::<i64>().value(0);
    let dim = if let Some(data) = data.as_primitive_opt::<Float64Type>() {
        data.len()
    } else {
        data.as_primitive::<Float32Type>().len()
    };
    info!("Found dimension {dim} for dataset at {path:?}");
    dim
}

async fn train_files(path: &Path) -> impl Stream<Item = PathBuf> {
    let readdirs = if let Ok(readdir) = fs::read_dir(path).await {
        vec![ReadDirStream::new(readdir)]
    } else {
        vec![]
    };
    stream::iter(readdirs)
        .flatten()
        .filter_map(|res| async move { res.ok() })
        .filter_map(|entry| async move {
            entry
                .file_type()
                .await
                .map(|ft| ft.is_file())
                .unwrap_or(false)
                .then_some(entry.path())
        })
        .filter_map(|path| async move {
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                return None;
            };
            let Some(ext) = path.extension().and_then(|ext| ext.to_str()) else {
                return None;
            };
            (name.contains(PATH_TRAIN) && ext == PATH_PARQUET).then_some(path)
        })
}

pub(crate) async fn count(path: &Path) -> usize {
    let size = train_files(path)
        .await
        .then(|path| async move {
            let builder = ParquetRecordBatchStreamBuilder::new(File::open(path).await.unwrap())
                .await
                .unwrap();
            let mask = ProjectionMask::columns(builder.parquet_schema(), [ID].into_iter());
            builder.with_projection(mask).build().unwrap()
        })
        .flatten()
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
    train_files(path)
        .await
        .then(|path| async move {
            ParquetRecordBatchStreamBuilder::new(File::open(path).await.unwrap())
                .await
                .unwrap()
                .build()
                .unwrap()
        })
        .flatten()
        .map(move |batch| batch.unwrap())
        .map(move |batch| {
            let ids = batch
                .column_by_name(ID)
                .unwrap()
                .as_primitive::<Int64Type>();
            let ids = ids.iter().map(|id| id.unwrap());

            let ids_embs =
                if let Some(embs) = batch.column_by_name(EMB).unwrap().as_list_opt::<i32>() {
                    let embs = embs.iter().map(|emb| emb.unwrap()).map(|emb| {
                        if let Some(emb) = emb.as_primitive_opt::<Float64Type>() {
                            emb.iter().map(|v| v.unwrap() as f32).collect_vec()
                        } else {
                            let emb = emb.as_primitive::<Float32Type>();
                            emb.iter().map(|v| v.unwrap()).collect_vec()
                        }
                    });
                    ids.zip(embs).collect_vec()
                } else {
                    let embs = batch.column_by_name(EMB).unwrap().as_list::<i64>();
                    let embs = embs.iter().map(|emb| emb.unwrap()).map(|emb| {
                        if let Some(emb) = emb.as_primitive_opt::<Float64Type>() {
                            emb.iter().map(|v| v.unwrap() as f32).collect_vec()
                        } else {
                            let emb = emb.as_primitive::<Float32Type>();
                            emb.iter().map(|v| v.unwrap()).collect_vec()
                        }
                    });
                    ids.zip(embs).collect_vec()
                };

            stream::iter(ids_embs)
        })
        .flatten()
}
