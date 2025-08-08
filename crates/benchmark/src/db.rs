/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::INDEX;
use crate::KEYSPACE;
use crate::MetricType;
use crate::Query;
use crate::TABLE;
use futures::Stream;
use futures::StreamExt;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::statement::prepared::PreparedStatement;
use std::net::SocketAddr;
use std::pin::pin;
use std::sync::Arc;
use tokio::sync::Semaphore;

const ID: &str = "id";
const VECTOR: &str = "vector";

#[derive(Clone)]
pub(crate) struct Scylla(Arc<State>);

struct State {
    session: Session,
    st_insert: PreparedStatement,
    st_search: PreparedStatement,
}

impl Scylla {
    pub(crate) async fn new(uri: SocketAddr) -> Self {
        let session = SessionBuilder::new()
            .known_node(uri.to_string())
            .build()
            .await
            .unwrap();

        let st_insert = session
            .prepare(format!(
                "INSERT INTO {KEYSPACE}.{TABLE} ({ID}, {VECTOR}) VALUES (?, ?)"
            ))
            .await
            .unwrap();

        let st_search = session
            .prepare(format!(
                "SELECT {ID} FROM {KEYSPACE}.{TABLE} ORDER BY {VECTOR} ANN OF ? LIMIT ?"
            ))
            .await
            .unwrap();

        Self(Arc::new(State {
            session,
            st_insert,
            st_search,
        }))
    }

    pub(crate) async fn create_table(&self, dimension: usize) {
        self.0.session
        .query_unpaged(
            format!(
                "
                CREATE KEYSPACE {KEYSPACE}
                WITH replication = {{'class': 'NetworkTopologyStrategy' , 'replication_factor': '3'}}
                AND tablets = {{'enabled': 'false'}}
                "
            ),
            &[],
        )
        .await
        .unwrap();

        self.0
            .session
            .query_unpaged(
                format!(
                    "
                CREATE TABLE {KEYSPACE}.{TABLE} (
                    {ID} bigint PRIMARY KEY,
                    {VECTOR} vector<float, {dimension}>,
                ) WITH CDC = {{'enabled' : true}}
                ",
                ),
                &[],
            )
            .await
            .unwrap();
    }

    pub(crate) async fn drop_table(&self) {
        self.0
            .session
            .query_unpaged(format!("DROP KEYSPACE IF EXISTS {KEYSPACE}"), &[])
            .await
            .unwrap();
    }

    pub(crate) async fn create_index(
        &self,
        metric_type: MetricType,
        m: usize,
        ef_construction: usize,
        ef_search: usize,
    ) {
        let metric_type = match metric_type {
            MetricType::Euclidean => "EUCLIDEAN",
            MetricType::Cosine => "COSINE",
            MetricType::DotProduct => "DOT_PRODUCT",
        };
        self.0
            .session
            .query_unpaged(
                format!(
                    "
                CREATE CUSTOM INDEX {INDEX} ON {KEYSPACE}.{TABLE} ({VECTOR})
                USING 'vector_index' WITH OPTIONS = {{
                    'similarity_function': '{metric_type}',
                    'maximum_node_connections': '{m}',
                    'construction_beam_width': '{ef_construction}',
                    'search_beam_width': '{ef_search}'
               }}
               "
                ),
                &[],
            )
            .await
            .unwrap();
    }

    pub(crate) async fn drop_index(&self) {
        self.0
            .session
            .query_unpaged(format!("DROP INDEX IF EXISTS {KEYSPACE}.{INDEX}"), &[])
            .await
            .unwrap();
    }

    pub(crate) async fn upload_vectors(
        &self,
        stream: impl Stream<Item = (i64, Vec<f32>)>,
        concurrency: usize,
    ) {
        let semaphore = Arc::new(Semaphore::new(concurrency));

        let mut stream = pin!(stream);
        while let Some((id, vector)) = stream.next().await {
            let permit = Arc::clone(&semaphore).acquire_owned().await.unwrap();
            let scylla = Arc::clone(&self.0);
            tokio::spawn(async move {
                scylla
                    .session
                    .execute_unpaged(&scylla.st_insert, (id, vector))
                    .await
                    .unwrap();
                drop(permit);
            });
        }
        _ = semaphore.acquire_many(concurrency as u32).await.unwrap();
    }

    pub(crate) async fn search(&self, query: &Query) {
        self.0
            .session
            .execute_unpaged(
                &self.0.st_search,
                (&query.query, query.neighbors.len() as i32),
            )
            .await
            .unwrap();
    }
}
