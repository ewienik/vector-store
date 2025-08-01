/*
 * Copyright 2025-present ScyllaDB
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

use crate::db_basic;
use crate::db_basic::Index;
use crate::db_basic::Table;
use crate::mock_opensearch;
use crate::wait_for;
use ::time::OffsetDateTime;
use httpclient::HttpClient;
use scylla::value::CqlValue;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use uuid::Uuid;
use vector_store::IndexMetadata;

#[tokio::test]
async fn simple_create_search_delete_index() {
    crate::enable_tracing();
    let node_state = vector_store::new_node_state().await;
    let (db_actor, db) = db_basic::new(node_state.clone());

    let index = IndexMetadata {
        keyspace_name: "vector".to_string().into(),
        table_name: "items".to_string().into(),
        index_name: "ann".to_string().into(),
        target_column: "embedding".to_string().into(),
        dimensions: NonZeroUsize::new(3).unwrap().into(),
        connectivity: Default::default(),
        expansion_add: Default::default(),
        expansion_search: Default::default(),
        space_type: Default::default(),
        version: Uuid::new_v4().into(),
    };
    let server = mock_opensearch::TestOpenSearchServer::start().await;

    let index_factory = vector_store::new_index_factory_opensearch(server.base_url()).unwrap();

    let (_server_actor, addr) = vector_store::run(
        SocketAddr::from(([127, 0, 0, 1], 0)).into(),
        Some(1),
        node_state,
        db_actor,
        index_factory,
    )
    .await
    .unwrap();

    let client = HttpClient::new(addr);

    db.add_table(
        index.keyspace_name.clone(),
        index.table_name.clone(),
        Table {
            primary_keys: vec!["pk".to_string().into(), "ck".to_string().into()],
            dimensions: [(index.target_column.clone(), index.dimensions)]
                .into_iter()
                .collect(),
        },
    )
    .unwrap();
    db.add_index(
        &index.keyspace_name,
        index.index_name.clone(),
        Index {
            table_name: index.table_name.clone(),
            target_column: index.target_column.clone(),
            connectivity: index.connectivity,
            expansion_add: index.expansion_add,
            expansion_search: index.expansion_search,
            space_type: index.space_type,
        },
    )
    .unwrap();
    db.insert_values(
        &index.keyspace_name,
        &index.table_name,
        &index.target_column,
        vec![
            (
                vec![CqlValue::Int(1), CqlValue::Text("one".to_string())].into(),
                Some(vec![1., 1., 1.].into()),
                OffsetDateTime::from_unix_timestamp(10).unwrap().into(),
            ),
            (
                vec![CqlValue::Int(2), CqlValue::Text("two".to_string())].into(),
                Some(vec![2., -2., 2.].into()),
                OffsetDateTime::from_unix_timestamp(20).unwrap().into(),
            ),
            (
                vec![CqlValue::Int(3), CqlValue::Text("three".to_string())].into(),
                Some(vec![3., 3., 3.].into()),
                OffsetDateTime::from_unix_timestamp(30).unwrap().into(),
            ),
        ],
    )
    .unwrap();

    wait_for(
        || async { client.count(&index).await == Some(3) },
        "Waiting for index to be added to the store",
    )
    .await;

    let indexes = client.indexes().await;
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0], vector_store::IndexInfo::new("vector", "ann"));

    let (primary_keys, distances) = client
        .ann(
            &index,
            vec![2.1, -2., 2.].into(),
            NonZeroUsize::new(1).unwrap().into(),
        )
        .await;
    assert_eq!(distances.len(), 1);
    let primary_keys_pk = primary_keys.get(&"pk".to_string().into()).unwrap();
    let primary_keys_ck = primary_keys.get(&"ck".to_string().into()).unwrap();
    assert_eq!(distances.len(), primary_keys_pk.len());
    assert_eq!(distances.len(), primary_keys_ck.len());
    assert_eq!(primary_keys_pk.first().unwrap().as_i64().unwrap(), 2);
    assert_eq!(primary_keys_ck.first().unwrap().as_str().unwrap(), "two");

    db.del_index(&index.keyspace_name, &index.index_name)
        .unwrap();

    wait_for(
        || async { client.indexes().await.is_empty() },
        "Waiting for index to be removed from the store",
    )
    .await;
}
