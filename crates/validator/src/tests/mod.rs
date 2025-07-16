mod crud;

use crate::test_case::TestActors;
use crate::test_case::TestCase;
use futures::stream;
use futures::stream::StreamExt;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use tracing::Instrument;
use tracing::info_span;

pub(crate) async fn register() -> Vec<(String, TestCase)> {
    let test_cases = vec![("crud", crud::new().await)]
        .into_iter()
        .map(|(name, test_case)| (name.to_string(), test_case))
        .collect::<Vec<_>>();
    test_cases
}

pub(crate) async fn run(
    actors: TestActors,
    test_cases: Vec<(String, TestCase)>,
    filter: Arc<HashMap<String, HashSet<String>>>,
) -> bool {
    stream::iter(test_cases.into_iter())
        .filter(|(name, _)| {
            let process = filter.is_empty() || filter.contains_key(name);
            async move { process }
        })
        .then(|(name, test_case)| {
            let actors = actors.clone();
            let filter = filter.clone();
            let filter_name = name.clone();
            async move {
                test_case
                    .run(
                        actors,
                        &filter.get(&filter_name).unwrap_or(&HashSet::new()).clone(),
                    )
                    .instrument(info_span!("test-case", name))
                    .await
            }
        })
        .filter(|ok| {
            let ok = *ok;
            async move { !ok }
        })
        .count()
        .await
        == 0
}
