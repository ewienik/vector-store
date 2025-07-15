mod crud;

use crate::test_case::TestActors;
use crate::test_case::TestCase;
use futures::stream;
use futures::stream::StreamExt;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

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
            async move {
                test_case
                    .run(
                        actors,
                        &filter.get(&name).unwrap_or(&HashSet::new()).clone(),
                    )
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
