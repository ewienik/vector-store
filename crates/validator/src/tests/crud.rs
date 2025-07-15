use crate::test_case::TestActors;
use crate::test_case::TestCase;

pub(crate) async fn new() -> TestCase {
    TestCase::empty()
        .with_init(init)
        .with_cleanup(cleanup)
        .with_test(
            "create_search_delete_single_index",
            create_search_delete_single_index,
        )
}

async fn init(_actors: TestActors) {
    todo!();
}

async fn cleanup(_actors: TestActors) {
    todo!();
}

async fn create_search_delete_single_index(_actors: TestActors) {
    todo!();
}
