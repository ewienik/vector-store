use crate::test_case::TestActors;
use crate::test_case::TestCase;
use crate::test_case::TestFuture;

pub(crate) async fn new() -> TestCase {
    TestCase::empty().with_init(init).with_cleanup(init)
}

async fn init(actors: TestActors) {
    todo!();
}

async fn cleanup(actors: TestActors) {
    todo!();
}
