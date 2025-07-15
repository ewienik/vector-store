use crate::db::Db;
use crate::dns::Dns;
use crate::vs::Vs;
use futures::FutureExt;
use futures::future::BoxFuture;
use futures::stream;
use futures::stream::StreamExt;
use std::collections::HashSet;
use tokio::sync::mpsc::Sender;

#[derive(Clone)]
pub(crate) struct TestActors {
    pub(crate) dns: Sender<Dns>,
    pub(crate) db: Sender<Db>,
    pub(crate) vs: Sender<Vs>,
}

type TestFuture = BoxFuture<'static, ()>;

type TestFn = Box<dyn Fn(TestActors) -> TestFuture>;

pub(crate) struct TestCase {
    init: Option<TestFn>,
    tests: Vec<(String, TestFn)>,
    cleanup: Option<TestFn>,
}

impl TestCase {
    pub(crate) fn empty() -> Self {
        Self {
            init: None,
            tests: vec![],
            cleanup: None,
        }
    }

    pub(crate) fn with_init<F, R>(mut self, test_fn: F) -> Self
    where
        F: Fn(TestActors) -> R + 'static,
        R: Future<Output = ()> + Send + 'static,
    {
        self.init = Some(wrap_test_fn(test_fn));
        self
    }

    pub(crate) fn with_test<F, R>(mut self, name: impl ToString, test_fn: F) -> Self
    where
        F: Fn(TestActors) -> R + 'static,
        R: Future<Output = ()> + Send + 'static,
    {
        self.tests.push((name.to_string(), wrap_test_fn(test_fn)));
        self
    }

    pub(crate) fn with_cleanup<F, R>(mut self, test_fn: F) -> Self
    where
        F: Fn(TestActors) -> R + 'static,
        R: Future<Output = ()> + Send + 'static,
    {
        self.cleanup = Some(wrap_test_fn(test_fn));
        self
    }

    pub(crate) async fn run(&self, actors: TestActors, filter: &HashSet<String>) -> bool {
        if let Some(init) = &self.init {
            if !run_single(init(actors.clone())).await {
                return false;
            }
        }

        let ok = stream::iter(self.tests.iter())
            .filter_map(|(name, test)| async move {
                (filter.is_empty() || filter.contains(name)).then_some(test)
            })
            .then(|test| {
                let actors = actors.clone();
                async move { run_single(test(actors)).await }
            })
            .filter(|ok| {
                let ok = *ok;
                async move { !ok }
            })
            .count()
            .await
            == 0;

        if let Some(cleanup) = &self.cleanup {
            if !run_single(cleanup(actors.clone())).await {
                return false;
            }
        }

        ok
    }
}

fn wrap_test_fn<F, R>(test_fn: F) -> TestFn
where
    F: Fn(TestActors) -> R + 'static,
    R: Future<Output = ()> + Send + 'static,
{
    Box::new(move |actors: TestActors| {
        let future = test_fn(actors);
        async move { future.await }.boxed()
    })
}

async fn run_single(future: TestFuture) -> bool {
    let task = tokio::spawn(async move {
        future.await;
    });
    task.await.is_ok()
}
