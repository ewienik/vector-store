use crate::db::Db;
use crate::dns::Dns;
use crate::ip::Ip;
use crate::vs::Vs;
use futures::FutureExt;
use futures::future::BoxFuture;
use futures::stream;
use futures::stream::StreamExt;
use std::collections::HashSet;
use std::future;
use tokio::sync::mpsc::Sender;
use tracing::Instrument;
use tracing::Span;
use tracing::info;
use tracing::info_span;

#[derive(Clone)]
pub(crate) struct TestActors {
    pub(crate) dns: Sender<Dns>,
    pub(crate) db: Sender<Db>,
    pub(crate) vs: Sender<Vs>,
    pub(crate) ip: Sender<Ip>,
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
        info!("started run");

        if let Some(init) = &self.init {
            if !run_single(info_span!("init"), init(actors.clone())).await {
                info!("finished: error in init");
                return false;
            }
        }

        let ok = stream::iter(self.tests.iter())
            .filter(|(name, _)| future::ready(filter.is_empty() || filter.contains(name)))
            .then(|(name, test)| {
                let actors = actors.clone();
                async move { run_single(info_span!("test", name), test(actors)).await }
            })
            .filter(|ok| future::ready(*ok == false))
            .count()
            .await
            == 0;

        if let Some(cleanup) = &self.cleanup {
            if !run_single(info_span!("cleanup"), cleanup(actors.clone())).await {
                info!("finished: error in cleanup");
                return false;
            }
        }

        info!("finished");
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

async fn run_single(span: Span, future: TestFuture) -> bool {
    let task = tokio::spawn(
        async move {
            future.await;
        }
        .instrument(span),
    );
    task.await.is_ok()
}
