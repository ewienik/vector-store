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
use std::time::Duration;
use tokio::sync::mpsc::Sender;
use tokio::time;
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
    init: Option<(Duration, TestFn)>,
    tests: Vec<(String, Duration, TestFn)>,
    cleanup: Option<(Duration, TestFn)>,
}

impl TestCase {
    pub(crate) fn empty() -> Self {
        Self {
            init: None,
            tests: vec![],
            cleanup: None,
        }
    }

    pub(crate) fn with_init<F, R>(mut self, timeout: Duration, test_fn: F) -> Self
    where
        F: Fn(TestActors) -> R + 'static,
        R: Future<Output = ()> + Send + 'static,
    {
        self.init = Some((timeout, wrap_test_fn(test_fn)));
        self
    }

    // TODO: provide a macro to simplify test name + function creation
    pub(crate) fn with_test<F, R>(
        mut self,
        name: impl ToString,
        timeout: Duration,
        test_fn: F,
    ) -> Self
    where
        F: Fn(TestActors) -> R + 'static,
        R: Future<Output = ()> + Send + 'static,
    {
        self.tests
            .push((name.to_string(), timeout, wrap_test_fn(test_fn)));
        self
    }

    pub(crate) fn with_cleanup<F, R>(mut self, timeout: Duration, test_fn: F) -> Self
    where
        F: Fn(TestActors) -> R + 'static,
        R: Future<Output = ()> + Send + 'static,
    {
        self.cleanup = Some((timeout, wrap_test_fn(test_fn)));
        self
    }

    pub(crate) async fn run(&self, actors: TestActors, filter: &HashSet<String>) -> bool {
        info!("started run");

        if let Some((timeout, init)) = &self.init {
            if !run_single(info_span!("init"), *timeout, init(actors.clone())).await {
                info!("finished: error in init");
                return false;
            }
        }

        let ok = stream::iter(self.tests.iter())
            .filter(|(name, _, _)| future::ready(filter.is_empty() || filter.contains(name)))
            .then(|(name, timeout, test)| {
                let actors = actors.clone();
                async move { run_single(info_span!("test", name), *timeout, test(actors)).await }
            })
            .filter(|ok| future::ready(*ok == false))
            .count()
            .await
            == 0;

        if let Some((timeout, cleanup)) = &self.cleanup {
            if !run_single(info_span!("cleanup"), *timeout, cleanup(actors.clone())).await {
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

async fn run_single(span: Span, timeout: Duration, future: TestFuture) -> bool {
    let task = tokio::spawn(
        async move {
            time::timeout(timeout, future)
                .await
                .expect("test timed out");
        }
        .instrument(span),
    );
    task.await.is_ok()
}
