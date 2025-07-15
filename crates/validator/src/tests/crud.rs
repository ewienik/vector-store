use crate::db::DbExt;
use crate::dns::DnsExt;
use crate::ip::IpExt;
use crate::test_case::TestActors;
use crate::test_case::TestCase;
use crate::vs::VsExt;

pub(crate) async fn new() -> TestCase {
    TestCase::empty()
        .with_init(init)
        .with_cleanup(cleanup)
        .with_test(
            "create_search_delete_single_index",
            create_search_delete_single_index,
        )
}

const VS_NAME: &str = "vs";

const VS_PORT: u16 = 6080;
const DB_PORT: u16 = 9042;

const VS_OCTET: u8 = 1;
const DB_OCTET: u8 = 2;

async fn init(actors: TestActors) {
    let vs_ip = actors.ip.calculate(VS_OCTET).await;

    actors.dns.upsert(VS_NAME.to_string(), Some(vs_ip)).await;

    let vs_url = format!("http://{}.{}:{}", VS_NAME, actors.dns.zone().await, VS_PORT);

    let db_ip = actors.ip.calculate(DB_OCTET).await;

    actors.db.start(vs_url, (db_ip, DB_PORT).into()).await;
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    actors
        .vs
        .start((vs_ip, VS_PORT).into(), (db_ip, DB_PORT).into())
        .await;
}

async fn cleanup(actors: TestActors) {
    actors.dns.upsert(VS_NAME.to_string(), None).await;
    actors.vs.stop().await;
    actors.db.stop().await;
}

async fn create_search_delete_single_index(_actors: TestActors) {}
