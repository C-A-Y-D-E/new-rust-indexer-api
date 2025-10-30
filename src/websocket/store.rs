use dashmap::DashMap;
use once_cell::sync::OnceCell;

use crate::services::clickhouse::ClickhouseService;

static CLICKHOUSE: OnceCell<ClickhouseService> = OnceCell::new();
static SUBSCRIPTIONS: OnceCell<DashMap<String, bool>> = OnceCell::new();

pub fn set_clickhouse_service(service: ClickhouseService) {
    let _ = CLICKHOUSE.set(service);
}

pub fn clickhouse() -> Option<&'static ClickhouseService> {
    CLICKHOUSE.get()
}

pub fn subs() -> &'static DashMap<String, bool> {
    SUBSCRIPTIONS.get_or_init(|| DashMap::new())
}

