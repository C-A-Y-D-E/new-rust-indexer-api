pub mod new_pool_event;
pub mod new_swap_event;
pub mod store;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use socketioxide::extract::{Data, SocketRef};

use tracing::info;

use axum::{extract::State, Json};
use rust_decimal::Decimal;

use crate::types::filter::{FactoryFilters, Filters, PulseFilter, PulseTable, RangeFilter};

#[derive(Debug, Deserialize, Serialize)]
#[serde(untagged)]
enum Room {
    #[serde(rename = "new-pair")]
    NewPair,
    #[serde(rename = "update-pulse")]
    UpdatePulse,
}

impl Room {
    fn as_str(&self) -> String {
        match self {
            Room::NewPair => "new-pair".to_string(),
            Room::UpdatePulse => "update-pulse".to_string(),
        }
    }
}

pub async fn on_connect(socket: SocketRef) {
    info!("Client connected: {:?}", socket.id);

    socket.on(
        "join",
        |_socket: SocketRef, Data::<Room>(room)| match room {
            Room::NewPair => {
                info!("✅ Joining new-pair room");
                _socket.join(room.as_str());
            }
            Room::UpdatePulse => {
                info!("✅ Joining update-pulse room");
                // _socket.join(room.as_str());
            }
        },
    );

    socket.on("message", |socket: SocketRef, Data::<Value>(data)| {
        // Handle subscribe/ping similar to Axiom
        let method = data.get("method").and_then(|v| v.as_str()).unwrap_or("");
        if method == "ping" {
            let _ = socket.emit("pong", &json!({"ok": true}));
            return;
        }

        if method == "subscribe" {
            let Some(sub) = data.get("subscription") else { return };
            let sub_type = sub.get("type").and_then(|v| v.as_str()).unwrap_or("");

            if sub_type == "update_pulse_v2" {
                // dedupe
                let id = socket.id.to_string();
                let exists = store::subs().insert(id.clone(), true).is_some();
                if exists {
                    let _ = socket
                        .emit(
                            "error",
                            &json!({
                                "channel": "error",
                                "data": format!("Already subscribed: {{\"type\":\"update_pulse_v2\"}}")
                            }),
                        );
                    return;
                }

                socket.join("update_pulse_v2");

                // ack
                let _ = socket
                    .emit(
                        "subscriptionResponse",
                        &json!({"method":"subscribe","subscription": {"type":"update_pulse_v2"}}),
                    );

                // snapshot
                if let Some(db) = store::clickhouse() {
                    let socket_clone = socket.clone();
                    let db = db.clone();
                    tokio::spawn(async move {
                        let filters = Filters {
                            factories: FactoryFilters { pump_fun: true, pump_swap: false },
                            search_keywords: vec![],
                            exclude_keywords: vec![],
                            age: RangeFilter { min: Some(0), max: Some(1440) },
                            top10_holders: RangeFilter { min: Some(Decimal::from(0)), max: Some(Decimal::from(100)) },
                            dev_holding: RangeFilter { min: Some(Decimal::from(0)), max: Some(Decimal::from(100)) },
                            snipers_holding: RangeFilter { min: Some(Decimal::from(0)), max: Some(Decimal::from(100)) },
                            holders: RangeFilter { min: Some(0), max: Some(1_000_000_000) },
                            bonding_curve: RangeFilter { min: Some(Decimal::from(0)), max: Some(Decimal::from(100)) },
                            liquidity: RangeFilter { min: Some(Decimal::from(0)), max: Some(Decimal::from(1_000_000_000u64)) },
                            volume: RangeFilter { min: Some(Decimal::from(0)), max: Some(Decimal::from(1_000_000_000u64)) },
                            market_cap: RangeFilter { min: Some(Decimal::from(0)), max: Some(Decimal::from(1_000_000_000u64)) },
                            txns: RangeFilter { min: Some(0), max: Some(1_000_000_000) },
                            num_buys: RangeFilter { min: Some(0), max: Some(1_000_000_000) },
                            num_sells: RangeFilter { min: Some(0), max: Some(1_000_000_000) },
                            num_migrations: RangeFilter { min: Some(0), max: Some(1_000_000_000) },
                            twitter: false,
                            website: false,
                            telegram: false,
                            at_least_one_social: false,
                        };

                        let payload = PulseFilter { filters, table: PulseTable::NewPairs };
                        match crate::routes::pulse::pulse(State(db), Json(payload)).await {
                            Ok(Json(value)) => {
                                let content = value.get("pools").cloned().unwrap_or(value);
                                let _ = socket_clone.emit(
                                    "update_pulse_v2",
                                    &json!({
                                        "channel": "update_pulse_v2",
                                        "data": {"isSnapshot": true, "content": content}
                                    }),
                                );
                            }
                            Err(_) => {
                                let _ = socket_clone
                                    .emit("error", &json!({"channel":"error","data":"snapshot failed"}));
                            }
                        }
                    });
                }
            }
        }
    })
}
