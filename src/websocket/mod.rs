pub mod store;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use socketioxide::extract::{Data, SocketRef};

use tracing::info;

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

    socket.on("message", |_socket: SocketRef, Data::<Value>(data)| {
        info!("✅ Successfully parsed: {:?}", data);
        // info!("Pool address: {}", data.pool_address);
        // info!("Room: {}", data.room);
    })
}
