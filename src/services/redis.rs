use futures_util::StreamExt;
use serde::Deserialize;
pub async fn subscribe_and_process<T, F, Fut>(channel: &str, handler: F) -> redis::RedisResult<()>
where
    T: for<'de> Deserialize<'de>,
    F: Fn(T) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send,
{
    let client = redis::Client::open("redis://127.0.0.1/")?;
    let mut pubsub = client.get_async_pubsub().await?;

    pubsub.subscribe(channel).await?;
    let mut stream = pubsub.on_message();

    while let Some(msg) = stream.next().await {
        let payload: String = msg.get_payload()?;

        match serde_json::from_str::<T>(&payload) {
            Ok(data) => handler(data).await,
            Err(e) => eprintln!("Failed to deserialize: {}", e),
        }
    }

    Ok(())
}
