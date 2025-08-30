use std::{
    collections::HashMap,
    error::Error,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde_json::Value;
use sqlx::{Row, postgres::PgNotification};

use crate::{models::pool::DBPool, services::db::DbService};
use chrono::Duration as ChronoDuration;

#[derive(Clone)]

pub struct PoolInfo {
    pub pool_address: Vec<u8>,
    pub token_base_address: Vec<u8>,
    pub token_quote_address: Vec<u8>,
    pub pool_base_address: Vec<u8>,
    pub pool_quote_address: Vec<u8>,
    pub curve_percentage: Option<Decimal>,
    pub factory: String,
    pub created_at: DateTime<Utc>,
}
pub struct PoolManager {
    pools: Vec<DBPool>,
    buffer: Vec<Vec<u8>>,
    batch_size: usize,
    last_flush: Instant,
    flush_interval: Duration,
}

impl PoolManager {
    pub fn new(pools: Vec<DBPool>) -> Self {
        Self {
            pools,
            buffer: Vec::<Vec<u8>>::new(),
            batch_size: 100, // Flush after 10 pool addresses or every 1 seconds
            last_flush: Instant::now(),
            flush_interval: Duration::from_secs(1),
        }
    }

    pub async fn update_pools(&mut self, pool: DBPool) {
        let cutoff = Utc::now() - ChronoDuration::hours(24);
        self.pools.retain(|p| p.created_at < cutoff);
        self.pools.push(pool);
    }

    pub async fn verify_and_add_pool(&mut self, pool_address: Vec<u8>) -> Option<Vec<Vec<u8>>> {
        // Check if pool is already in the buffer
        if self.buffer.contains(&pool_address) {
            // println!("Pool already in buffer");
            return None;
        }
        // Check if pool is already in the pools
        let pool_info = self.pools.iter().find(|p| p.pool_address == pool_address);
        if pool_info.is_none() {
            self.buffer.push(pool_address);
        }
        // Check if we should flush
        if self.should_flush().await {
            return self.flush_and_process().await;
        }
        None
    }

    async fn should_flush(&self) -> bool {
        self.buffer.len() >= self.batch_size
            || (!self.buffer.is_empty() && self.last_flush.elapsed() >= self.flush_interval)
    }
    async fn flush_and_process(&mut self) -> Option<Vec<Vec<u8>>> {
        if !self.buffer.is_empty() {
            let batch: Vec<Vec<u8>> = std::mem::take(&mut self.buffer);
            self.last_flush = Instant::now();
            Some(batch)
        } else {
            None
        }
    }
}
// pub async fn on_new_swap_event(
//     notification: &PgNotification,
//     db_clone: &DbService,
// ) -> Result<Vec<Vec<u8>>, Box<dyn Error + Send + Sync>> {
//     let raw_notification = serde_json::from_str::<Value>(notification.payload()).unwrap();
//     let pool_address = raw_notification["pool_address"].as_str().unwrap();
//     let mut pool_manager = PoolManager::new(db_clone.clone());
//     let batch = pool_manager
//         .verify_and_add_pool(hex::decode(pool_address).unwrap())
//         .await;

//     if batch.is_some() {
//         println!("Batch: {:?}", batch);
//         Ok(batch.unwrap_or_default())
//     } else {
//         println!("No batch");
//         Ok(vec![])
//     }
// }
