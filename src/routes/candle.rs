// use actix_web::{HttpRequest, HttpResponse, get, web::Data, web::Query};
// use serde::Deserialize;

// use crate::services::db::DbService;

// #[derive(Deserialize)]
// struct CandleQuery {
//     limit: Option<i64>,
//     offset: Option<i64>,
// }

// #[get("/candle/{pool_address}")]
// async fn get_candle(
//     data: Data<DbService>,
//     req: HttpRequest,
//     query: Query<CandleQuery>,
// ) -> HttpResponse {
//     let pool_address = req.match_info().get("pool_address").unwrap();
//     println!("Pool address: {}", pool_address);

//     HttpResponse::Ok().body("Hello world!")

//     // let limit = query.limit.unwrap_or(100);
//     // let offset = query.offset.unwrap_or(0);

//     // let candles = data
//     //     .get_ohlcv_1s(pool_address, Some(limit), Some(offset))
//     //     .await;

//     // match candles {
//     //     Ok(candles) => HttpResponse::Ok().json(candles),
//     //     Err(e) => {
//     //         println!("Error: {}", e);
//     //         HttpResponse::InternalServerError().json(format!("Database error: {}", e))
//     //     }
//     // }
// }
