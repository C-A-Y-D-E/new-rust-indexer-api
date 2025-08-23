use serde::{Deserialize, Deserializer};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Interval {
    OneSecond,
    FiveSeconds,
    FifteenSeconds,
    ThirtySeconds,
    OneMinute,
    FiveMinutes,
    FifteenMinutes,
    ThirtyMinutes,
    OneHour,
    FourHours,
    SixHours,
    TwelveHours,
    OneDay,
}

impl Interval {
    pub fn to_string(&self) -> String {
        match self {
            Interval::OneSecond => "1s".to_string(),
            Interval::FiveSeconds => "5s".to_string(),
            Interval::FifteenSeconds => "15s".to_string(),
            Interval::ThirtySeconds => "30s".to_string(),
            Interval::OneMinute => "1m".to_string(),
            Interval::FiveMinutes => "5m".to_string(),
            Interval::FifteenMinutes => "15m".to_string(),
            Interval::ThirtyMinutes => "30m".to_string(),
            Interval::OneHour => "1h".to_string(),
            Interval::FourHours => "4h".to_string(),
            Interval::SixHours => "6h".to_string(),
            Interval::TwelveHours => "12h".to_string(),
            Interval::OneDay => "1d".to_string(),
        }
    }

    pub fn from_string(interval: &str) -> Result<Interval, String> {
        match interval {
            "1s" => Ok(Interval::OneSecond),
            "5s" => Ok(Interval::FiveSeconds),
            "15s" => Ok(Interval::FifteenSeconds),
            "30s" => Ok(Interval::ThirtySeconds),
            "1m" => Ok(Interval::OneMinute),
            "5m" => Ok(Interval::FiveMinutes),
            "15m" => Ok(Interval::FifteenMinutes),
            "30m" => Ok(Interval::ThirtyMinutes),
            "1h" => Ok(Interval::OneHour),
            "4h" => Ok(Interval::FourHours),
            "6h" => Ok(Interval::SixHours),
            "12h" => Ok(Interval::TwelveHours),
            "1d" => Ok(Interval::OneDay),
            _ => Err(format!(
                "Invalid interval: {}. Must be one of: 1s, 5s, 15s, 30s, 1m, 5m, 15m, 30m, 1h, 4h, 6h, 12h, 1d",
                interval
            )),
        }
    }
}

impl<'de> Deserialize<'de> for Interval {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Interval::from_string(&s).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Deserialize)]
pub struct CandlestickQuery {
    pub pool_address: String,
    pub interval: Interval,
    pub start_time: Option<i64>, // Unix timestamp (seconds since epoch)
    pub end_time: Option<i64>,   // Unix timestamp (seconds since epoch)
    pub limit: i32,
}
