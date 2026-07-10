use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Redis: {0}")]
    Redis(#[from] redis::RedisError),
    #[error("Serialization: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("Invalid coordinates lat={lat} lon={lon}")]
    InvalidCoordinates { lat: f64, lon: f64 },
    #[error("Invalid S2 level {0} — must be 1–30")]
    InvalidLevel(u8),
}

pub type Result<T> = std::result::Result<T, Error>;
