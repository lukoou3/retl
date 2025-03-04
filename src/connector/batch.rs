use std::marker::PhantomData;
use serde::{Deserialize, Serialize};

pub trait BatchSettings {
    const MAX_ROWS: usize;
    const MAX_BYTES: usize;
    const INTERVAL_MS: u64;
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct BatchConfig<D: BatchSettings + Clone> {
    #[serde(default = "default_max_rows::<D>", rename = "batch_max_rows")]
    pub max_rows: usize,
    #[serde(default = "default_max_bytes::<D>", rename = "batch_max_bytes")]
    pub max_bytes: usize,
    #[serde(default = "default_interval::<D>", rename = "batch_interval_ms")]
    pub interval_ms: u64,
    #[serde(skip)]
    _d: PhantomData<D>,
}

const fn default_max_rows<D: BatchSettings>() -> usize {
    D::MAX_ROWS
}

const fn default_max_bytes<D: BatchSettings>() -> usize {
    D::MAX_BYTES
}

const fn default_interval<D: BatchSettings>() -> u64 {
    D::INTERVAL_MS
}

#[derive(Clone, Copy, Debug, Default)]
pub struct DefaultBatchSettings;

impl BatchSettings for DefaultBatchSettings {
    const MAX_ROWS: usize = 10000;
    const MAX_BYTES: usize = 1024 * 1024 * 10;
    const INTERVAL_MS: u64 = 30000;
}