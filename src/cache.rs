use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::common::Result as DataFusionResult;

#[async_trait]
pub trait QueryCache: Send + Sync + fmt::Debug {
    async fn entry(&self, query_fingerprint: &str) -> DataFusionResult<CacheEntry>;
}

#[derive(Debug, Clone)]
pub enum CacheEntry {
    Occupied(Arc<dyn OccupiedCacheEntry>),
    Vacant(Arc<dyn VacantCacheEntry>),
}

impl CacheEntry {
    pub fn occupied(&self) -> bool {
        matches!(self, CacheEntry::Occupied(_))
    }

    pub async fn put(&self, timestamp: i64, record_batch: &[RecordBatch]) -> DataFusionResult<()> {
        match self {
            CacheEntry::Occupied(entry) => entry.put(timestamp, record_batch).await,
            CacheEntry::Vacant(entry) => entry.put(timestamp, record_batch).await,
        }
    }
}

#[async_trait]
pub trait OccupiedCacheEntry: Send + Sync + fmt::Debug {
    /// The timestamp of the cache entry - e.g. when data was written to the cache.
    fn timestamp(&self) -> i64;

    /// Returns the record batch stored in this cache entry.
    async fn get(&self) -> DataFusionResult<&[RecordBatch]>;

    /// Updates the record batch stored in this cache entry with a new timestamp.
    async fn put(&self, timestamp: i64, record_batch: &[RecordBatch]) -> DataFusionResult<()>;
}

#[async_trait]
pub trait VacantCacheEntry: Send + Sync + fmt::Debug {
    /// Set the record batch stored in this cache entry.
    async fn put(&self, timestamp: i64, record_batch: &[RecordBatch]) -> DataFusionResult<()>;
}

#[derive(Clone, Default)]
#[allow(clippy::type_complexity)]
pub struct MemoryQueryCache {
    cache: Arc<Mutex<HashMap<String, (i64, Arc<Vec<RecordBatch>>)>>>,
}

impl fmt::Debug for MemoryQueryCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "MemoryQueryCache{{")?;
        for (fingerprint, (timestamp, record_batch)) in self.cache.lock().unwrap().iter() {
            let table = pretty_format_batches(record_batch).map_err(|_| fmt::Error)?;
            writeln!(f, "{fingerprint}\ntimestamp: {timestamp} data:\n{table}\n")?;
        }
        writeln!(f, "}}")
    }
}

impl MemoryQueryCache {
    fn put(&self, fingerprint: &str, timestamp: i64, record_batch: &[RecordBatch]) {
        let mut cache = self.cache.lock().unwrap();
        cache.insert(fingerprint.to_owned(), (timestamp, Arc::new(record_batch.to_vec())));
    }
}

#[async_trait]
impl QueryCache for MemoryQueryCache {
    async fn entry(&self, query_fingerprint: &str) -> DataFusionResult<CacheEntry> {
        let cache = self.cache.lock().unwrap();
        if let Some((timestamp, record_batch)) = cache.get(query_fingerprint).cloned() {
            let entry = OccupiedMemoryCacheEntry {
                fingerprint: query_fingerprint.to_string(),
                timestamp,
                record_batch,
                cache: self.clone(),
            };
            Ok(CacheEntry::Occupied(Arc::new(entry)))
        } else {
            let entry = VacantMemoryCacheEntry {
                fingerprint: query_fingerprint.to_string(),
                cache: self.clone(),
            };
            Ok(CacheEntry::Vacant(Arc::new(entry)))
        }
    }
}

#[derive(Debug)]
struct OccupiedMemoryCacheEntry {
    fingerprint: String,
    timestamp: i64,
    record_batch: Arc<Vec<RecordBatch>>,
    cache: MemoryQueryCache,
}

#[async_trait]
impl OccupiedCacheEntry for OccupiedMemoryCacheEntry {
    fn timestamp(&self) -> i64 {
        self.timestamp
    }

    async fn get(&self) -> DataFusionResult<&[RecordBatch]> {
        Ok(&self.record_batch)
    }

    async fn put(&self, timestamp: i64, record_batch: &[RecordBatch]) -> DataFusionResult<()> {
        self.cache.put(&self.fingerprint, timestamp, record_batch);
        Ok(())
    }
}

#[derive(Debug)]
struct VacantMemoryCacheEntry {
    fingerprint: String,
    cache: MemoryQueryCache,
}

#[async_trait]
impl VacantCacheEntry for VacantMemoryCacheEntry {
    async fn put(&self, timestamp: i64, record_batch: &[RecordBatch]) -> DataFusionResult<()> {
        self.cache.put(&self.fingerprint, timestamp, record_batch);
        Ok(())
    }
}
