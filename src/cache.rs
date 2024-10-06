use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::common::{Result as DataFusionResult};

#[async_trait]
pub trait QueryCache: Send + Sync + fmt::Debug {
    async fn entry(&self, query_fingerprint: &str) -> DataFusionResult<Arc<dyn QueryCacheEntry>>;
}

#[async_trait]
pub trait QueryCacheEntry: Send + Sync + fmt::Debug {
    fn occupied(&self) -> bool;

    async fn get(&self) -> DataFusionResult<Option<&Vec<RecordBatch>>>;

    async fn put(&self, record_batch: &Vec<RecordBatch>) -> DataFusionResult<()>;
}

type MemoryHashmap = Arc<Mutex<HashMap<String, Arc<Vec<RecordBatch>>>>>;
#[derive(Debug, Default)]
pub struct MemoryQueryCache {
    cache: MemoryHashmap,
}

#[async_trait]
impl QueryCache for MemoryQueryCache {
    async fn entry(&self, query_fingerprint: &str) -> DataFusionResult<Arc<dyn QueryCacheEntry>> {
        let cache = self.cache.lock().unwrap();
        let entry = cache.get(query_fingerprint).cloned();

        Ok(Arc::new(MemoryQueryCacheEntry {
            fingerprint: query_fingerprint.to_string(),
            record_batch: entry,
            cache: self.cache.clone(),
        }))
    }
}

#[derive(Debug)]
pub struct MemoryQueryCacheEntry {
    fingerprint: String,
    record_batch: Option<Arc<Vec<RecordBatch>>>,
    cache: MemoryHashmap,
}

#[async_trait]
impl QueryCacheEntry for MemoryQueryCacheEntry {
    fn occupied(&self) -> bool {
        self.record_batch.is_some()
    }

    async fn get(&self) -> DataFusionResult<Option<&Vec<RecordBatch>>> {
        Ok(self.record_batch.as_deref())
    }

    async fn put(&self, record_batch: &Vec<RecordBatch>) -> DataFusionResult<()> {
        let mut cache = self.cache.lock().unwrap();
        cache.insert(self.fingerprint.clone(), Arc::new(record_batch.clone()));
        Ok(())
    }
}
