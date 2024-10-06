use chrono::{DateTime, FixedOffset};
use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray, TimestampNanosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::arrow::util::pretty::print_batches;
use datafusion::common::Column;
use datafusion::datasource::MemTable;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_query_cache::{with_query_cache_log, LogStderrColors, MemoryQueryCache, QueryCacheConfig};
use std::sync::Arc;

#[tokio::main]
async fn main() {
    let cache = Arc::new(MemoryQueryCache::default());

    let divide = DateTime::parse_from_rfc3339("2024-01-01T17:18:19Z").unwrap();
    let batch1 = create_data(DateTime::parse_from_rfc3339("2024-01-01T00:00:00Z").unwrap(), divide);

    let ctx = session_ctx(cache.clone(), divide.timestamp_nanos_opt()).await;
    let table = MemTable::try_new(batch1.schema(), vec![vec![batch1.clone()]]).unwrap();
    ctx.register_table("records", Arc::new(table)).unwrap();

    // let sql = "SELECT date_trunc('hour', timestamp), round(avg(value), 2), count(*) from records where value>1 group by 1 order by 1 desc";

    let sql = "SELECT round(avg(value), 2), count(*) from records where value>1";

    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    println!("first run:");
    print_batches(&batches).unwrap();

    let batch2 = create_data(divide, DateTime::parse_from_rfc3339("2024-01-02T00:00:00Z").unwrap());

    let ctx = session_ctx(cache.clone(), None).await;
    let partitions = vec![vec![batch1.clone(), batch2]];
    let table = MemTable::try_new(batch1.schema(), partitions.clone()).unwrap();
    ctx.register_table("records", Arc::new(table)).unwrap();

    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    println!("second run (more data added):");
    print_batches(&batches).unwrap();

    let ctx_simple = SessionContext::new();
    let table = MemTable::try_new(batch1.schema(), partitions.clone()).unwrap();
    ctx_simple.register_table("records", Arc::new(table)).unwrap();
    // dbg!(ctx_simple.sql(sql).await.unwrap().create_physical_plan().await.unwrap());

    let batches = ctx_simple.sql(sql).await.unwrap().collect().await.unwrap();
    println!("second run with no caching:");
    print_batches(&batches).unwrap();

    let sql = format!("EXPLAIN ANALYZE {sql}");
    let df = ctx.sql(&sql).await.unwrap();
    let batches = df.collect().await.unwrap();

    // second column, first value is the plan
    let plan = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0);
    println!("\nEXPLAIN ANALYZE:\n{}", plan);

    // println!("{}", cache.display());
}

async fn session_ctx(cache: Arc<MemoryQueryCache>, override_now: Option<i64>) -> SessionContext {
    let config = SessionConfig::new().with_target_partitions(10);
    let runtime = Arc::new(RuntimeEnv::default());
    let state_builder = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(runtime)
        .with_default_features();

    let sort_col = Column::new(Some("records".to_string()), "timestamp".to_string());
    let query_cache_config = QueryCacheConfig::new(sort_col, cache)
        .with_group_by_function("date_trunc")
        .with_override_now(override_now);

    let log = LogStderrColors::default();
    let state_builder = with_query_cache_log(state_builder, query_cache_config, log);
    SessionContext::new_with_state(state_builder.build())
}

fn create_data(start: DateTime<FixedOffset>, stop: DateTime<FixedOffset>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("service", DataType::Utf8, true),
        Field::new("value", DataType::Int64, true),
    ]));

    let mut timestamp = start.timestamp_nanos_opt().unwrap();
    let mut timestamps = Vec::new();
    let mut service_names = Vec::new();
    let mut values = Vec::new();

    let end = stop.timestamp_nanos_opt().unwrap();

    let mut seed = 0;
    loop {
        timestamps.push(timestamp);
        timestamp += 1_000_000_000;
        service_names.push(SERVICES[usize::try_from(seed).unwrap() % 5]);
        values.push(seed);
        if timestamp >= end {
            break;
        }
        seed += 1;
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampNanosecondArray::from(timestamps)),
            Arc::new(StringArray::from(service_names)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .unwrap()
}

const SERVICES: [&str; 5] = ["foo", "bar", "baz", "qux", "quux"];
