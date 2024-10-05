use std::sync::Arc;

use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::arrow::util::pretty::print_batches;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};

use datafusion_query_cache::{with_query_cache, QueryCacheConfig};

#[tokio::main]
async fn main() {
    let ctx = session_ctx().await;
    let input_batch = create_data();
    ctx.register_batch("records", input_batch.clone()).unwrap();

    let sql = "SELECT date_trunc('hour', timestamp), avg(value), count(*) from records group by 1 order by 1 desc";
    let df = ctx.sql(sql).await.unwrap();
    let batches = df.collect().await.unwrap();
    print_batches(&batches).unwrap();
}

async fn session_ctx() -> SessionContext {
    let config = SessionConfig::new();
    let runtime = Arc::new(RuntimeEnv::default());
    let state_builder = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(runtime)
        .with_default_features();

    let query_cache_config = QueryCacheConfig::default()
        .with_temporal_column_table_col("records", "timestamp")
        .with_group_by_function("date_trunc");

    let state_builder = with_query_cache(state_builder, query_cache_config);
    SessionContext::new_with_state(state_builder.build())
}

fn create_data() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("service", DataType::Utf8, true),
        Field::new("value", DataType::Int64, true),
    ]));

    // 2024-01-01 00:00:00
    let mut timestamp = 1704067200000000;
    let count = 10_000;
    let mut timestamps = Vec::with_capacity(count);
    let mut service_names = Vec::with_capacity(count);
    let mut values = Vec::with_capacity(count);

    let mut seed = 0i64;

    for _ in 0..50_000 {
        // 0 - 999_000 us, so 0 - 0.999 s
        timestamps.push(timestamp);
        timestamp += 1_000_000;
        service_names.push(SERVICES[seed as usize % 5]);
        values.push(seed % 500);
        seed += 1;
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampMicrosecondArray::from(timestamps)),
            Arc::new(StringArray::from(service_names)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .unwrap()
}

const SERVICES: [&str; 5] = ["foo", "bar", "baz", "qux", "quux"];
