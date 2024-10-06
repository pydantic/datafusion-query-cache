mod aggregate;
mod cache;
mod log;

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::{Column, Result as DataFusionResult};
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};

use aggregate::{QCAggregateExecPlanner, QCAggregateOptimizerRule};
pub use cache::MemoryQueryCache;
use cache::QueryCache;
pub use log::{LogNoOp, LogStderrColors};

#[derive(Debug)]
pub struct QueryCacheConfig {
    default_sort_column: Column,
    temporal_columns: HashSet<Column>,
    group_by_functions: HashSet<String>,
    override_now: Option<i64>,
    cache: Arc<dyn QueryCache>,
}

impl QueryCacheConfig {
    pub fn new(default_sort_column: Column, cache: Arc<dyn QueryCache>) -> Self {
        let temporal_columns = HashSet::from([default_sort_column.clone()]);
        Self {
            default_sort_column,
            temporal_columns,
            override_now: None,
            group_by_functions: HashSet::new(),
            cache,
        }
    }

    pub fn with_temporal_column(mut self, column: Column) -> Self {
        self.temporal_columns.insert(column);
        self
    }

    pub fn with_override_now(mut self, timestamp: Option<i64>) -> Self {
        self.override_now = timestamp;
        self
    }

    pub fn with_group_by_function(mut self, function: impl Into<String>) -> Self {
        self.group_by_functions.insert(function.into());
        self
    }

    pub(crate) fn default_sort_column(&self) -> &Column {
        &self.default_sort_column
    }

    pub(crate) fn allow_group_by_function(&self, function: &str) -> bool {
        self.group_by_functions.contains(function)
    }

    pub(crate) fn allow_temporal_column(&self, column: &Column) -> bool {
        self.temporal_columns.contains(column)
    }

    pub fn cache(&self) -> &Arc<dyn QueryCache> {
        &self.cache
    }
}

pub fn with_query_cache(builder: SessionStateBuilder, config: QueryCacheConfig) -> SessionStateBuilder {
    with_query_cache_log(builder, config, LogNoOp)
}

pub fn with_query_cache_log<Log: log::AbstractLog>(
    builder: SessionStateBuilder,
    config: QueryCacheConfig,
    log: Log,
) -> SessionStateBuilder {
    let config = Arc::new(config);
    builder
        .with_query_planner(Arc::new(QueryCacheQueryPlanner::new(log.clone(), config.clone())))
        .with_optimizer_rule(Arc::new(QCAggregateOptimizerRule::new(log, config)))
}

#[derive(Debug)]
struct QueryCacheQueryPlanner<Log: log::AbstractLog> {
    log: Log,
    config: Arc<QueryCacheConfig>,
}

impl<Log: log::AbstractLog> QueryCacheQueryPlanner<Log> {
    pub fn new(log: Log, config: Arc<QueryCacheConfig>) -> Self {
        Self { log, config }
    }
}

#[async_trait]
impl<Log: log::AbstractLog> QueryPlanner for QueryCacheQueryPlanner<Log> {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>> = vec![Arc::new(QCAggregateExecPlanner::new(
            self.log.clone(),
            self.config.clone(),
        ))];

        DefaultPhysicalPlanner::with_extension_planners(planners)
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}
