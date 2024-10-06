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
pub use log::{LogNoOp, LogStderrColors};
use cache::QueryCache;

#[derive(Debug)]
pub struct QueryCacheConfig {
    temporal_columns: HashSet<Column>,
    group_by_functions: HashSet<String>,
    cache: Arc<dyn QueryCache>,
}

impl QueryCacheConfig {
    pub fn new(cache: Arc<dyn QueryCache>) -> Self {
        Self {
            cache,
            temporal_columns: HashSet::new(),
            group_by_functions: HashSet::new(),
        }
    }

    pub fn with_temporal_column(mut self, column: Column) -> Self {
        self.temporal_columns.insert(column);
        self
    }
    pub fn with_temporal_column_table_col(
        mut self,
        table_name: impl Into<String>,
        column_name: impl Into<String>,
    ) -> Self {
        let column = Column::new(Some(table_name.into()), column_name.into());
        self.temporal_columns.insert(column);
        self
    }

    pub fn with_group_by_function(mut self, function: impl Into<String>) -> Self {
        self.group_by_functions.insert(function.into());
        self
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

pub fn with_query_cache_log<Log: log::AbstractLog>(builder: SessionStateBuilder, config: QueryCacheConfig, log: Log) -> SessionStateBuilder {
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
        let planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>> =
            vec![Arc::new(QCAggregateExecPlanner::new(self.log.clone(), self.config.clone()))];

        DefaultPhysicalPlanner::with_extension_planners(planners)
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}
