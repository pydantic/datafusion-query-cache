mod aggregate;

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::{Column, Result as DataFusionResult};
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};

use crate::aggregate::{QCAggregateExecPlanner, QCAggregateOptimizerRule};

#[derive(Debug, Default)]
pub struct QueryCacheConfig {
    temporal_columns: HashSet<Column>,
    group_by_functions: HashSet<String>,
}

impl QueryCacheConfig {
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
}

pub fn with_query_cache(
    builder: SessionStateBuilder,
    config: QueryCacheConfig,
) -> SessionStateBuilder {
    let config = Arc::new(config);
    builder
        .with_query_planner(Arc::new(QueryCacheQueryPlanner::default()))
        .with_optimizer_rule(Arc::new(QCAggregateOptimizerRule::new(config)))
}

#[derive(Debug, Default)]
struct QueryCacheQueryPlanner;

#[async_trait]
impl QueryPlanner for QueryCacheQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let planners: Vec<Arc<dyn ExtensionPlanner + Send + Sync>> =
            vec![Arc::new(QCAggregateExecPlanner::default())];

        DefaultPhysicalPlanner::with_extension_planners(planners)
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}
