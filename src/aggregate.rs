use std::any::Any;
use std::borrow::Cow;
use std::collections::HashSet;
use std::fmt;
use std::fmt::Formatter;
use std::hash::Hash;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion::common::tree_node::Transformed;
use datafusion::common::{internal_err, plan_err, Column, DFSchemaRef, Result as DataFusionResult, ScalarValue};
use datafusion::execution::{SendableRecordBatchStream, SessionState, TaskContext};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    Aggregate, Between, BinaryExpr, Expr, Extension, Filter, LogicalPlan, Operator, TableScan, UserDefinedLogicalNode,
    UserDefinedLogicalNodeCore,
};
use datafusion::optimizer::optimizer::ApplyOrder;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::physical_expr::expressions::{
    BinaryExpr as PhysicalBinaryExpr, Column as PhysicalColumn, Literal as PhysicalLiteral,
};
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{collect, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use futures::TryFutureExt;

use crate::cache::{CacheEntry, OccupiedCacheEntry};
use crate::log::{log_info, log_warn, AbstractLog};
use crate::QueryCacheConfig;

#[derive(Debug)]
pub(crate) struct QCAggregateOptimizerRule<Log: AbstractLog> {
    log: Log,
    config: Arc<QueryCacheConfig>,
}

impl<Log: AbstractLog> QCAggregateOptimizerRule<Log> {
    pub fn new(log: Log, config: Arc<QueryCacheConfig>) -> Self {
        Self { log, config }
    }

    /// Find a column used in a group by expression that matches one of our temporal columns
    fn find_temporal_group_by(&self, expr: &Expr) -> Option<Column> {
        let Expr::ScalarFunction(ScalarFunction { func, args }) = expr else {
            return None;
        };
        if !self.config.allow_group_by_function(func.name()) {
            return None;
        }
        let second_arg = args.get(1)?;

        if let Expr::Column(column) = second_arg {
            if self.config.allow_temporal_column(column) {
                return Some(column.clone());
            }
        }

        None
    }
}
impl<Log: AbstractLog> OptimizerRule for QCAggregateOptimizerRule<Log> {
    fn name(&self) -> &str {
        "query-cache-agg-group-by"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    // Example rewrite pass to insert a user defined LogicalPlanNode
    fn rewrite(
        &self,
        mut plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DataFusionResult<Transformed<LogicalPlan>> {
        // println!("rewrite -> {}", plan.display());
        let LogicalPlan::Aggregate(agg) = &plan else {
            // not an aggregation, continue rewrite
            return Ok(Transformed::no(plan));
        };
        let mut fingerprint = plan.display_indent_schema().to_string();

        let Aggregate { input, group_expr, .. } = agg;
        let agg_input = input.as_ref().clone();
        let mut temporal_group_bys = group_expr.iter().filter_map(|e| self.find_temporal_group_by(e));

        let temporal_group_by = temporal_group_bys.next();

        if temporal_group_bys.next().is_some() {
            // I've no idea if this is even possible, and what we could do if it is, do nothing for now
            self.log.info(
                &fingerprint,
                "multiple group bys using temporal columns, caching not possible!",
            )?;
            return Ok(Transformed::no(plan));
        }

        let (dynamic_lower_bound, input) = if let LogicalPlan::Filter(filter) = &agg_input {
            let needle_columns = if let Some(temporal_group_by) = &temporal_group_by {
                Cow::Owned(HashSet::from([temporal_group_by.clone()]))
            } else {
                Cow::Borrowed(&self.config.temporal_columns)
            };

            let dlb = match DynamicLowerBound::find(&filter.predicate, &needle_columns) {
                DynamicLowerBound::Found(bin_expr) => Some(bin_expr),
                DynamicLowerBound::Stable => None,
                _ => {
                    // we found an unstable expression, we can't rewrite the plan
                    self.log
                        .info(&fingerprint, "we found an unstable expression, caching not possible")?;
                    return Ok(Transformed::no(plan));
                }
            };
            (dlb, filter.input.as_ref().clone())
        } else {
            (None, agg_input.clone())
        };

        if temporal_group_by.is_none() {
            // if temporal_group_by is none, we need to make sure the sort column is in the projection
            let LogicalPlan::TableScan(scan) = input else {
                // TODO we need to support this, e.g. a subquery
                self.log
                    .info(&fingerprint, "input not a table scan, caching not possible")?;
                return Ok(Transformed::no(plan));
            };
            // TODO check table name
            let field_name = self.config.default_temporal_column().name.clone();
            if !scan.projected_schema.fields().iter().any(|f| f.name() == &field_name) {
                let new_col_id = scan
                    .source
                    .schema()
                    .fields()
                    .iter()
                    .enumerate()
                    .find_map(|(id, f)| (f.name() == &field_name).then_some(id));

                let Some(new_col_id) = new_col_id else {
                    log_info!(
                        self.log,
                        &fingerprint,
                        "sort column '{}' not found in table, caching not possible",
                        field_name
                    );
                    return Ok(Transformed::no(plan));
                };

                let mut new_projection = scan.projection.expect("no projection found");
                new_projection.push(new_col_id);
                new_projection.sort_unstable();

                let new_table_scan = TableScan::try_new(
                    scan.table_name,
                    scan.source,
                    Some(new_projection),
                    scan.filters,
                    scan.fetch,
                )
                .map(LogicalPlan::TableScan)?;

                let inner_plan = if let LogicalPlan::Filter(filter) = &agg_input {
                    LogicalPlan::Filter(Filter::try_new(filter.predicate.clone(), Arc::new(new_table_scan))?)
                } else {
                    new_table_scan
                };
                plan = LogicalPlan::Aggregate(Aggregate::try_new(
                    Arc::new(inner_plan),
                    agg.group_expr.clone(),
                    agg.aggr_expr.clone(),
                )?);
                fingerprint = plan.display_indent_schema().to_string();
            }
        }

        // if dynamic_lower_bound.is_some() && temporal_group_by.is_none() {
        //     self.log.info(
        //         &fingerprint,
        //         "found a dynamic lower bound but no temporal group by, caching not possible",
        //     )?;
        //     return Ok(Transformed::no(plan));
        // }
        if dynamic_lower_bound.is_some() {
            return plan_err!("dynamic lower bound not yet supported");
        }

        let temporal_column = temporal_group_by.unwrap_or_else(|| self.config.default_temporal_column().clone());

        // do we need to check the input is a table scan?
        log_info!(
            self.log,
            &fingerprint,
            "query valid for caching, sort column {}",
            temporal_column
        );
        Ok(Transformed::yes(LogicalPlan::Extension(Extension {
            node: Arc::new(QCAggregatePlanNode::new(
                plan.clone(),
                temporal_column,
                dynamic_lower_bound,
                Some(fingerprint),
            )?),
        })))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
struct QCAggregatePlanNode {
    input: LogicalPlan,
    fingerprint: String,
    temporal_column: Column,
    dynamic_lower_bound: Option<BinaryExpr>,
}

impl fmt::Display for QCAggregatePlanNode {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

/// Placeholder for th cached aggregation in the logical plan, there's no logic here, just boilerplate
impl QCAggregatePlanNode {
    fn new(
        input: LogicalPlan,
        temporal_column: Column,
        dynamic_lower_bound: Option<BinaryExpr>,
        fingerprint: Option<String>,
    ) -> DataFusionResult<Self> {
        if let LogicalPlan::Extension(e) = input {
            if let Some(node) = e.node.as_any().downcast_ref::<QCAggregatePlanNode>() {
                // already a `QCAggregatePlanNode`, return it
                Ok(node.clone())
            } else {
                plan_err!("unexpected extension node, expected QCAggregatePlanNode")
            }
        } else if matches!(input, LogicalPlan::Aggregate(..)) {
            let fingerprint = fingerprint.unwrap_or_else(|| input.display_indent_schema().to_string());
            Ok(Self {
                input,
                fingerprint,
                temporal_column,
                dynamic_lower_bound,
            })
        } else {
            plan_err!(
                "unexpected input to QCAggregatePlanNode, mut be Aggregate or Extension(QCAggregatePlanNode), got {}",
                input.display()
            )
        }
    }
}

impl UserDefinedLogicalNodeCore for QCAggregatePlanNode {
    fn name(&self) -> &str {
        "QueryCacheAggregate"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        let mut expressions = vec![Expr::Column(self.temporal_column.clone())];
        if let Some(expr) = &self.dynamic_lower_bound {
            expressions.push(Expr::BinaryExpr(expr.clone()));
        }
        expressions
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "QueryCacheAggregate: {}", self.input.display())
    }

    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> DataFusionResult<Self> {
        let mut iter_exprs = exprs.into_iter();
        let Some(Expr::Column(column)) = iter_exprs.next() else {
            return plan_err!("UserDefinedLogicalNodeCore  expected temporal column as first expressoin");
        };
        let dynamic_lower_bound = if let Some(expr) = iter_exprs.next() {
            if iter_exprs.next().is_some() {
                return plan_err!("UserDefinedLogicalNodeCore expected one or two expressions");
            }

            if let Expr::BinaryExpr(dlb) = expr {
                Some(dlb)
            } else {
                return plan_err!("UserDefinedLogicalNodeCore expected binary expression as second expression");
            }
        } else {
            None
        };

        let mut iter_inputs = inputs.into_iter();
        let Some(input) = iter_inputs.next() else {
            return plan_err!("UserDefinedLogicalNodeCore expected one inputs");
        };
        if iter_inputs.next().is_some() {
            plan_err!("UserDefinedLogicalNodeCore expected one inputs")
        } else {
            Self::new(input, column, dynamic_lower_bound, None)
        }
    }
}

/// A physical planner that knows how to convert a `QCAggregatePlanNode` into physical plan,
/// using `QCInnerAggregateExec`.
#[derive(Debug)]
pub(crate) struct QCAggregateExecPlanner<Log: AbstractLog> {
    log: Log,
    config: Arc<QueryCacheConfig>,
}

impl<Log: AbstractLog> QCAggregateExecPlanner<Log> {
    pub fn new(log: Log, config: Arc<QueryCacheConfig>) -> Self {
        Self { log, config }
    }
}

#[async_trait]
impl<Log: AbstractLog> ExtensionPlanner for QCAggregateExecPlanner<Log> {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> DataFusionResult<Option<Arc<dyn ExecutionPlan>>> {
        let Some(agg_node) = node.as_any().downcast_ref::<QCAggregatePlanNode>() else {
            return Ok(None);
        };
        if physical_inputs.len() != 1 {
            // maybe Ok(None) is ok here?
            return plan_err!("QueryCacheGroupByExec expected one input");
        }

        let exec = physical_inputs[0].clone();

        if find_existing_inner_exec(&exec) {
            // already a `QCInnerAggregateExec` (or contains a `QCInnerAggregateExec`), return it
            return Ok(Some(exec));
        }

        let Some(agg_exec): Option<&AggregateExec> = exec.as_any().downcast_ref() else {
            // should this be an error?
            log_warn!(
                self.log,
                &agg_node.fingerprint,
                "QueryCacheGroupByExec expected an AggregateExec input, found {}",
                exec.name()
            );
            return Ok(Some(exec));
        };

        let cache_entry = self.config.cache().entry(&agg_node.fingerprint).await?;
        log_info!(
            self.log,
            &agg_node.fingerprint,
            "cache entry hit {:?}",
            cache_entry.occupied()
        );

        let now = self.config.override_now.unwrap_or_else(|| {
            session_state
                .execution_props()
                .query_execution_start_time
                .timestamp_nanos_opt()
                // we'll be in trouble after 2262!
                .unwrap()
        });

        let partial_agg_exec = agg_exec.input().clone();

        let input_exec = match &cache_entry {
            CacheEntry::Occupied(entry) => {
                let cached_exec = CachedAggregateExec::new_exec_plan(entry.clone(), partial_agg_exec.properties());
                let new_exec = with_lower_bound(&partial_agg_exec, &agg_node.temporal_column, entry.timestamp())?;

                let combined_input = Arc::new(UnionExec::new(vec![cached_exec, new_exec]));
                Arc::new(CoalescePartitionsExec::new(combined_input))
            }
            CacheEntry::Vacant(_) => partial_agg_exec,
        };

        // whether or not we had a cache hit, we wrap the input in a `CacheUpdateAggregateExec`
        // to store the complete (but partial) aggregation
        let input_exec = CacheUpdateAggregateExec::new_exec_plan(cache_entry, input_exec, now);
        let input_schema = input_exec.schema();

        Ok(Some(Arc::new(AggregateExec::try_new(
            AggregateMode::Final,
            agg_exec.group_expr().clone(),
            agg_exec.aggr_expr().to_vec(),
            agg_exec.filter_expr().to_vec(),
            input_exec,
            input_schema,
        )?)))
    }
}

/// apply a lower bound to an `AggregateExec`
fn with_lower_bound(
    partial_agg_exec: &Arc<dyn ExecutionPlan>,
    bound_column: &Column,
    lower_bound_ns: i64,
) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
    let Some(agg_exec): Option<&AggregateExec> = partial_agg_exec.as_any().downcast_ref() else {
        return plan_err!("expected an AggregateExec input, found {}", partial_agg_exec.name());
    };

    let find_column = agg_exec
        .input()
        .schema()
        .fields()
        .iter()
        .enumerate()
        .find_map(|(id, f)| {
            if f.name() == &bound_column.name {
                if let DataType::Timestamp(time_unit, _) = f.data_type() {
                    let lower_bound = match time_unit {
                        TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(Some(lower_bound_ns), None),
                        TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(Some(lower_bound_ns / 1000), None),
                        TimeUnit::Millisecond => {
                            ScalarValue::TimestampMillisecond(Some(lower_bound_ns / 1_000_000), None)
                        }
                        TimeUnit::Second => ScalarValue::TimestampSecond(Some(lower_bound_ns / 1_000_000_000), None),
                    };
                    Some((id, lower_bound))
                } else {
                    None
                }
            } else {
                None
            }
        });

    let Some((column_id, lower_bound_scalar)) = find_column else {
        return plan_err!("Timestamp column '{}' not found in input schema", bound_column.name);
    };

    let lower_bound_predicate = Arc::new(PhysicalBinaryExpr::new(
        Arc::new(PhysicalColumn::new(&bound_column.name, column_id)),
        Operator::GtEq,
        Arc::new(PhysicalLiteral::new(lower_bound_scalar)),
    ));
    let filter_exec = if let Some(filter) = agg_exec.input().as_any().downcast_ref::<FilterExec>() {
        let new_predicate = PhysicalBinaryExpr::new(filter.predicate().clone(), Operator::And, lower_bound_predicate);
        let new_filter = FilterExec::try_new(Arc::new(new_predicate), filter.input().clone())?;
        new_filter.with_projection(filter.projection().cloned())?
    } else {
        FilterExec::try_new(lower_bound_predicate, agg_exec.input().clone())?
    };

    let input_schema = filter_exec.schema();

    Ok(Arc::new(AggregateExec::try_new(
        *agg_exec.mode(),
        agg_exec.group_expr().clone(),
        agg_exec.aggr_expr().to_vec(),
        agg_exec.filter_expr().to_vec(),
        Arc::new(filter_exec),
        input_schema,
    )?))
}

/// check for an existing `QCInnerAggregateExec` in the plan
fn find_existing_inner_exec(plan: &Arc<dyn ExecutionPlan>) -> bool {
    match plan.name() {
        "QueryCacheAggregateExec" => true,
        "CoalescePartitionsExec" => {
            let coalesce = plan.as_any().downcast_ref::<CoalescePartitionsExec>().unwrap();
            find_existing_inner_exec(coalesce.input())
        }
        "AggregateExec" => {
            let agg = plan.as_any().downcast_ref::<AggregateExec>().unwrap();
            find_existing_inner_exec(agg.input())
        }
        _ => false,
        // name => {
        //     dbg!(name);
        //     false
        // }
    }
}

/// A wrapper for `AggregateExec` that caches the result of the aggregation
#[derive(Debug)]
struct CacheUpdateAggregateExec {
    cache_entry: CacheEntry,
    input: Arc<dyn ExecutionPlan>,
    /// from `session_state.execution_props().query_execution_start_time.timestamp_nanos_opt()`
    now: i64,
    properties: PlanProperties,
    metrics: ExecutionPlanMetricsSet,
}

impl CacheUpdateAggregateExec {
    fn new_exec_plan(cache_entry: CacheEntry, input: Arc<dyn ExecutionPlan>, now: i64) -> Arc<dyn ExecutionPlan> {
        // we need one partition so we can store one result in the cache, use `CoalescePartitionsExec`
        // if input is not already one
        let input = if input.name() == "CoalescePartitionsExec" {
            input
        } else {
            Arc::new(CoalescePartitionsExec::new(input))
        };
        let properties = input.properties().clone();

        Arc::new(Self {
            cache_entry,
            input,
            now,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl DisplayAs for CacheUpdateAggregateExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default => write!(f, "{}({})", self.name(), self.input.name()),
            DisplayFormatType::Verbose => write!(f, "{self:?}"),
        }
    }
}

#[async_trait]
impl ExecutionPlan for CacheUpdateAggregateExec {
    fn name(&self) -> &str {
        "CacheUpdateAggregateExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() == 1 {
            Ok(Self::new_exec_plan(
                self.cache_entry.clone(), // TODO is it safe to reuse the cache entry?
                children[0].clone(),
                self.now,
            ))
        } else {
            internal_err!("CacheUpdateAggregateExec expected one child")
        }
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> DataFusionResult<SendableRecordBatchStream> {
        assert_eq!(partition, 0, "CacheUpdateAggregateExec does not support partitioning");
        let metrics = BaselineMetrics::new(&self.metrics, partition);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.input.schema(),
            execute_store(self.input.clone(), self.cache_entry.clone(), self.now, context, metrics)
                .map_ok(|partitions| futures::stream::iter(partitions.into_iter().map(Ok)))
                .try_flatten_stream(),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

async fn execute_store(
    input: Arc<dyn ExecutionPlan>,
    cache_entry: CacheEntry,
    now: i64,
    context: Arc<TaskContext>,
    metrics: BaselineMetrics,
) -> DataFusionResult<Vec<RecordBatch>> {
    let batches = collect(input, context).await?;
    // store the result for future use
    cache_entry.put(now, &batches).await?;
    metrics.record_output(batches.iter().map(RecordBatch::num_rows).sum());
    metrics.done();
    Ok(batches)
}

/// A wrapper for `AggregateExec` that caches the result of the aggregation
#[derive(Debug)]
struct CachedAggregateExec {
    cache_entry: Arc<dyn OccupiedCacheEntry>,
    schema: SchemaRef,
    properties: PlanProperties,
    metrics: ExecutionPlanMetricsSet,
}

impl CachedAggregateExec {
    fn new_exec_plan(
        cache_entry: Arc<dyn OccupiedCacheEntry>,
        inner_properties: &PlanProperties,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(Self {
            cache_entry,
            schema: inner_properties.eq_properties.schema().clone(),
            properties: inner_properties.clone(),
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl DisplayAs for CachedAggregateExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default => write!(f, "{}", self.name()),
            DisplayFormatType::Verbose => write!(f, "{self:?}"),
        }
    }
}

#[async_trait]
impl ExecutionPlan for CachedAggregateExec {
    fn name(&self) -> &str {
        "CachedAggregateExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            internal_err!("Children cannot be replaced in {}", self.name())
        }
    }

    fn execute(&self, partition: usize, _context: Arc<TaskContext>) -> DataFusionResult<SendableRecordBatchStream> {
        assert_eq!(partition, 0, "CachedAggregateExec does not support partitioning");
        let metrics = BaselineMetrics::new(&self.metrics, partition);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            execute_get(self.cache_entry.clone(), metrics)
                .map_ok(|partitions| futures::stream::iter(partitions.into_iter().map(Ok)))
                .try_flatten_stream(),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

async fn execute_get(
    cache_entry: Arc<dyn OccupiedCacheEntry>,
    metrics: BaselineMetrics,
) -> DataFusionResult<Vec<RecordBatch>> {
    let batches = cache_entry.get().await.map(<[RecordBatch]>::to_vec)?;
    metrics.record_output(batches.iter().map(RecordBatch::num_rows).sum());
    metrics.done();
    Ok(batches)
}

/// Find a binary expression which must have the form `{column} >(=) {something with now()}` represents a
/// lower bound on `column` but changes over time.
#[derive(Debug)]
enum DynamicLowerBound {
    /// we found an unstable expression which means we can't rewrite the plan
    Abandon,
    /// we found a suitable lower bound
    Found(BinaryExpr),
    /// we found `now()` or similar function which is allowed if within an expression which sets the lower bound
    FoundNow,
    /// we did not find a suitable lower bound, but the expression is stable
    Stable,
}

impl DynamicLowerBound {
    fn find(expr: &Expr, columns: &HashSet<Column>) -> Self {
        match expr {
            Expr::BinaryExpr(bin_expr) => Self::find_bin_expr(bin_expr, columns),
            Expr::Between(between) => Self::find_between(between, columns),
            Expr::Literal(_)
            | Expr::Like(_)
            | Expr::IsNotNull(_)
            | Expr::IsNull(_)
            | Expr::IsTrue(_)
            | Expr::IsFalse(_)
            | Expr::IsNotTrue(_)
            | Expr::IsNotFalse(_)
            | Expr::Column(_) => Self::Stable,
            Expr::Not(e) | Expr::Negative(e) => match Self::find(e, columns) {
                Self::Stable => Self::Stable,
                _ => Self::Abandon,
            },
            Expr::ScalarFunction(scalar) => Self::find_scalar_function(scalar),
            // TODO there are other allowed cases
            _ => Self::Abandon,
        }
    }

    fn find_bin_expr(bin_expr: &BinaryExpr, columns: &HashSet<Column>) -> Self {
        let BinaryExpr { left, op, right } = bin_expr;
        match op {
            Operator::Gt | Operator::GtEq => {
                // expression of the form `left >(=) right`, for this to be a lower bound:
                // `left` must be the column
                // and `right` must be a dynamic bound
                if let Expr::Column(col) = left.as_ref() {
                    if columns.contains(col) {
                        return match Self::find(right, columns) {
                            Self::Stable => Self::Stable,
                            Self::FoundNow => Self::Found(bin_expr.clone()),
                            _ => Self::Abandon,
                        };
                    }
                }
            }
            Operator::Lt | Operator::LtEq => {
                // expression of the form `left <(=) right`, for this to be a lower bound:
                // `left` must be a dynamic bound
                // and `right` must be the column
                if let Expr::Column(col) = right.as_ref() {
                    if columns.contains(col) {
                        return match Self::find(left, columns) {
                            Self::Stable => Self::Stable,
                            Self::FoundNow => {
                                let op = match op {
                                    Operator::Lt => Operator::GtEq,
                                    Operator::LtEq => Operator::Gt,
                                    _ => unreachable!(),
                                };
                                Self::Found(BinaryExpr {
                                    right: left.clone(),
                                    op,
                                    left: right.clone(),
                                })
                            }
                            _ => Self::Abandon,
                        };
                    }
                }
            }
            // AND or a simple arithmetic operation, check both sides
            Operator::And
            | Operator::Eq
            | Operator::Plus
            | Operator::Minus
            | Operator::Multiply
            | Operator::Divide
            | Operator::Modulo => (),
            _ => return Self::Abandon,
        };

        let left = Self::find(left, columns);
        let right = Self::find(right, columns);
        left.either(right)
    }

    fn find_between(_between: &Between, _columns: &HashSet<Column>) -> Self {
        todo!()
    }

    fn find_scalar_function(scalar: &ScalarFunction) -> Self {
        if matches!(scalar.name(), "now" | "current_timestamp" | "current_date") {
            Self::FoundNow
        } else {
            Self::Abandon
        }
    }

    /// Find the value which is more important
    #[allow(clippy::match_same_arms)]
    fn either(self, other: Self) -> Self {
        match (self, other) {
            (Self::Abandon, _) | (_, Self::Abandon) => Self::Abandon,
            // found on both sides, not sure what to do
            (Self::Found(..), Self::Found(..)) => Self::Abandon,
            (Self::FoundNow, _) | (_, Self::FoundNow) => Self::FoundNow,
            (Self::Stable, other) | (other, Self::Stable) => other,
        }
    }
}
