use std::any::Any;
use std::fmt;
use std::fmt::Formatter;
use std::hash::Hash;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::util::pretty::print_batches;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{plan_err, Column, DFSchemaRef, Result as DataFusionResult};
use datafusion::execution::{SendableRecordBatchStream, SessionState, TaskContext};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    Aggregate, Between, BinaryExpr, Expr, Extension, LogicalPlan, Operator, UserDefinedLogicalNode,
    UserDefinedLogicalNodeCore,
};
use datafusion::optimizer::optimizer::ApplyOrder;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{collect, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use futures::TryFutureExt;

use crate::QueryCacheConfig;

#[derive(Debug)]
pub(crate) struct QCAggregateOptimizerRule {
    config: Arc<QueryCacheConfig>,
}

impl QCAggregateOptimizerRule {
    pub fn new(config: Arc<QueryCacheConfig>) -> Self {
        Self { config }
    }
}

impl QCAggregateOptimizerRule {
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

impl OptimizerRule for QCAggregateOptimizerRule {
    fn name(&self) -> &str {
        "query-cache-agg-group-by"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    // Example rewrite pass to insert a user defined LogicalPlanNode
    fn rewrite(&self, plan: LogicalPlan, _config: &dyn OptimizerConfig) -> DataFusionResult<Transformed<LogicalPlan>> {
        // println!("rewrite -> {}", plan.display());
        let LogicalPlan::Aggregate(agg) = &plan else {
            // not an aggregation, continue rewrite
            return Ok(Transformed::no(plan));
        };

        let Aggregate { input, group_expr, .. } = agg;
        let agg_input = input.as_ref().clone();
        let mut temporal_group_bys = group_expr.iter().filter_map(|e| self.find_temporal_group_by(e));

        let Some(temporal_group_by) = temporal_group_bys.next() else {
            // no temporal group by, do nothing
            eprintln!("no temporal group by, do nothing");
            return Ok(Transformed::no(plan));
        };
        if temporal_group_bys.next().is_some() {
            // multiple group bys using temporal columns!
            // I've no idea if this is even possible, and what we could do if it is, do nothing for now
            eprintln!("multiple group bys using temporal columns!");
            return Ok(Transformed::no(plan));
        }

        let dynamic_lower_bound = if let LogicalPlan::Filter(filter) = &agg_input {
            match DynamicLowerBound::find(&filter.predicate, &temporal_group_by) {
                DynamicLowerBound::Found(bin_expr) => Some(bin_expr),
                DynamicLowerBound::Stable => None,
                _ => {
                    // we found an unstable expression, we can't rewrite the plan
                    eprintln!("we found an unstable expression, we can't rewrite the plan");
                    return Ok(Transformed::no(plan));
                }
            }
        } else {
            None
        };
        // TODO, maybe we need to check the input is a table scan?
        return Ok(Transformed::yes(LogicalPlan::Extension(Extension {
            node: Arc::new(QCAggregatePlanNode::new(
                plan.clone(),
                temporal_group_by,
                dynamic_lower_bound,
            )?),
        })));
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
struct QCAggregatePlanNode {
    input: LogicalPlan,
    unique_desc: String,
    temporal_group_by: Column,
    dynamic_lower_bound: Option<BinaryExpr>,
}

impl fmt::Display for QCAggregatePlanNode {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl QCAggregatePlanNode {
    fn new(
        input: LogicalPlan,
        temporal_group_by: Column,
        dynamic_lower_bound: Option<BinaryExpr>,
    ) -> DataFusionResult<Self> {
        if let LogicalPlan::Extension(e) = input {
            if let Some(node) = e.node.as_any().downcast_ref::<QCAggregatePlanNode>() {
                // already a `QCAggregatePlanNode`, return it
                Ok(node.clone())
            } else {
                plan_err!("unexpected extension node, expected QCAggregatePlanNode")
            }
        } else if matches!(input, LogicalPlan::Aggregate(..)) {
            let unique_desc = format!("QueryCacheAggregate: {}", input.display_indent_schema());
            Ok(Self {
                input,
                unique_desc,
                temporal_group_by,
                dynamic_lower_bound,
            })
        } else {
            plan_err!("unexpected input to QCAggregatePlanNode, mut be Aggregate or Extension(QCAggregatePlanNode)")
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
        // this should not include expressions from the input plan
        let mut expressions = vec![Expr::Column(self.temporal_group_by.clone())];
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
            return plan_err!("expected temporal column");
        };
        let dynamic_lower_bound = if let Some(expr) = iter_exprs.next() {
            if iter_exprs.next().is_some() {
                return plan_err!("too many expressions");
            }

            if let Expr::BinaryExpr(dlb) = expr {
                Some(dlb)
            } else {
                return plan_err!("expected binary expression");
            }
        } else {
            None
        };

        let mut iter_inputs = inputs.into_iter();
        let Some(input) = iter_inputs.next() else {
            return plan_err!("expected one input");
        };
        if iter_inputs.next().is_some() {
            plan_err!("too many inputs")
        } else {
            Self::new(input, column, dynamic_lower_bound)
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct QCAggregateExecPlanner;

#[async_trait]
impl ExtensionPlanner for QCAggregateExecPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> DataFusionResult<Option<Arc<dyn ExecutionPlan>>> {
        let Some(group_by_node) = node.as_any().downcast_ref::<QCAggregatePlanNode>() else {
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
            // TODO if this can actually happen return `Ok(Some(expr))`
            return plan_err!("QueryCacheGroupByExec expected one AggregateExec input");
        };

        let input_exec = QCInnerAggregateExec::new(
            agg_exec.input().clone(),
            group_by_node.temporal_group_by.clone(),
            group_by_node.dynamic_lower_bound.clone(),
        )?;

        // let input_exec = Arc::new(CoalescePartitionsExec::new(input_exec));
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

#[derive(Debug)]
struct QCInnerAggregateExec {
    input: Arc<dyn ExecutionPlan>,
    temporal_group_by: Column,
    dynamic_lower_bound: Option<BinaryExpr>,
    cache: PlanProperties,
}

impl QCInnerAggregateExec {
    fn new(
        input: Arc<dyn ExecutionPlan>,
        temporal_group_by: Column,
        dynamic_lower_bound: Option<BinaryExpr>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let cache = input.properties().clone();

        let input = if input.name() == "CoalescePartitionsExec" {
            input
        } else {
            Arc::new(CoalescePartitionsExec::new(input))
        };

        Ok(Arc::new(Self {
            input,
            temporal_group_by,
            dynamic_lower_bound,
            cache,
        }))
    }
}

impl DisplayAs for QCInnerAggregateExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default => write!(f, "{}({})", self.name(), self.input.name()),
            DisplayFormatType::Verbose => write!(f, "{:?}", self),
        }
    }
}

#[async_trait]
impl ExecutionPlan for QCInnerAggregateExec {
    fn name(&self) -> &str {
        "QueryCacheAggregateExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() == 1 {
            Self::new(
                children[0].clone(),
                self.temporal_group_by.clone(),
                self.dynamic_lower_bound.clone(),
            )
        } else {
            plan_err!("QueryCacheGroupByExec expected one child")
        }
    }

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> DataFusionResult<SendableRecordBatchStream> {
        assert_eq!(partition, 0, "QCAggregateExec does not support partitioning");
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.input.schema(),
            async_execute(self.input.clone(), context)
                .map_ok(|partitions| futures::stream::iter(partitions.into_iter().map(Ok)))
                .try_flatten_stream(),
        )))
    }
}

async fn async_execute(input: Arc<dyn ExecutionPlan>, context: Arc<TaskContext>) -> DataFusionResult<Vec<RecordBatch>> {
    // TODO store the result for future use
    let batches = collect(input, context).await?;
    print_batches(&batches)?;
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
    fn find(expr: &Expr, column: &Column) -> Self {
        match expr {
            Expr::BinaryExpr(bin_expr) => Self::find_bin_expr(bin_expr, column),
            Expr::Between(between) => Self::find_between(between, column),
            Expr::Literal(_)
            | Expr::Like(_)
            | Expr::IsNotNull(_)
            | Expr::IsNull(_)
            | Expr::IsTrue(_)
            | Expr::IsFalse(_)
            | Expr::IsNotTrue(_)
            | Expr::IsNotFalse(_) => Self::Stable,
            Expr::Not(e) | Expr::Negative(e) => match Self::find(e, column) {
                Self::Stable => Self::Stable,
                _ => Self::Abandon,
            },
            Expr::ScalarFunction(scalar) => Self::find_scalar_function(scalar),
            Expr::Column(_) => Self::Stable,
            // TODO there are other allowed cases
            _ => Self::Abandon,
        }
    }

    fn find_bin_expr(bin_expr: &BinaryExpr, column: &Column) -> Self {
        let BinaryExpr { left, op, right } = bin_expr;
        match op {
            Operator::Gt | Operator::GtEq => {
                // expression of the form `left >(=) right`, for this to be a lower bound:
                // `left` must be the column
                // and `right` must be a dynamic bound
                if let Expr::Column(col) = left.as_ref() {
                    if col == column {
                        return match Self::find(right, column) {
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
                    if col == column {
                        return match Self::find(left, column) {
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

        let left = Self::find(left, column);
        let right = Self::find(right, column);
        return left.either(right);
    }

    fn find_between(_between: &Between, _column: &Column) -> Self {
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
