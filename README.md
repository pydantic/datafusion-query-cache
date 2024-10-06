# Datafusion Query Cache

**WIP this project is very early.**

Cache the intermediate results of queries on timeseries data in DataFusion.

## How it works (the very quick version)

Let's say you run the query:

```sql
SELECT max(price) FROM stock_prices WHERE symbol = 'AAPL' and timestamp > '2000-01-01'
```

Then 10 minutes later you run the same query — by default DataFusion will process every one of the millions of record in the `stock_prices` table again to calculate the result, even though only the last 10 minutes of data has changed.

Obvious we could save a lot of time and compute if we could remember the result of the first query, then combining it with a query on the last 10 minutes of data to get a result.

**That's what `datafusion-query-cache` does!**

The key is that often in timeseries data, new data is inserted with a `timestamp` column that is close to `now()`, so it's trivial to know what results we can cache and what results we must recompute.

`datafusion-query-cache` doesn't have opinions about where the cached data is stored, instead you need to implement the `QueryCache` trait to store data. A very simple `MemoryQueryCache` is provided for testing, we should add `ObjectStoreQueryCache` too.

## How it works (the longer version)

Some people reading the above example will already being asking

> But combining max values is easy (you just take the max of the maxes), what about more complex queries?
> If we had used `avg` instead of `max` you can't combine two averages by just averaging them.

The best bit is: DataFusion already has all the machinery to combine partial query results, so `datafusion-query-cache` doesn't need any special logic for different aggregations, indeed it doesn't even know what they are.

Instead we just hook into the right place in the physical plan to provide the cached results, constrain the query on new data and store the new result.

### Let's look at an example

The physical plan for

```sql
SELECT avg(price) FROM stock_prices WHERE symbol = 'AAPL' and timestamp > '2000-01-01'
```

looks something like this (lots of details omitted):

```rs
AggegateExec {
    mode: Final,
    aggr_expr: [Avg(price)],
    input: AggegateExec {
        mode: Parital,
        aggr_expr: [Avg(price)],
        input: FilterExec {
            predicate: (symbol = 'AAPL' and timestamp > '2000-01-01'),
            input: TableScanExec {
                table: stock_prices
            }
        }
    }
}
```

Notice how the `input` for the top level `AggegateExec` is another `AggegateExec`? That's DataFusion allowing parallel execution by splitting the data into chunks and aggregating them separately. The output of the inner `AggegateExec` (note `mode: Parital`) will look something like:

| `avg(price)[count]` | `avg(price)[sum]` |
|---------------------|-------------------|
| 123.4               | 1000              |
| 125.4               | 1000              |
| 127.4               | 1000              |
| ...                 | ...               |

The top level `AggegateExec` with (`mode: Final`), then combines these partial results to get the final answer.

This "combine partial results" is exactly what `datafusion-query-cache` uses to combine the cached result with the new data.

So `datafusion-query-cache`, would rewrite the above query to have the following physical plan:

```rs
AggegateExec {
    mode: Final,
    aggr_expr: [Avg(price)],
    input: CacheUpdateAggregateExec {  // wrap the partial aggegations and stores the result for later
        input: UnionExec {
            inputs: [
                AggegateExec {  // compute aggegates for the new data
                    mode: Parital,
                    aggr_expr: [Avg(price)],
                    input: FilterExec {
                        predicate: ((symbol = 'AAPL' and timestamp > '2000-01-01') and timestamp < '{last run}'),
                        input: TableScanExec {
                            table: stock_prices
                        }
                    }
                },
                CachedAggregateExec {  // get the cached result
                    cache_key: "SELECT avg(price)...",
                }
            ]
        }
    }
}
```

The beauty is, if we wrote a more complex query, say:

```sql
SELECT
    date_trunc('hour', timestamp) AS time_bucket,
    round(avg(value), 2) as avg_value,
    round(min(value), 2) as min_value,
    round(max(value), 2) as max_value
FROM stock_prices
WHERE symbol = 'AAPL' AND timestamp > '2000-01-01'
GROUP BY time_bucket
ORDER BY time_bucket DESC
```

`datafusion-query-cache` doesn't need to be any cleverer, DataFusion does the hard work of combining the partial results, even accounting for the different buckets and aggregations and combining them correctly.

## Prior art

Other database have similar concepts, e.g. [continuous aggregates](https://docs.timescale.com/use-timescale/latest/continuous-aggregates/) in TimeScaleDB, but they require explicit setup. In contrast, `datafusion-query-cache` analyses queries (including subqueries) and automatically applies the cache if it can.

## What's supported

* [x] `GROUP BY` aggregation queries with a static lower bound (or no lower bound)
* [x] Aggregation queries (no `GROUP BY`) with a static lower bound (or no lower bound)
* [ ] Simple filter queries — this should be simple enough
* [ ] `GROUP BY` aggregation queries with a dynamic lower bound (e.g . `timestamp > now() - interval '1 day'`) - this requires a `FilterExec` wrapping the `UnionExec` and discarding older data
* [ ] Aggregation queries (no `GROUP BY`) with a dynamic lower bound - this is harder, we probably have to rewrite the aggregation to include a `group_by` clause, then filter, then aggregate again???

## How to use

`datafusion-query-cache` implements [`QueryPlanner`](https://docs.rs/datafusion/latest/datafusion/execution/context/trait.QueryPlanner.html), [`OptimizerRule`](https://docs.rs/datafusion/latest/datafusion/optimizer/trait.OptimizerRule.html), [`UserDefinedLogicalNodeCore`](https://docs.rs/datafusion/latest/datafusion/logical_expr/trait.UserDefinedLogicalNodeCore.html) and [`ExecutionPlan`](https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html) to customise query execution.

Usage is as simple as calling `with_query_cache` on a `SessionStateBuilder`, here's a complete (if minimal) example of creating a `SessionContext`:

```rs
async fn session_ctx() -> SessionContext {
    let state_builder = SessionStateBuilder::new()
        .with_config(SessionConfig::new())
        .with_runtime_env(Arc::new(RuntimeEnv::default()))
        .with_default_features();

    // records.timetamp is the default (and only) temporal column to look at
    let temporal_col = Column::new(Some("records".to_string()), "timestamp".to_string());

    // create an in memory cache for the query results
    // (in reality, you'd want to impl the `QueryCache` trait and store the data somewhere persistent)
    let cache = Arc::new(datafusion_query_cache::MemoryQueryCache::default());

    // create the query cache config
    let query_cache_config = datafusion_query_cache::QueryCacheConfig::new(temporal_col, cache)
        .with_group_by_function("date_trunc");

    // call with_query_cache to register the planners and optimizers
    let state_builder = datafusion_query_cache::with_query_cache(state_builder, query_cache_config);
    SessionContext::new_with_state(state_builder.build())
}
```

See [`examples/demo.rs](./examples/demo.rs) for a more complete working example.
