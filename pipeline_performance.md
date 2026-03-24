# Pipeline Operations

## Recent Run Performance

```sql run_history
SELECT
    generated_at as run_time,
    status,
    count(unique_id) as models_run,
    sum(execution_time) as total_duration_sec
FROM operations.dbt_run_results
GROUP BY 1, 2
ORDER BY 1 DESC
LIMIT 10
```

<DataTable data={run_history} />

## Bronze-to-Silver Latency

```sql latency
SELECT unique_id, execution_time, rows_affected
FROM operations.dbt_run_results
WHERE unique_id LIKE '%stg_ais%'
ORDER BY generated_at DESC
```

<BarChart data={latency} x=unique_id y=execution_time title="Staging Model Duration (s)" />
