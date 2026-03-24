```sql vessel_stats
SELECT
    vessel_name,
    count(*) as total_trips,
    sum(distance_nm) as total_nm
FROM main_gold.voyages
WHERE mmsi = '${params.mmsi}'
GROUP BY ALL
```

# Vessel Profile: <Value data={vessel_stats} column=vessel_name />

<Grid cols=2>
    <Value data={vessel_stats} column=total_trips title="Total Trips" />
    <Value data={vessel_stats} column=total_nm title="Total Distance" />
</Grid>

## Route History

```sql history
SELECT dep_locode, arr_locode, dep_time, duration_hrs
FROM main_gold.voyages
WHERE mmsi = '${params.mmsi}'
ORDER BY dep_time DESC
```

<DataTable data={history} />
