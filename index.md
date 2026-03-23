```sql total_stats
SELECT
    count(*) as voyages,
    count(distinct mmsi) as vessels,
    sum(distance_nm) as total_distance
FROM main_gold.voyages
```

# Maritime Traffic Dashboard

<Grid cols=3>
    <Value data={total_stats} column=voyages title="Total Voyages" />
    <Value data={total_stats} column=vessels title="Unique Vessels" />
    <Value data={total_stats} column=total_distance title="Total Distance (NM)" />
</Grid>

## Recent Voyages

```sql recent
SELECT * FROM main_gold.voyages ORDER BY arr_time DESC LIMIT 10
```

<DataTable data={recent} />
