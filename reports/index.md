---
title: Maritime Logistics Tracker
---

```sql voyages_last_30d
select 
    vessel_name,
    dep_locode,
    arr_locode,
    dep_time,
    distance_nm,
    route_geometry
from gold.voyages
where dep_time >= current_date - interval '30 days'
order by dep_time desc
limit 100
