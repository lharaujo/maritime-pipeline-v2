SELECT
    mmsi,
    REGEXP_REPLACE(imo, '^IMO', '') as imo,
    vessel_name,
    port_locode,
    latitude as lat,
    longitude as lon,
    dep_time

FROM {{ source('motherduck', 'raw_ais_pings') }}
