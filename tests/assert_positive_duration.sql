-- This test fails if any voyage has a duration <= 0 hours
-- Returns the failing rows

SELECT *
FROM {{ ref('voyages') }}
WHERE duration_hrs <= 0
