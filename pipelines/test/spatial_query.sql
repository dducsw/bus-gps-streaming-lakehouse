-- Spatial Bounding Box Query
-- Finds buses in a specific region of the city
SELECT vehicle, x, y, timestamp
FROM catalog_iceberg.bus_bronze.buswaypoint_test
WHERE x BETWEEN 106.70 AND 106.80 
  AND y BETWEEN 10.80 AND 10.90
ORDER BY timestamp DESC;


EXPLAIN ANALYZE
SELECT vehicle, x, y, timestamp
FROM catalog_iceberg.bus_bronze.buswaypoint_test
WHERE timestamp BETWEEN TIMESTAMP '2025-04-01 00:00:00'
                     AND TIMESTAMP '2025-04-10 23:59:59'
  AND x BETWEEN 106.70 AND 106.80 
  AND y BETWEEN 10.80 AND 10.90
ORDER BY timestamp DESC;