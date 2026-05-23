EXPLAIN ANALYZE
SELECT m.route_no, DATE(w.timestamp) AS date, COUNT(*) as waypoint_count
FROM catalog_iceberg.bus_bronze.buswaypoint_test w
JOIN catalog_iceberg.bus_bronze.vehicle_bus_mapping m 
    ON w.vehicle = m.vehicle
WHERE w.timestamp BETWEEN TIMESTAMP '2025-04-01 00:00:00' 
                      AND TIMESTAMP '2025-04-10 23:59:59'
GROUP BY m.route_no, DATE(w.timestamp)
ORDER BY date ASC;