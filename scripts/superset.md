```route analysis

WITH route_list AS (
    SELECT 
        route_no,
        ROW_NUMBER() OVER (ORDER BY route_no) AS rn
    FROM (
        SELECT DISTINCT route_no
        FROM catalog_iceberg.bus_gold.gps_dashboard
        WHERE date = DATE '2025-03-25'
    )
),
route_count AS (
    SELECT COUNT(*) AS total_routes FROM route_list
),
selected_index AS (
    SELECT 
        (CAST(to_unixtime(date_trunc('minute', current_timestamp) - 
            interval '1' minute * (minute(current_timestamp) % 2)) / 120 AS BIGINT) 
        % total_routes) + 1 AS rn
    FROM route_count
),
selected_route AS (
    SELECT route_no
    FROM route_list
    WHERE rn = (SELECT rn FROM selected_index)
),
data AS (
    SELECT g.*
    FROM catalog_iceberg.bus_gold.gps_dashboard g
    JOIN selected_route r 
        ON g.route_no = r.route_no
    WHERE g.date = DATE '2025-03-25'
)
SELECT * FROM data

``` speed route and system
WITH selected_route AS (
    WITH route_list AS (
        SELECT 
            route_no,
            ROW_NUMBER() OVER (ORDER BY route_no) AS rn
        FROM (
            SELECT DISTINCT route_no
            FROM catalog_iceberg.bus_gold.gps_dashboard
            WHERE date = DATE '2025-03-25'
        )
    ),
    route_count AS (
        SELECT COUNT(*) AS total_routes FROM route_list
    )
    SELECT route_no
    FROM route_list
    WHERE rn = (
        SELECT (CAST(to_unixtime(date_trunc('minute', current_timestamp) - 
            interval '1' minute * (minute(current_timestamp) % 2)) / 120 AS BIGINT) 
        % total_routes) + 1
        FROM route_count
    )
),
avg_selected_route AS (
    SELECT 
        hour,
        AVG(speed) AS avg_speed_route
    FROM catalog_iceberg.bus_gold.gps_dashboard
    WHERE date = DATE '2025-03-25'
      AND route_no = (SELECT route_no FROM selected_route)
    GROUP BY hour
),
avg_all_routes AS (
    SELECT 
        hour,
        AVG(speed) AS avg_speed_all
    FROM catalog_iceberg.bus_gold.gps_dashboard
    WHERE date = DATE '2025-03-25'
    GROUP BY hour
)
SELECT 
    a.hour,
    a.avg_speed_all,
    COALESCE(r.avg_speed_route, 0) AS avg_speed_route,
    (SELECT route_no FROM selected_route) AS route_no
FROM avg_all_routes a
LEFT JOIN avg_selected_route r ON a.hour = r.hour
ORDER BY a.hour