-- In spark-sql, first register the distributed partitioned Parquet lake:
CREATE OR REPLACE TEMP VIEW readings USING parquet
OPTIONS (path 'hdfs://localhost:19000/energy-insights/silver');

-- Total observed energy, never power summed as if it were energy.
SELECT year, month, SUM(energy_kwh) AS observed_kwh,
       COUNT(global_active_power) AS valid_minutes
FROM readings GROUP BY year, month ORDER BY year, month;

-- Demand profile; each observed minute has equal weight.
SELECT dayofweek(timestamp) AS weekday, hour(timestamp) AS hour,
       AVG(global_active_power) AS mean_kw
FROM readings GROUP BY dayofweek(timestamp), hour(timestamp)
ORDER BY weekday, hour;

-- Peak readings; device attribution is unavailable.
SELECT timestamp, global_active_power AS kw, voltage
FROM readings ORDER BY global_active_power DESC LIMIT 20;

-- Partition pruning example for the selected month.
EXPLAIN SELECT AVG(global_active_power) FROM readings
WHERE year = 2009 AND month = 1;
