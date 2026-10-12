"""Distributed ETL and SQL aggregation; runs unchanged on local Spark or YARN."""
import argparse
import hashlib
import json
import time
from datetime import datetime
from pathlib import Path
from pyspark.sql import SparkSession, functions as F, types as T

COLUMNS = ['Date', 'Time', 'Global_active_power', 'Global_reactive_power', 'Voltage',
           'Global_intensity', 'Sub_metering_1', 'Sub_metering_2', 'Sub_metering_3']

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--input', required=True)
    p.add_argument('--lake', required=True)
    p.add_argument('--output', required=True)
    args = p.parse_args()
    out = Path(args.output); out.mkdir(parents=True, exist_ok=True)
    start = time.perf_counter()
    spark = SparkSession.builder.appName('WattWise-Household-ETL').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    spark.conf.set('spark.sql.session.timeZone', 'UTC')
    raw = spark.read.option('header', True).option('sep', ';').option('nullValue', '?').schema(
        T.StructType([T.StructField(c, T.StringType()) for c in COLUMNS])).csv(args.input)
    df = raw.select(F.to_timestamp(F.concat_ws(' ', 'Date', 'Time'), 'd/M/yyyy HH:mm:ss').alias('timestamp'),
                    *[F.col(c).cast('double').alias(c.lower()) for c in COLUMNS[2:]]).cache()
    numeric = [c.lower() for c in COLUMNS[2:]]
    # Format inside Spark so Python's host timezone cannot shift naive collected datetimes.
    quality = df.agg(F.count('*').alias('rows'), F.date_format(F.min('timestamp'), 'yyyy-MM-dd HH:mm:ss').alias('start'), F.date_format(F.max('timestamp'), 'yyyy-MM-dd HH:mm:ss').alias('end'),
        F.sum(F.col('timestamp').isNull().cast('int')).alias('invalid_timestamps'),
        *[F.sum(F.col(c).isNull().cast('int')).alias('missing_' + c) for c in numeric],
        F.sum((F.col('global_active_power') < 0).cast('int')).alias('negative_power')).first().asDict()
    quality['duplicate_timestamps'] = df.where('timestamp IS NOT NULL').count() - df.select('timestamp').where('timestamp IS NOT NULL').distinct().count()
    if quality['invalid_timestamps'] or quality['duplicate_timestamps'] or quality['negative_power']:
        raise ValueError(f'Data contract failed: {quality}')
    quality['expected_minutes'] = int((datetime.fromisoformat(quality['end']) - datetime.fromisoformat(quality['start'])).total_seconds() / 60) + 1
    quality['absent_minutes'] = quality['expected_minutes'] - quality['rows']
    for key in ['start', 'end']: quality[key] = str(quality[key])
    clean = df.withColumn('energy_kwh', F.col('global_active_power') / 60.0).withColumn(
        'unmetered_wh', F.col('global_active_power') * 1000 / 60 - F.col('sub_metering_1') - F.col('sub_metering_2') - F.col('sub_metering_3'))
    quality['negative_unmetered_minutes'] = clean.where('unmetered_wh < -0.01').count()
    # Preserve nulls and signed residuals: never invent energy or clamp measurements.
    clean.withColumn('year', F.year('timestamp')).withColumn('month', F.month('timestamp')).write.mode('overwrite').partitionBy('year', 'month').parquet(args.lake)
    clean.createOrReplaceTempView('readings')
    daily = spark.sql('''SELECT to_date(timestamp) AS date, count(*) AS recorded_minutes,
      count(global_active_power) AS valid_minutes, sum(energy_kwh) AS energy_kwh,
      avg(global_active_power) AS mean_kw, max(global_active_power) AS peak_kw,
      avg(voltage) AS voltage, sum(sub_metering_1)/1000 AS kitchen_kwh,
      sum(sub_metering_2)/1000 AS laundry_kwh, sum(sub_metering_3)/1000 AS climate_kwh,
      sum(unmetered_wh)/1000 AS other_kwh
      FROM readings GROUP BY to_date(timestamp) ORDER BY date''').toPandas()
    daily['coverage'] = daily.valid_minutes / 1440
    daily['eligible'] = daily.coverage >= 0.95
    daily.to_csv(out / 'daily.csv', index=False)
    hourly = spark.sql('''SELECT date_trunc('hour', timestamp) AS timestamp,
      count(global_active_power) AS valid_minutes, sum(energy_kwh) AS energy_kwh,
      avg(global_active_power) AS mean_kw, max(global_active_power) AS peak_kw
      FROM readings GROUP BY date_trunc('hour', timestamp) ORDER BY timestamp''').toPandas()
    hourly['coverage'] = hourly.valid_minutes / 60
    hourly.to_csv(out / 'hourly.csv', index=False)
    totals = clean.agg(F.sum('energy_kwh').alias('observed_energy_kwh'),
        F.avg('global_active_power').alias('mean_kw'), F.max('global_active_power').alias('peak_kw'),
        F.sum('sub_metering_1').alias('kitchen_wh'), F.sum('sub_metering_2').alias('laundry_wh'),
        F.sum('sub_metering_3').alias('climate_wh'), F.sum('unmetered_wh').alias('other_wh')).first().asDict()
    manifest = {'quality': quality, 'totals': totals, 'spark_version': spark.version,
        'master': spark.sparkContext.master, 'application_id': spark.sparkContext.applicationId,
        'input': args.input, 'parquet_location': args.lake, 'runtime_seconds': round(time.perf_counter() - start, 2),
        'eligible_days': int(daily.eligible.sum()), 'total_days': len(daily),
        'units': {'power': 'kW', 'minute_energy': 'kW / 60 = kWh', 'sub_metering': 'Wh per minute'},
        'timestamp_policy': 'Naive source wall-clock values; UTC session used solely for deterministic parsing; no timezone conversion.'}
    (out / 'manifest.json').write_text(json.dumps(manifest, indent=2))
    print(json.dumps(manifest, indent=2))
    df.unpersist(); spark.stop()

if __name__ == '__main__': main()
