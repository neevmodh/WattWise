#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"
STREAMING_JAR="$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar"
OUTPUT="/energy-insights/mapreduce/run-$(date +%Y%m%d-%H%M%S)"
hadoop jar "$STREAMING_JAR" \
  -D mapreduce.job.name=WattWise-Daily-MapReduce \
  -D mapreduce.job.reduces=1 \
  -files "$PROJECT_ROOT/src/mapper.py,$PROJECT_ROOT/src/reducer.py" \
  -input /energy-insights/raw/household_power_consumption.txt \
  -output "$OUTPUT" \
  -mapper "$PROJECT_ROOT/.venv/bin/python mapper.py" \
  -reducer "$PROJECT_ROOT/.venv/bin/python reducer.py" \
  2>&1 | tee "$PROJECT_ROOT/runtime/logs/mapreduce.log"
hdfs dfs -cat "$OUTPUT/part-*" > "$PROJECT_ROOT/results/mapreduce_daily.tsv"
python "$PROJECT_ROOT/tests/verify_results.py" --results "$PROJECT_ROOT/results"
