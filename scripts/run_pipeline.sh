#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"
MODE="${1:-yarn}"
DATASET="${2:-$PROJECT_ROOT/../household_power_consumption.txt}"
mkdir -p "$PROJECT_ROOT/results" "$PROJECT_ROOT/runtime/logs"
if [[ "$MODE" == yarn ]]; then
  hdfs dfs -mkdir -p /energy-insights/raw
  hdfs dfs -put -f "$DATASET" /energy-insights/raw/household_power_consumption.txt
  INPUT=hdfs://localhost:19000/energy-insights/raw/household_power_consumption.txt
  LAKE=hdfs://localhost:19000/energy-insights/silver
  MASTER=yarn
else
  INPUT="$DATASET"
  LAKE="$PROJECT_ROOT/data/silver"
  MASTER='local[2]'
fi
spark-submit --master "$MASTER" --deploy-mode client --driver-memory 1g \
  --executor-memory 1g --executor-cores 2 --num-executors 1 \
  --conf spark.yarn.am.memory=512m --conf spark.sql.shuffle.partitions=8 \
  "$PROJECT_ROOT/src/spark_pipeline.py" --input "$INPUT" --lake "$LAKE" --output "$PROJECT_ROOT/results" \
  2>&1 | tee "$PROJECT_ROOT/runtime/logs/spark-pipeline.log"
python "$PROJECT_ROOT/src/analyze.py" --results "$PROJECT_ROOT/results"
