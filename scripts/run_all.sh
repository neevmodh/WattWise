#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"
trap 'bash "$PROJECT_ROOT/scripts/stop_cluster.sh"' EXIT
bash "$PROJECT_ROOT/scripts/start_cluster.sh"
bash "$PROJECT_ROOT/scripts/run_pipeline.sh" yarn "${1:-$PROJECT_ROOT/../household_power_consumption.txt}"
bash "$PROJECT_ROOT/scripts/run_mapreduce.sh"
python "$PROJECT_ROOT/scripts/verify_project.py"
curl --fail --silent http://127.0.0.1:18088/ws/v1/cluster/apps > "$PROJECT_ROOT/results/yarn_applications.json"
hdfs dfsadmin -report > "$PROJECT_ROOT/results/hdfs_status.txt"
hdfs dfs -du -s /energy-insights/raw /energy-insights/silver > "$PROJECT_ROOT/results/hdfs_storage.txt"
python "$PROJECT_ROOT/scripts/build_report.py"
echo 'Project completed and verified. Start dashboard: bash scripts/dashboard.sh'
