#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"
python "$PROJECT_ROOT/scripts/configure_cluster.py"
# This formats only this project's isolated storage, once.
if [[ ! -f "$PROJECT_ROOT/runtime/namenode/current/VERSION" ]]; then
  hdfs namenode -format -nonInteractive energy-insights
fi
hdfs --daemon start namenode
hdfs --daemon start datanode
yarn --daemon start resourcemanager
yarn --daemon start nodemanager
for attempt in {1..30}; do
  if hdfs dfsadmin -safemode get 2>/dev/null | grep -q 'OFF' && yarn node -list 2>/dev/null | grep -q RUNNING; then
    echo 'HDFS and YARN ready. HDFS UI: http://127.0.0.1:19870 | YARN UI: http://127.0.0.1:18088'
    exit 0
  fi
  sleep 2
done
echo 'Cluster did not become ready. Inspect runtime/logs.' >&2
exit 1
