#!/usr/bin/env bash
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export JAVA_HOME="${JAVA_HOME:-/opt/homebrew/opt/openjdk@11}"
export HADOOP_HOME="${HADOOP_HOME:-/usr/local/hadoop}"
export HADOOP_CONF_DIR="$PROJECT_ROOT/conf"
export HADOOP_LOG_DIR="$PROJECT_ROOT/runtime/logs"
export HADOOP_PID_DIR="$PROJECT_ROOT/runtime/pids"
export HADOOP_IDENT_STRING=energy_insights
export HADOOP_HEAPSIZE_MAX=512
export YARN_HEAPSIZE=512
export SPARK_LOCAL_IP=127.0.0.1
export PYSPARK_PYTHON="$PROJECT_ROOT/.venv/bin/python"
export PATH="$JAVA_HOME/bin:$HADOOP_HOME/bin:$PROJECT_ROOT/.venv/bin:$PATH"
