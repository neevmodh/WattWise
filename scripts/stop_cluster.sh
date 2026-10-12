#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"
yarn --daemon stop nodemanager
yarn --daemon stop resourcemanager
hdfs --daemon stop datanode
hdfs --daemon stop namenode
