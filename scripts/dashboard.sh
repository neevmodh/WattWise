#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/env.sh"
cd "$PROJECT_ROOT"
exec streamlit run "$PROJECT_ROOT/app.py" --server.address 127.0.0.1 --server.port 18501 --server.headless true --browser.gatherUsageStats false
