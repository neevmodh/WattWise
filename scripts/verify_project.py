"""Run quality and UI contracts, recording evidence only on successful completion."""
from pathlib import Path
import json
import re
import subprocess
import sys
from datetime import datetime, timezone
root=Path(__file__).resolve().parents[1]
subprocess.run([sys.executable,str(root/'tests/verify_results.py'),'--results',str(root/'results')],check=True)
run=subprocess.run([sys.executable,'-m','unittest','discover','-s',str(root/'tests'),'-p','test_*.py','-v'],capture_output=True,text=True)
print(run.stdout+run.stderr)
if run.returncode: raise SystemExit(run.returncode)
count=int(re.search(r'Ran (\d+) tests',run.stderr).group(1))
(root/'results/ui_verification.json').write_text(json.dumps({'verified_at_utc':datetime.now(timezone.utc).isoformat(),
    'automated_tests_passed':count,'checks':['Every dashboard page','Single-day heatmap','Year preset','Empty forecast range',
    'Missing-only day','Filtered export dates','Missing-data plot gaps','Past-only model features','Calendar lags']},indent=2))
