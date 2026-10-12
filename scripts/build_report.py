"""Generate an offline, self-contained report from measured results."""
from pathlib import Path
import hashlib
import json
import platform
import subprocess
import sys
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

root = Path(__file__).resolve().parents[1]
r = root / 'results'
m = json.loads((r / 'manifest.json').read_text())
k = json.loads((r / 'metrics.json').read_text())
d = pd.read_csv(r / 'daily.csv', parse_dates=['date'])
h = pd.read_csv(r / 'hourly.csv', parse_dates=['timestamp'])
f = pd.read_csv(r / 'forecast_test.csv', parse_dates=['date'])
q = m['quality']; name = k['selected_on_validation']; test = k['test'][name]
source = root.parent / 'household_power_consumption.txt'
digest = hashlib.file_digest(source.open('rb'), 'sha256').hexdigest() if source.exists() else 'source not present'
versions = subprocess.check_output([sys.executable, '-m', 'pip', 'freeze'], text=True)
(root / 'requirements.lock.txt').write_text(versions)
eligible = d[d.coverage >= .95]
hour_profile = h[h.coverage >= .95].groupby(h.timestamp.dt.hour).mean(numeric_only=True)
peak_hour = int(hour_profile.mean_kw.idxmax())
components = d[['kitchen_kwh','laundry_kwh','climate_kwh','other_kwh']].sum()
improvement = 100 * (1 - test['mae_kwh'] / k['test']['Weekly baseline']['mae_kwh'])
model_table = '| Model | Validation MAE (kWh) | Test MAE (kWh) | Test RMSE (kWh) | Test WAPE (%) | Selected |\n|---|---:|---:|---:|---:|---|\n' + '\n'.join(
    f"| {n} | {k['validation'][n]['mae_kwh']:.3f} | {v['mae_kwh']:.3f} | {v['rmse_kwh']:.3f} | {v['wape_percent']:.3f} | {'Yes' if n == name else ''} |" for n, v in k['test'].items())
split_table = '| Split | First eligible date | Last eligible date | Days |\n|---|---|---|---:|\n' + '\n'.join(
    f"| {n.title()} | {v['start']} | {v['end']} | {v['days']} |" for n, v in k['splits'].items())
ui_evidence = r / 'ui_verification.json'
ui_note = f"Dashboard and temporal-contract verification: {json.loads(ui_evidence.read_text())['automated_tests_passed']} automated tests passed. Evidence is saved in `results/ui_verification.json`." if ui_evidence.exists() else ''
checks = json.loads((r / 'verification.json').read_text())['passed']
report = f'''# WattWise: Household Electricity Analytics with Hadoop and YARN

## Abstract
This project processes {q['rows']:,} minute-level electricity observations using HDFS, YARN, Apache Spark and Hadoop Streaming MapReduce. It creates a partitioned Parquet lake, profiles data quality, analyzes consumption, evaluates next-day forecasting, and identifies unusual daily consumption. A Streamlit dashboard and an offline HTML report make the results reviewable. All reported measurements come from the supplied file.

## Problem and objectives
How does a household's electricity use change over time, which monitored circuits contribute energy, when does demand peak, and how accurately can tomorrow's consumption be predicted? Objectives are scalable ingestion, reliable energy accounting, temporal analysis, independent computation verification, and an honest out-of-time forecast benchmark.

## Dataset and provenance
UCI Individual Household Electric Power Consumption: one household in Sceaux, France. The supplied file spans {q['start']} to {q['end']}. Source SHA-256: `{digest}`. File: `household_power_consumption.txt`. UCI DOI: https://doi.org/10.24432/C58K54 . Source documentation: https://archive.ics.uci.edu/dataset/235/individual+household+electric+power+consumption . The dataset is historical and is not a live electricity feed. The source values have no explicit timezone metadata.

Global active power is kW; voltage is V; intensity is A. UCI labels the reactive-power field in kW, although conventional reactive-power notation is kVAR; this project retains it unchanged and derives energy only from active power. Sub-meterings are Wh per minute: kitchen, laundry, and water heating/air conditioning. Circuit labels are broad groupings, not appliance identification. Every observed minute contributes `active_power / 60` kWh. Residual energy is `active_power * 1000 / 60 - sub1 - sub2 - sub3` Wh. Small negative residuals are retained and counted because measurements and rounding do not guarantee exact circuit reconciliation.

## Architecture
```mermaid
flowchart LR
  A[Supplied semicolon text] --> B[HDFS raw layer]
  B --> C[Spark SQL on YARN]
  C --> D[HDFS Parquet by year/month]
  C --> E[Daily/hourly aggregates]
  B --> F[Hadoop Streaming MapReduce on YARN]
  F --> G[Independent daily-total check]
  E --> G
  E --> H[Chronological forecasting and anomaly rules]
  H --> I[Streamlit dashboard and offline report]
```
HDFS stores data, YARN allocates containers, Spark executes distributed ETL, and MapReduce provides a second implementation. Modeling runs on small daily aggregates in the client process using scikit-learn. The verified deployment is a single-node pseudo-distributed teaching cluster, not a multi-machine scalability benchmark. Replication is one for this local cluster. The full raw file is modest in storage size; its millions of records demonstrate the big-data toolchain without claiming internet-scale volume.

## Data quality and cleaning
- Raw readings: {q['rows']:,}; expected timestamp minutes: {q['expected_minutes']:,}; absent timestamp minutes: {q['absent_minutes']:,}.
- Missing active-power measurements: {q['missing_global_active_power']:,} ({100*q['missing_global_active_power']/q['rows']:.3f}%). Missing markers `?` become nulls, not zeros.
- Duplicate timestamps: {q['duplicate_timestamps']}; invalid timestamps: {q['invalid_timestamps']}; negative active power: {q['negative_power']}.
- Negative residual-energy minutes below -0.01 Wh: {q['negative_unmetered_minutes']:,}.
- Eligible days: {m['eligible_days']:,} of {m['total_days']:,}, requiring at least 95% of 1,440 expected valid minute readings. Partial first/last days and low-coverage days are excluded from forecasts and typical daily summaries.

No energy is imputed. Daily/monthly sums are observed totals and can understate actual use where observations are missing. Forecast lag windows with missing/low-coverage days are dropped. Wall-clock values are parsed deterministically without shifting source times.

## Measured findings
- Total observed energy: {m['totals']['observed_energy_kwh']:,.2f} kWh.
- Mean power over valid minutes: {m['totals']['mean_kw']:.3f} kW; largest minute reading: {m['totals']['peak_kw']:.3f} kW.
- Eligible-day average observed consumption: {eligible.energy_kwh.mean():.2f} kWh/day.
- Highest average hourly demand among hours with ≥95% coverage: {peak_hour:02d}:00.
- Climate/water-heating circuit share of observed energy: {components.climate_kwh/components.sum():.1%}. This is a circuit share, not the share caused solely by air conditioning.

## Forecast methodology and results
Daily features include calendar lags 1, 2, 7, 14 and 28 days, previous-only rolling means/standard deviations, weekday, weekend and annual seasonality. The target is observed daily kWh on eligible days. Features contain no current-day measurements. Data is split chronologically into 60% train, 20% validation and 20% final test after invalid feature windows are removed. Models are Ridge, Random Forest, Histogram Gradient Boosting, and a same-weekday-last-week baseline. Minimum validation MAE selects the model before test evaluation. Models are refit on train plus validation for the final test.

{model_table}

Validation-selected model: **{name}**. Final test MAE: **{test['mae_kwh']:.3f} kWh**, RMSE: **{test['rmse_kwh']:.3f} kWh**, WAPE: **{test['wape_percent']:.2f}%**. MAE reduction against the weekly baseline: **{improvement:.1f}%**. The table reports all model results for comparison, but test scores do not determine selection.

Split boundaries:

{split_table}
Evaluation is rolling one-day-ahead: yesterday's actual value is known before predicting today. This is not a multi-day open-loop forecast. {k['excluded_feature_or_target_days']} calendar days are excluded for target or feature completeness. A forecast after the dataset is withheld if its lag window is incomplete; this avoids an unsupported prediction. Future work can add weather, a second household, rolling-origin evaluation and uncertainty intervals.

## Anomaly detection
{k['anomaly_days']} eligible days have absolute robust z-score >3.5 compared with the preceding 28 calendar days, with at least 14 usable observations. The reference is a past-only median and median absolute deviation. Flags are review candidates, not proven electrical faults or supervised anomaly labels. Seasonal changes and household behavior can produce flags.

## Execution evidence and verification
Spark {m['spark_version']} executed on `{m['master']}` with application ID `{m['application_id']}`. ETL wall time: {m['runtime_seconds']:.2f} seconds including loading, validation, Parquet writing, and aggregate export. This single-run duration is not a performance comparison. Python: {platform.python_version()}. Dependency versions are frozen in `requirements.lock.txt`.

Passed checks: {', '.join(checks)}. Separate tests prove that changing current/future target values cannot change today's forecast inputs and that missing dates do not compress seven-day calendar lags. `runtime/logs/spark-pipeline.log` and `runtime/logs/mapreduce.log` record the actual distributed executions. `results/manifest.json`, `metrics.json`, and `verification.json` retain machine-readable evidence.

{ui_note}

## Limitations and responsible conclusions
One home cannot represent a city or an entire population. Missing energy is not recoverable from these readings alone. Broad sub-meterings cannot identify individual appliances or causes. No tariff is provided, so monetary savings are not estimated. The cluster has one worker; distributed execution is demonstrated, but multi-node speedups and fault tolerance are not benchmarked. No real-time streaming, Hive service, or weather integration is claimed. The rule-based anomaly detector has no labeled fault ground truth.

## Reproduce and present
See `README.md` for exact setup and run commands. Dashboard pages separate Overview, Consumption, Forecasting, Anomalies, Data & platform, and Project & downloads. Date filters apply to plotted predictions and exports; benchmark scores are explicitly labeled as full-test metrics. Demo consumption trends, circuit shares, the hourly heatmap, the forecast compared with the weekly baseline, and the Data & platform page. Then show the YARN application evidence and independent MapReduce agreement. Use `docs/VIVA.md` to explain design decisions.

## References
- [UCI household power dataset](https://archive.ics.uci.edu/dataset/235/individual+household+electric+power+consumption)
- [Apache Hadoop 3.3.6 single-node setup](https://hadoop.apache.org/docs/r3.3.6/hadoop-project-dist/hadoop-common/SingleCluster.html)
- [Spark 3.5.6 on YARN](https://dist.apache.org/repos/dist/release/spark/docs/3.5.6/running-on-yarn.html)
'''
(root / 'docs/PROJECT_REPORT.md').write_text(report)
figs = [
    ('Observed daily consumption', px.line(d, x='date', y='energy_kwh', labels={'energy_kwh':'kWh','date':'Date'})),
    ('Circuit energy share', px.pie(names=['Kitchen','Laundry','Climate / water heating','Other'],values=components,hole=.65)),
    ('Average demand by hour', px.bar(x=hour_profile.index,y=hour_profile.mean_kw,labels={'x':'Hour','y':'Mean kW'})),
    ('Forecast on unseen dates', px.line(f.set_index('date').reindex(pd.date_range(f.date.min(),f.date.max())).rename(columns={'actual_kwh':'Observed','prediction_kwh':name+' forecast','weekly_baseline_kwh':'Previous-week baseline'}),y=['Observed',name+' forecast','Previous-week baseline'],labels={'value':'Daily energy (kWh)','variable':'Series','index':'Date'})),
    ('Daily observation coverage', px.line(d,x='date',y='coverage',labels={'date':'Date','coverage':'Valid minute readings'}))]
figs[-1][1].update_yaxes(tickformat='.0%', range=[0, 1.05])
sys.path.insert(0,str(root))
from src.report_layout import render_report
(r / 'PROJECT_REPORT.html').write_text(render_report(report, figs, m, k))
print(f'Reports created: {r / "PROJECT_REPORT.html"}')
