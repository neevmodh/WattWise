import argparse
import json
from pathlib import Path
import numpy as np
import pandas as pd

p = argparse.ArgumentParser(); p.add_argument('--results', type=Path, required=True); a = p.parse_args()
m = json.loads((a.results / 'manifest.json').read_text())
d = pd.read_csv(a.results / 'daily.csv')
h = pd.read_csv(a.results / 'hourly.csv')
assert m['quality']['rows'] == 2075259
assert m['quality']['start'] == '2006-12-16 17:24:00'
assert m['quality']['end'] == '2010-11-26 21:02:00'
assert m['quality']['duplicate_timestamps'] == 0
assert d.date.is_unique
assert np.isclose(d.energy_kwh.sum(), m['totals']['observed_energy_kwh'])
assert np.isclose(h.energy_kwh.sum(), d.energy_kwh.sum())
assert np.allclose(d[['kitchen_kwh','laundry_kwh','climate_kwh','other_kwh']].sum(axis=1, min_count=1), d.energy_kwh, atol=1e-8, equal_nan=True)
assert d.valid_minutes.sum() == m['quality']['rows'] - m['quality']['missing_global_active_power']
assert ((d.coverage >= 0) & (d.coverage <= 1)).all()
f = pd.read_csv(a.results / 'forecast_test.csv')
assert f.date.is_monotonic_increasing and f.prediction_kwh.notna().all()
checks = ['Source row count and original wall-clock boundaries', 'Unique timestamps', 'Energy conservation', 'Hourly/daily agreement',
          'Missing-data accounting', 'Coverage bounds', 'Chronological forecast']
mrpath = a.results / 'mapreduce_daily.tsv'
if mrpath.exists():
    mr = pd.read_csv(mrpath, sep='\t', names=['date','mr_kwh','mr_minutes'])
    joined = d.merge(mr, on='date', validate='one_to_one')
    assert len(joined) == len(d)
    # Entirely missing days remain explicit zero-count groups in MapReduce.
    assert np.allclose(joined.energy_kwh.fillna(0), joined.mr_kwh, atol=1e-6)
    assert (joined.valid_minutes == joined.mr_minutes).all()
    checks.append('Independent Hadoop MapReduce/Spark agreement')
(a.results / 'verification.json').write_text(json.dumps({'passed': checks}, indent=2))
print('PASS: ' + ', '.join(checks))
