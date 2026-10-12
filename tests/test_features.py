"""Meaningful temporal leakage test: future edits cannot change today's inputs."""
import sys
import unittest
from pathlib import Path
import pandas as pd
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from analyze import features

class TemporalContract(unittest.TestCase):
    def test_future_cannot_change_past_features(self):
        s = pd.Series(range(90), index=pd.date_range('2008-01-01',periods=90),dtype=float)
        mutated = s.copy(); mutated.iloc[60:] = 999999
        pd.testing.assert_frame_equal(features(s).iloc[:61],features(mutated).iloc[:61])
    def test_missing_date_does_not_compress_calendar_lags(self):
        s = pd.Series(range(90),index=pd.date_range('2008-01-01',periods=90),dtype=float)
        s.iloc[40] = float('nan')
        self.assertTrue(pd.isna(features(s).iloc[47].lag_7))
        self.assertEqual(features(s).iloc[48].lag_7,41)

if __name__ == '__main__': unittest.main()
