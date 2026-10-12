import sys
import unittest
from pathlib import Path
import pandas as pd
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from dashboard_data import select_days, weekly_heatmap, calendar_series, forecast_window

class FilterContract(unittest.TestCase):
    def test_missing_days_remain_plot_gaps(self):
        d=pd.DataFrame({'date':pd.date_range('2009-01-01',periods=3),'coverage':[1,.2,1],'energy_kwh':[10,2,12]})
        series=calendar_series(select_days(d,'2009-01-01','2009-01-03'),'2009-01-01','2009-01-03')
        self.assertEqual(len(series),3); self.assertTrue(pd.isna(series.iloc[1]))
    def test_heatmap_has_calendar_shape_for_one_hour(self):
        h=pd.DataFrame({'timestamp':pd.to_datetime(['2009-01-01 04:00']),'coverage':[1.],'mean_kw':[2.]})
        grid=weekly_heatmap(h,pd.to_datetime(['2009-01-01']))
        self.assertEqual(grid.shape,(7,24)); self.assertEqual(grid.loc[3,4],2)
        self.assertEqual(grid.notna().sum().sum(),1)
    def test_forecast_exports_respect_date_selection(self):
        f=pd.DataFrame({'date':pd.date_range('2010-01-01',periods=4),'prediction_kwh':[1,2,3,4]})
        self.assertEqual(forecast_window(f,'2010-01-02','2010-01-03').prediction_kwh.tolist(),[2,3])

if __name__=='__main__': unittest.main()
