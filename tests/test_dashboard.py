"""Exercise every page plus short and empty date-range cases."""
from pathlib import Path
from datetime import date
import unittest
import pandas as pd
from streamlit.testing.v1 import AppTest

class DashboardTest(unittest.TestCase):
    def setUp(self):
        self.root=Path(__file__).resolve().parents[1]
        self.app=AppTest.from_file(str(self.root/'app.py'),default_timeout=30).run()
    def check(self):
        self.assertEqual(len(self.app.exception),0,str(self.app.exception))
    def test_every_page_and_export(self):
        self.check(); self.assertEqual(len(self.app.metric),4)
        for page in ['Consumption','Forecasting','Anomalies','Data & platform','Project & downloads']:
            self.app.sidebar.radio[0].set_value(page).run(); self.check()
        self.assertEqual(len(self.app.download_button),6)
    def test_single_day_and_year_preset(self):
        self.app.sidebar.radio[0].set_value('Consumption').run()
        self.app.sidebar.date_input[0].set_value((date(2009,1,1),date(2009,1,1))).run(); self.check()
        self.app.sidebar.selectbox[0].set_value('2010').run(); self.check()
        self.assertEqual(self.app.sidebar.date_input[0].value[0].year,2010)
    def test_forecast_without_test_dates(self):
        self.app.sidebar.radio[0].set_value('Forecasting').run()
        self.app.sidebar.date_input[0].set_value((date(2007,1,1),date(2007,1,31))).run(); self.check()
        self.assertTrue(any('No evaluated predictions' in x.value for x in self.app.info))
        self.assertEqual(len(self.app.download_button),0)
    def test_entirely_missing_day(self):
        daily=pd.read_csv(self.root/'results/daily.csv',parse_dates=['date'])
        missing=daily.loc[daily.valid_minutes==0,'date'].iloc[0].date()
        self.app.sidebar.toggle[0].set_value(False).run()
        self.app.sidebar.date_input[0].set_value((missing,missing)).run(); self.check()
        self.assertTrue(any('no valid power readings' in x.value for x in self.app.warning))

if __name__=='__main__': unittest.main()
