"""Small, testable transformations used by the dashboard."""
import pandas as pd

def select_days(daily, start, end, eligible_only=True):
    chosen = daily[daily.date.between(pd.Timestamp(start), pd.Timestamp(end))].copy()
    if eligible_only: chosen = chosen[chosen.coverage >= .95]
    return chosen

def weekly_heatmap(hourly, dates):
    valid = hourly[hourly.timestamp.dt.normalize().isin(dates) & (hourly.coverage >= .95)].copy()
    valid['weekday'] = valid.timestamp.dt.dayofweek
    valid['hour'] = valid.timestamp.dt.hour
    return valid.pivot_table(index='weekday',columns='hour',values='mean_kw',aggfunc='mean').reindex(index=range(7),columns=range(24))

def calendar_series(selected, start, end):
    """Keep omitted days explicit so plots never connect across missing readings."""
    return selected.set_index('date').energy_kwh.reindex(pd.date_range(start,end,freq='D'))

def forecast_window(forecasts, start, end):
    return forecasts[forecasts.date.between(pd.Timestamp(start),pd.Timestamp(end))].copy()
