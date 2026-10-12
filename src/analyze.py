"""Chronological model evaluation and explainable, past-only anomaly flags."""
import argparse
import json
import pickle
from pathlib import Path
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor, RandomForestRegressor
from sklearn.linear_model import Ridge
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error

def features(series):
    x = pd.DataFrame(index=series.index)
    for lag in [1, 2, 7, 14, 28]: x[f'lag_{lag}'] = series.shift(lag)
    for window in [7, 28]:
        x[f'mean_{window}'] = series.shift(1).rolling(window).mean()
        x[f'std_{window}'] = series.shift(1).rolling(window).std()
    x['weekday'] = x.index.dayofweek
    x['weekend'] = (x.index.dayofweek >= 5).astype(int)
    x['season_sin'] = np.sin(2 * np.pi * x.index.dayofyear / 365.25)
    x['season_cos'] = np.cos(2 * np.pi * x.index.dayofyear / 365.25)
    return x

def score(y, pred):
    return {'mae_kwh': float(mean_absolute_error(y, pred)),
            'rmse_kwh': float(np.sqrt(mean_squared_error(y, pred))),
            'wape_percent': float(np.abs(np.asarray(y) - pred).sum() / np.abs(y).sum() * 100)}

def main():
    p = argparse.ArgumentParser(); p.add_argument('--results', type=Path, required=True); a = p.parse_args()
    daily = pd.read_csv(a.results / 'daily.csv', parse_dates=['date']).set_index('date')
    series = daily.energy_kwh.where(daily.coverage >= 0.95).asfreq('D')
    x = features(series)
    frame = x.assign(target=series).dropna()
    if len(frame) < 150: raise ValueError('Not enough complete days to evaluate a forecast.')
    x, y = frame.drop(columns='target'), frame.target
    train_end, val_end = int(len(frame) * 0.6), int(len(frame) * 0.8)
    xt, xv, xe = x.iloc[:train_end], x.iloc[train_end:val_end], x.iloc[val_end:]
    yt, yv, ye = y.iloc[:train_end], y.iloc[train_end:val_end], y.iloc[val_end:]
    models = {'Ridge': make_pipeline(StandardScaler(), Ridge(alpha=10)),
              'Random forest': RandomForestRegressor(n_estimators=200, max_depth=10, min_samples_leaf=5, random_state=42, n_jobs=2),
              'Gradient boosting': HistGradientBoostingRegressor(max_iter=200, max_leaf_nodes=15, l2_regularization=5, random_state=42)}
    validation = {'Weekly baseline': score(yv, xv.lag_7.to_numpy())}
    for name, model in models.items():
        model.fit(xt, yt); validation[name] = score(yv, np.maximum(0, model.predict(xv)))
    selected = min(validation, key=lambda n: validation[n]['mae_kwh'])
    test = {'Weekly baseline': score(ye, xe.lag_7.to_numpy())}
    predictions = pd.DataFrame({'date': ye.index, 'actual_kwh': ye.values, 'weekly_baseline_kwh': xe.lag_7.values})
    for name, model in models.items():
        model.fit(x.iloc[:val_end], y.iloc[:val_end])
        pred = np.maximum(0, model.predict(xe)); test[name] = score(ye, pred)
        predictions[name] = pred
    predictions['prediction_kwh'] = predictions.weekly_baseline_kwh if selected == 'Weekly baseline' else predictions[selected]
    predictions.to_csv(a.results / 'forecast_test.csv', index=False)
    (a.results / 'model.pkl').write_bytes(pickle.dumps({'name': selected, 'model': models.get(selected), 'features': list(x.columns)}))
    # A prospective next-day forecast refits after all evaluation and is saved separately.
    extended = series.reindex(pd.date_range(series.index.min(), series.index.max() + pd.Timedelta(days=1), freq='D'))
    next_x = features(extended).iloc[[-1]]
    next_forecast = None
    if not next_x.isna().any(axis=None):
        if selected == 'Weekly baseline': next_value = next_x.lag_7.iloc[0]
        else:
            models[selected].fit(x, y); next_value = max(0, models[selected].predict(next_x)[0])
        next_forecast = {'date': str(next_x.index[0].date()), 'energy_kwh': float(next_value)}
    med = series.shift(1).rolling(28, min_periods=14).median()
    mad = series.shift(1).rolling(28, min_periods=14).apply(lambda z: np.nanmedian(np.abs(z - np.nanmedian(z))), raw=True)
    robust_z = (series - med) / (1.4826 * mad.replace(0, np.nan))
    anomalies = pd.DataFrame({'date': series.index, 'energy_kwh': series.values,
        'past_median_kwh': med.values, 'robust_z': robust_z.values})
    anomalies = anomalies[anomalies.robust_z.abs() > 3.5]
    anomalies.to_csv(a.results / 'anomalies.csv', index=False)
    metrics = {'selected_on_validation': selected, 'validation': validation, 'test': test,
        'splits': {label: {'start': str(part.index.min().date()), 'end': str(part.index.max().date()), 'days': len(part)}
                   for label, part in [('train', yt), ('validation', yv), ('test', ye)]},
        'excluded_feature_or_target_days': len(series) - len(frame), 'anomaly_days': len(anomalies),
        'next_day_forecast': next_forecast,
        'evaluation': 'Rolling one-day-ahead evaluation. Prior observed days are available at prediction time. Calendar lags preserved; incomplete targets and lag windows excluded, never interpolated.',
        'anomaly_method': 'Absolute robust z > 3.5 relative to previous 28 calendar days, min 14 eligible observations. Flags are not labeled faults.'}
    (a.results / 'metrics.json').write_text(json.dumps(metrics, indent=2))
    print(json.dumps(metrics, indent=2))

if __name__ == '__main__': main()
