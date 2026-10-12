"""Optional exact, local descriptive audit; separate from distributed Spark ETL."""
import argparse
import hashlib
import json
from pathlib import Path
import pandas as pd

def main():
    root=Path(__file__).resolve().parents[1]
    p=argparse.ArgumentParser()
    p.add_argument('--input',type=Path,default=root.parent/'household_power_consumption.txt')
    args=p.parse_args()
    df=pd.read_csv(args.input,sep=';',na_values=['?'],low_memory=False)
    measurements=list(df.columns[2:])
    stats=df[measurements].describe(percentiles=[.25,.5,.75]).T
    stats['missing']=df[measurements].isna().sum()
    r=root/'results'; r.mkdir(exist_ok=True)
    stats.to_csv(r/'dataset_statistics.csv')
    timestamp=pd.to_datetime(df.Date+' '+df.Time,format='%d/%m/%Y %H:%M:%S')
    active=df.Global_active_power
    daily=pd.DataFrame({'date':timestamp.dt.normalize(),'energy_kwh':active/60,'valid_minutes':active.notna().astype(int)})
    daily=daily.groupby('date').agg(energy_kwh=('energy_kwh',lambda x:x.sum(min_count=1)),valid_minutes=('valid_minutes','sum'))
    daily['year']=daily.index.year; daily['eligible']=daily.valid_minutes/1440>=.95
    annual=daily.groupby('year').agg(days=('valid_minutes','size'),valid_minutes=('valid_minutes','sum'),observed_kwh=('energy_kwh','sum'),eligible_days=('eligible','sum'))
    annual.to_csv(r/'dataset_annual.csv')
    with args.input.open('rb') as f: digest=hashlib.file_digest(f,'sha256').hexdigest()
    profile={'source':str(args.input.resolve()),'rows':len(df),'bytes':args.input.stat().st_size,'sha256':digest,
      'columns':list(df.columns),'fully_missing_measurement_rows':int(df[measurements].isna().all(axis=1).sum()),
      'fully_missing_days':int((daily.valid_minutes==0).sum()),'first_day_valid_minutes':int(daily.iloc[0].valid_minutes),
      'last_day_valid_minutes':int(daily.iloc[-1].valid_minutes),'start':str(timestamp.min()),'end':str(timestamp.max())}
    (r/'dataset_profile.json').write_text(json.dumps(profile,indent=2))
    print(f'Profiled {len(df):,} records. Statistics and provenance saved in {r}')

if __name__=='__main__': main()
