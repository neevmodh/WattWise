"""Read-only exploration of verified Hadoop/Spark aggregates."""
from pathlib import Path
import html
import json
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from src.dashboard_data import select_days, weekly_heatmap, calendar_series, forecast_window

ROOT=Path(__file__).resolve().parent
RESULTS=ROOT/'results'
TEAL,BLUE,AMBER,PURPLE='#50dbc0','#7ea5ff','#f4bd72','#be9bff'
st.set_page_config(page_title='WattWise · Household Analytics',page_icon='⚡',layout='wide')
st.markdown('''<style>
.stApp{background:#0c1321;color:#edf2fa}
[data-testid="stMainBlockContainer"]{padding:4.5rem 2rem 3rem;max-width:1440px}
[data-testid="stSidebar"]{background:#111b2c;border-right:1px solid #253247}
h1,h2,h3{letter-spacing:-.035em}h1{font-size:2.65rem!important}
.brand{font-size:22px;font-weight:750;letter-spacing:-.7px;margin-bottom:2px}
.brand span{color:#50dbc0}.muted{color:#94a7bf;font-size:13px;line-height:1.6}
.eyebrow{color:#50dbc0;font-size:11px;font-weight:700;letter-spacing:2.5px;margin-bottom:10px}
.subtitle{color:#a9b8ce;font-size:16px;line-height:1.65;max-width:820px;margin-bottom:20px}
.scope{color:#bfd0e5;background:#152237;border:1px solid #29374d;padding:12px 16px;border-radius:9px;font-size:13px;line-height:1.6;margin-bottom:20px}
[data-testid="stMetric"]{background:#152034;border:1px solid #29374d;border-radius:12px;padding:18px;min-height:105px}
[data-testid="stMetricValue"]{font-size:1.65rem!important;letter-spacing:-.6px}
[data-testid="stMetricLabel"] p{white-space:normal!important;font-size:12px!important;color:#afc0d5}
.insight{padding:17px 20px;border-left:3px solid #50dbc0;background:#142334;border-radius:0 8px 8px 0;color:#c3d2e6;font-size:14px;line-height:1.65;margin:12px 0 24px}
.pill{display:inline-block;padding:5px 10px;border:1px solid #2c6459;background:#15342f;color:#70e5ce;border-radius:20px;font-size:11px;font-weight:700}
.pipeline{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px;margin:20px 0}
.stage{background:#152034;border:1px solid #29374d;border-radius:10px;padding:16px;font-size:14px}
.stage small{display:block;color:#95a9c2;margin-top:7px;line-height:1.5}
@media(max-width:1100px){[data-testid="stHorizontalBlock"]:has([data-testid="stMetric"]){display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}[data-testid="stHorizontalBlock"]:has([data-testid="stMetric"])>[data-testid="stColumn"]{width:100%;min-width:0}.pipeline{grid-template-columns:repeat(2,minmax(0,1fr))}}
@media(max-width:600px){h1{font-size:2rem!important}[data-testid="stMainBlockContainer"]{padding:1.5rem 1rem}.pipeline{grid-template-columns:1fr}}
</style>''',unsafe_allow_html=True)
FILES=['daily.csv','hourly.csv','forecast_test.csv','anomalies.csv','manifest.json','metrics.json']
if any(not (RESULTS/n).exists() for n in FILES):
    st.title('WattWise'); st.info('Analysis results are not ready. Run the pipeline to generate them.')
    st.code('bash scripts/run_all.sh',language='bash'); st.stop()

@st.cache_data
def load(revision):
    return (pd.read_csv(RESULTS/'daily.csv',parse_dates=['date']),pd.read_csv(RESULTS/'hourly.csv',parse_dates=['timestamp']),
        pd.read_csv(RESULTS/'forecast_test.csv',parse_dates=['date']),pd.read_csv(RESULTS/'anomalies.csv',parse_dates=['date']),
        json.loads((RESULTS/'manifest.json').read_text()),json.loads((RESULTS/'metrics.json').read_text()))
d,h,f,anomalies,manifest,metrics=load(tuple((RESULTS/n).stat().st_mtime_ns for n in FILES))
q=manifest['quality']
with st.sidebar:
    st.markdown('<div class="brand"><span>⚡</span> WattWise</div><div class="muted">Household electricity analytics</div>',unsafe_allow_html=True)
    st.divider()
    page=st.radio('Workspace',['Overview','Consumption','Forecasting','Anomalies','Data & platform','Project & downloads'],label_visibility='collapsed')
    st.divider(); st.subheader('Date range')
    year=st.selectbox('Quick range',['All years']+[str(y) for y in sorted(d.date.dt.year.unique())],key='range_preset')
    bounds=d if year=='All years' else d[d.date.dt.year==int(year)]
    dates=st.date_input('Observation window',value=(bounds.date.min().date(),bounds.date.max().date()),
        min_value=d.date.min().date(),max_value=d.date.max().date(),key=f'dates_{year}')
    complete=st.toggle('Require ≥95% daily coverage',value=True,help='Excludes partial days and days with more than 5% missing readings.')
    st.caption('Consumption and anomalies use both filters. Forecast charts use dates; benchmark metrics retain the full test period.')
    st.divider(); st.markdown('<span class="pill">VERIFIED HISTORICAL DATA</span>',unsafe_allow_html=True)
    st.caption('One household · Sceaux, France\n\n16 Dec 2006 – 26 Nov 2010')
if len(dates)!=2: st.info('Choose both a start date and an end date.'); st.stop()
start,end=dates
raw_window=select_days(d,start,end,False)
window=select_days(d,start,end,complete)
excluded=len(raw_window)-len(window)
name=metrics['selected_on_validation']; full_score=metrics['test'][name]
TITLES={
 'Overview':('Household energy, understood.','Electricity demand, data quality, and forecast performance at a glance.'),
 'Consumption':('Explore consumption.','Daily demand, monitored circuits, and the household’s weekly rhythm.'),
 'Forecasting':('Measure predictive performance.','Next-day predictions compared with a weekly baseline on future dates.'),
 'Anomalies':('Investigate unusual days.','Review deviations from recent demand before drawing conclusions.'),
 'Data & platform':('From source to verified results.','Missing readings, data lineage, and completed Hadoop/YARN applications.'),
 'Project & downloads':('A reproducible big data project.','Methodology, deliverables, and instructions for running the project.')}
title,subtitle=TITLES[page]
st.markdown(f'<div class="eyebrow">WATTWISE / {html.escape(page.upper())}</div>',unsafe_allow_html=True)
st.title(title); st.markdown(f'<div class="subtitle">{subtitle}</div>',unsafe_allow_html=True)
if page in ['Overview','Consumption','Anomalies']:
    st.markdown(f'<div class="scope">{start:%d %b %Y} → {end:%d %b %Y} · {len(window):,} days shown · {excluded:,} days excluded by coverage</div>',unsafe_allow_html=True)

def chart(fig,height=350):
    fig.update_layout(template='plotly_dark',paper_bgcolor='#0c1321',plot_bgcolor='#0c1321',colorway=[TEAL,BLUE,AMBER,PURPLE],
        font=dict(color='#b7c6da',size=12),margin=dict(l=12,r=12,t=30,b=30),height=height,
        legend=dict(orientation='h',y=1.13,x=0),hovermode='x unified')
    fig.update_xaxes(showgrid=False); fig.update_yaxes(gridcolor='#243249',zeroline=False)
    st.plotly_chart(fig,width='stretch',config={'displaylogo':False,'modeBarButtonsToRemove':['lasso2d','select2d']})

def demand_plot():
    s=calendar_series(window,start,end)
    fig=go.Figure(go.Scatter(x=s.index,y=s.values,name='Observed energy',mode='lines',line=dict(color=TEAL,width=1.6),
        connectgaps=False,hovertemplate='%{x|%d %b %Y}<br>%{y:.2f} kWh<extra></extra>'))
    fig.update_layout(yaxis_title='Daily energy (kWh)'); chart(fig)

if page in ['Overview','Consumption','Anomalies'] and window.empty:
    st.warning('No days meet the coverage requirement. Expand the date range or turn off the coverage filter.'); st.stop()
if page in ['Overview','Consumption'] and window.valid_minutes.sum()==0:
    st.warning('This range has no valid power readings. Energy cannot be estimated.'); st.stop()

if page=='Overview':
    c=st.columns(4)
    c[0].metric('Observed energy',f'{window.energy_kwh.sum():,.0f} kWh')
    c[1].metric('Average per observed day',f'{window.energy_kwh.mean():.1f} kWh')
    c[2].metric('Peak minute load',f'{window.peak_kw.max():.2f} kW')
    c[3].metric('Reading coverage',f'{window.valid_minutes.sum()/(len(window)*1440):.2%}')
    st.subheader('Daily electricity demand'); st.caption('Gaps represent missing or excluded days. Totals use observed readings only.'); demand_plot()
    maximum=window.loc[window.energy_kwh.idxmax()]
    st.markdown(f'<div class="insight"><b>Highest observed day:</b> {maximum.date:%d %b %Y}, with {maximum.energy_kwh:.1f} kWh. Average daily use in this selection is {window.energy_kwh.mean():.1f} kWh.</div>',unsafe_allow_html=True)
    st.subheader('Project at a glance')
    st.write(f'{q["rows"]:,} source readings processed with Spark on YARN. Hadoop MapReduce independently verifies daily energy totals and valid-reading counts.')
    with st.container(border=True):
        st.markdown(f'**Forecast benchmark · {name}**')
        st.write(f'Test MAE: {full_score["mae_kwh"]:.2f} kWh/day. Selected on validation before final test evaluation.')
        st.caption('This benchmark describes the full test period, regardless of the date filter.')

elif page=='Consumption':
    st.subheader('Demand over time'); demand_plot()
    st.subheader('Energy by monitored circuit')
    values=window[['kitchen_kwh','laundry_kwh','climate_kwh','other_kwh']].sum()
    circuits=pd.DataFrame({'Circuit':['Kitchen','Laundry','Water heating / climate','Other residual'],'Energy (kWh)':values.values})
    circuits['Share (%)']=circuits['Energy (kWh)']/circuits['Energy (kWh)'].sum()*100
    fig=px.bar(circuits,x='Energy (kWh)',y='Circuit',orientation='h',text='Share (%)',color='Circuit',color_discrete_sequence=[TEAL,BLUE,PURPLE,AMBER])
    fig.update_traces(texttemplate='%{text:.1f}%',textposition='outside')
    fig.update_layout(showlegend=False,xaxis_range=[0,circuits['Energy (kWh)'].max()*1.15]); chart(fig,290)
    st.caption('Circuit 3 combines water heating and air conditioning. Other residual is the difference from total measured energy.')
    st.subheader('Hourly demand across the week'); grid=weekly_heatmap(h,window.date)
    if grid.notna().any(axis=None):
        fig=go.Figure(go.Heatmap(z=grid.values,x=list(range(24)),y=['Mon','Tue','Wed','Thu','Fri','Sat','Sun'],
            colorscale=[[0,'#15243a'],[.5,'#26887f'],[1,TEAL]],colorbar=dict(title='kW'),
            hovertemplate='%{y}, %{x}:00<br>Mean demand %{z:.2f} kW<extra></extra>'))
        fig.update_layout(xaxis_title='Hour of day',yaxis=dict(autorange='reversed')); chart(fig,340)
    else: st.info('No hours have sufficient coverage for a demand profile in this selection.')
    st.caption('Only hours with ≥95% coverage contribute. Blank cells have no eligible observations.')
    with st.expander('Inspect selected daily readings'):
        st.dataframe(window[['date','energy_kwh','peak_kw','coverage']].rename(columns={'date':'Date','energy_kwh':'Energy (kWh)','peak_kw':'Peak (kW)','coverage':'Coverage'}),hide_index=True,width='stretch')

elif page=='Forecasting':
    st.markdown('<div class="scope">One-day-ahead forecast · model selected on validation · benchmark scores use the full test period</div>',unsafe_allow_html=True)
    improvement=(1-full_score['mae_kwh']/metrics['test']['Weekly baseline']['mae_kwh'])*100
    c=st.columns(3); c[0].metric('Selected model',name); c[1].metric('Full-test MAE',f'{full_score["mae_kwh"]:.2f} kWh'); c[2].metric('MAE reduction vs baseline',f'{improvement:.1f}%')
    selected=forecast_window(f,start,end); st.subheader('Predictions versus observations')
    if selected.empty:
        split=metrics['splits']['test']
        st.info(f'No evaluated predictions fall in this range. The test period is {split["start"]} to {split["end"]}. Select dates within it.')
    else:
        fig=go.Figure()
        for col,label,color in [('actual_kwh','Observed',TEAL),('prediction_kwh',name+' forecast',BLUE),('weekly_baseline_kwh','Previous-week baseline',AMBER)]:
            series=selected.set_index('date')[col].reindex(pd.date_range(selected.date.min(),selected.date.max()))
            fig.add_trace(go.Scatter(x=series.index,y=series.values,name=label,line=dict(color=color,width=1.8),connectgaps=False))
        fig.update_layout(yaxis_title='Daily energy (kWh)'); chart(fig,400)
        mae=(selected.actual_kwh-selected.prediction_kwh).abs().mean()
        st.caption(f'{len(selected):,} evaluated days shown · selection MAE {mae:.2f} kWh. Incomplete feature windows have no prediction.')
        st.download_button('Export selected predictions',selected.to_csv(index=False).encode(),'forecast_selection.csv','text/csv')
    st.subheader('Model comparison')
    table=pd.DataFrame([{'Model':n,'Validation MAE (kWh)':metrics['validation'][n]['mae_kwh'],'Test MAE (kWh)':v['mae_kwh'],
        'Test RMSE (kWh)':v['rmse_kwh'],'Test WAPE (%)':v['wape_percent'],'Selected':n==name} for n,v in metrics['test'].items()])
    st.dataframe(table.round(3),hide_index=True,width='stretch')
    st.caption('Lowest validation MAE selects the model. Test scores do not change selection. MAE is average absolute error; WAPE is total absolute error divided by observed energy.')
    with st.expander('How evaluation works'):
        st.write(metrics['evaluation']); st.dataframe(pd.DataFrame(metrics['splits']).T,width='stretch')
        st.write('Inputs use calendar lags, shifted rolling statistics, weekday and annual seasonality. Incomplete targets and feature windows are excluded.')
    if metrics['next_day_forecast'] is None:
        st.info('A forecast beyond the historical dataset is withheld because the final lag window is incomplete.')

elif page=='Anomalies':
    aw=anomalies[anomalies.date.isin(window.date)].copy()
    c=st.columns(3); c[0].metric('Flagged days',len(aw)); c[1].metric('Days shown',len(window)); c[2].metric('Rule threshold','|z| > 3.5')
    st.subheader('Unusual daily demand'); s=calendar_series(window,start,end)
    fig=go.Figure(go.Scatter(x=s.index,y=s.values,name='Observed energy',line=dict(color=BLUE),connectgaps=False))
    fig.add_trace(go.Scatter(x=aw.date,y=aw.energy_kwh,name='Flagged day',mode='markers',marker=dict(color=AMBER,size=9)))
    fig.update_layout(yaxis_title='Daily energy (kWh)'); chart(fig)
    st.caption('Past-only reference: previous 28 days, minimum 14 eligible observations. Flags identify unusual consumption; they do not prove an electrical fault.')
    if aw.empty: st.success('No flagged days in this selection.')
    else:
        aw['Direction']=aw.robust_z.map(lambda z:'Higher than recent demand' if z>0 else 'Lower than recent demand')
        st.dataframe(aw.sort_values('robust_z',key=abs,ascending=False).rename(columns={'date':'Date','energy_kwh':'Energy (kWh)','past_median_kwh':'Prior median (kWh)','robust_z':'Robust z'}).round(2),hide_index=True,width='stretch')
        st.download_button('Export flagged days',aw.to_csv(index=False).encode(),'anomaly_selection.csv','text/csv')

elif page=='Data & platform':
    st.markdown('<div class="scope">Quality and execution evidence cover the entire dataset. The coverage chart follows selected dates.</div>',unsafe_allow_html=True)
    c=st.columns(3); c[0].metric('Source readings',f'{q["rows"]:,}'); c[1].metric('Missing power readings',f'{q["missing_global_active_power"]:,}'); c[2].metric('Invalid / duplicate timestamps',f'{q["invalid_timestamps"]} / {q["duplicate_timestamps"]}')
    st.subheader('Measurement coverage')
    fig=px.line(raw_window,x='date',y='coverage',color_discrete_sequence=[AMBER],labels={'coverage':'Valid minute readings','date':'Date'})
    fig.update_yaxes(tickformat='.0%',range=[0,1.05]); fig.add_hline(y=.95,line_dash='dot',line_color=TEAL); chart(fig,300)
    st.caption('Each calendar day expects 1,440 readings. The dotted line is the 95% modeling threshold.')
    st.subheader('Processing architecture')
    stages=[('Source text','Original minute-level readings'),('HDFS','Raw data and partitioned Parquet'),('YARN + Spark','Distributed validation and aggregation'),
            ('MapReduce','Independent daily-total verification'),('Forecast models','Chronological daily evaluation'),('Dashboard','Read-only exploration and exports')]
    st.markdown('<div class="pipeline">'+''.join(f'<div class="stage">{i:02} · {a}<small>{b}</small></div>' for i,(a,b) in enumerate(stages,1))+'</div>',unsafe_allow_html=True)
    st.caption('Verified on one machine in pseudo-distributed mode. Models train on small daily aggregates in the client process.')
    apps_file=RESULTS/'yarn_applications.json'
    if apps_file.exists():
        apps=json.loads(apps_file.read_text()).get('apps') or {}
        st.subheader('Completed YARN applications')
        st.dataframe(pd.DataFrame([{'Application':a['name'],'ID':a['id'],'State':a['state'],'Result':a['finalStatus']} for a in apps.get('app',[])]),hide_index=True,width='stretch')
        st.caption('Saved evidence of completed runs. This is not a live cluster-status monitor.')
    with st.expander('Validation checks and source metadata'):
        v=RESULTS/'verification.json'
        if v.exists(): st.markdown('\n'.join('- ✓ '+check for check in json.loads(v.read_text())['passed']))
        st.json(manifest,expanded=False)

else:
    st.subheader('Project summary')
    st.write('Analyze household electricity with HDFS, YARN, Spark SQL and independent Hadoop Streaming. Evaluate forecasting on later dates and flag unusual consumption using a past-only rule.')
    st.markdown('**Dataset:** UCI Individual Household Electric Power Consumption · 2,075,259 minute readings · one household in Sceaux, France.')
    st.markdown('**Limits:** historical data from one home; missing energy is not imputed; one worker; anomaly flags have no labeled fault ground truth.')
    st.subheader('Reports and evidence')
    for label,path,mime in [('Download advanced project guide',ROOT/'docs/ADVANCED_README.html','text/html'),('Download offline report',RESULTS/'PROJECT_REPORT.html','text/html'),('Download submission report',ROOT/'docs/PROJECT_REPORT.md','text/markdown'),
                           ('Download viva guide',ROOT/'docs/VIVA.md','text/markdown'),('Download execution evidence',RESULTS/'yarn_applications.json','application/json')]:
        if path.exists(): st.download_button(label,path.read_bytes(),path.name,mime,key=path.name)
    st.subheader('Export selected observations'); st.caption(f'{len(window):,} rows under the current date and coverage filters.')
    st.download_button('Download selected daily readings',window.to_csv(index=False).encode(),'daily_selection.csv','text/csv')
    st.subheader('Run the project'); st.code('cd /Users/neev/Downloads/BIGDATA/WattWise\nbash scripts/run_all.sh',language='bash')
    st.caption('The run starts the isolated cluster, executes and checks the jobs, generates reports, and stops the cluster. The dashboard uses saved results.')
    with st.expander('Read setup instructions'): st.markdown((ROOT/'README.md').read_text())
st.divider(); st.caption('WATTWISE · UCI household power dataset · observed energy, never estimated missing consumption')
