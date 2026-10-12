"""Readable, self-contained HTML layout for the generated academic report."""
import html
import re
import markdown

ARCHITECTURE='''<div class="architecture"><div>Original text <span>2.08 million readings</span></div><b>→</b><div>HDFS raw layer <span>Copy of source file</span></div><b>→</b><div>YARN containers <span>Spark SQL + MapReduce</span></div><b>→</b><div>Verified outputs <span>Parquet · aggregates · models</span></div></div>'''

def render_report(report, figures, manifest, metrics):
    clean=re.sub(r'```mermaid\n.*?```',ARCHITECTURE,report,flags=re.S)
    # The report's content is generated locally from the known template and measurements.
    md=markdown.Markdown(extensions=['tables','fenced_code','toc'])
    body=md.convert(clean.split('\n',1)[1])
    charts=[]
    for i,(title,fig) in enumerate(figures):
        fig.update_layout(template='plotly_dark',paper_bgcolor='#111e31',plot_bgcolor='#111e31',height=380,
            margin=dict(l=30,r=20,t=45,b=35),colorway=['#50dbc0','#7ea5ff','#f4bd72','#be9bff'],
            legend=dict(orientation='h',y=1.13))
        charts.append(f'<section><h2>{html.escape(title)}</h2>'+fig.to_html(full_html=False,include_plotlyjs=i==0,config={'displaylogo':False})+'</section>')
    q=manifest['quality']; name=metrics['selected_on_validation']; score=metrics['test'][name]
    reduction=(1-score['mae_kwh']/metrics['test']['Weekly baseline']['mae_kwh'])*100
    return f'''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>WattWise | Household Electricity Big Data Project</title><style>
*{{box-sizing:border-box}}html{{scroll-behavior:smooth}}body{{margin:0;background:#0c1321;color:#edf2fa;font:15px/1.75 system-ui}}
main{{max-width:1120px;margin:auto;padding:52px 28px}}h1{{font-size:clamp(32px,5vw,54px);line-height:1.12;letter-spacing:-2px;margin:15px 0 22px}}
h2{{font-size:24px;line-height:1.3;letter-spacing:-.6px;margin-top:36px;scroll-margin-top:20px}}h3{{font-size:19px}}p,li{{color:#bdcce0}}.tag{{color:#50dbc0;letter-spacing:2px;font-size:11px;font-weight:700}}
.lead{{font-size:18px;max-width:800px}}.cards{{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:14px;margin:28px 0}}.card,section{{background:#111e31;border:1px solid #28374e;border-radius:12px;padding:24px;margin-bottom:20px}}.number{{font-size:25px;color:#50dbc0;font-weight:650;letter-spacing:-.5px}}
section h2{{margin-top:0}}a{{color:#65e2ca;overflow-wrap:anywhere}}code{{font:13px ui-monospace;background:#18263b;border-radius:4px;padding:3px 5px;overflow-wrap:anywhere}}pre{{white-space:pre-wrap;background:#18263b;border-radius:8px;padding:18px;overflow-wrap:anywhere;line-height:1.6}}pre code{{padding:0;background:transparent}}table{{border-collapse:collapse;width:100%;font-size:14px;margin:20px 0}}th,td{{padding:12px;border-bottom:1px solid #30415b;text-align:left}}th{{background:#182b43;color:#70e5ce}}.contents ul{{list-style:none;padding:0;columns:2}}.contents a{{text-decoration:none}}.architecture{{display:flex;align-items:center;gap:10px;flex-wrap:wrap;padding:20px;background:#152438;border:1px solid #2f4964;border-radius:10px}}.architecture div{{flex:1;min-width:130px;color:#67e0c8;font-weight:650}}.architecture span{{display:block;color:#abc0da;font-size:12px;font-weight:400}}.report{{padding:32px}}.report>h2{{margin-top:36px;border-top:1px solid #30415b;padding-top:28px}}
@media(max-width:720px){{main{{padding:28px 15px}}.cards{{grid-template-columns:repeat(2,minmax(0,1fr))}}.card,section{{padding:18px}}.number{{font-size:21px}}.contents ul{{columns:1}}table{{display:block;overflow-x:auto}}}}
@media print{{body{{background:white;color:#172438}}main{{max-width:none;padding:0}}.card,section{{background:white;border-color:#ccd5df}}p,li{{color:#263a51}}h2{{break-after:avoid}}table,.architecture{{break-inside:avoid}}a{{color:#12685b}}.contents{{display:none}}.report{{padding:0}}}}
</style></head><body><main><div class="tag">BIG DATA COURSE PROJECT · HADOOP / YARN / SPARK</div>
<h1>Household electricity,<br>measured and understood.</h1><p class="lead">WattWise analyzes the supplied minute-level dataset through a verified Hadoop pipeline and evaluates next-day energy forecasting on later dates.</p>
<p>One household · Sceaux, France · {q['start'][:10]} to {q['end'][:10]} · Historical observations</p>
<div class="cards"><div class="card">Source readings<div class="number">{q['rows']:,}</div></div><div class="card">Observed energy<div class="number">{manifest['totals']['observed_energy_kwh']:,.0f} kWh</div></div><div class="card">Missing readings<div class="number">{100*q['missing_global_active_power']/q['rows']:.2f}%</div></div><div class="card">Selected-model test MAE<div class="number">{score['mae_kwh']:.2f} kWh</div></div></div>
<section><h2>Verified findings</h2><p>Spark and Hadoop MapReduce agree on daily observed energy and valid-reading counts. The validation-selected <strong>{html.escape(name)}</strong> model reduces final-test MAE by <strong>{reduction:.1f}%</strong> relative to the weekly baseline.</p><p>Missing energy is not imputed. Execution uses a single-node pseudo-distributed cluster. Model scores describe rolling one-day-ahead prediction; anomaly flags require investigation.</p></section>
{''.join(charts)}<section class="contents"><h2>Methodology and submission report</h2>{md.toc}</section><section class="report">{body}</section>
<p>Self-contained report · Interactive charts work offline · Source, methodology, and execution evidence included</p></main></body></html>'''
