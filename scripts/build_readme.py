"""Export README with offline SVG diagrams, readable tables, and contents links."""
from pathlib import Path
import html
import re
import markdown

def node(x,y,w,h,lines,accent=False):
    rect=f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="10" fill="{"#14392f" if accent else "#17263b"}" stroke="#42607c"/>'
    text=''.join(f'<text x="{x+w/2}" y="{y+25+i*19}" text-anchor="middle" fill="{"#70e7cb" if i==0 else "#bdcce0"}" font-size="{14 if i==0 else 12}">{html.escape(line)}</text>' for i,line in enumerate(lines))
    return rect+text

def edge(x1,y1,x2,y2):
    return f'<path d="M{x1},{y1} L{x2},{y2}" stroke="#60cdb6" stroke-width="1.7" fill="none" marker-end="url(#arrow)"/>'

def svg(content,height,label):
    return f'<div class="diagram"><svg role="img" aria-label="{label}" viewBox="0 0 980 {height}" xmlns="http://www.w3.org/2000/svg"><defs><marker id="arrow" markerWidth="9" markerHeight="9" refX="8" refY="4" orient="auto"><path d="M0,0 L8,4 L0,8" fill="#60cdb6"/></marker></defs>{content}</svg></div>'

def logical():
    boxes=[(350,15,280,65,['Supplied minute-level text','Original source file'],True),
      (350,115,280,65,['HDFS raw layer','Copy of original text'],False),
      (55,225,270,65,['Spark SQL on YARN','Typed ETL and aggregation'],False),
      (655,225,270,65,['MapReduce on YARN','Independent daily totals'],False),
      (20,335,210,65,['Partitioned Parquet','HDFS silver lake'],False),
      (275,335,235,65,['Daily / hourly CSV','Local analytical aggregates'],False),
      (660,335,255,65,['Cross-framework check','Energy and valid counts'],False),
      (270,445,245,65,['Forecast + anomaly rules','Client-side daily analysis'],False),
      (660,445,255,65,['Evidence + reports','Verified run artifacts'],False),
      (350,555,280,65,['Read-only dashboard','Consumption / models / evidence'],True)]
    lines=[(490,80,490,115),(445,180,190,225),(535,180,790,225),(135,290,125,335),(240,290,395,335),
      (790,290,790,335),(510,365,660,365),(395,400,395,445),(790,400,790,445),(515,478,660,478),
      (395,510,440,555),(790,510,550,555),(510,390,610,555)]
    return svg(''.join(edge(*e) for e in lines)+''.join(node(*n) for n in boxes),645,'Logical data flow: independent Spark and MapReduce branches')

def deployment():
    s='<rect x="5" y="5" width="970" height="360" rx="14" fill="#102035" stroke="#42607c"/><text x="490" y="34" text-anchor="middle" fill="#70e7cb" font-size="17">ONE PHYSICAL MAC · SEPARATE SERVICE PROCESSES</text>'
    s+=node(25,65,285,255,['CLIENT PROCESSES','Run scripts / spark-submit','Python Spark driver','scikit-learn daily models','Streamlit dashboard'])
    s+=node(345,65,285,255,['HDFS STORAGE','NameNode: metadata','DataNode: data blocks','Raw input + silver Parquet'])
    s+=node(665,65,285,255,['YARN EXECUTION','ResourceManager: scheduling','NodeManager: containers','ApplicationMaster','Spark executor / MR tasks'])
    s+='<text x="490" y="345" text-anchor="middle" fill="#bdcce0" font-size="12">Driver → executor; executor ↔ HDFS; YARN allocates containers. Models and UI use local aggregates.</text>'
    return svg(s,380,'Physical deployment: client, HDFS, and YARN processes on one Mac')

def lifecycle():
    labels=[('Initialize isolated config','Start project HDFS / YARN'),('Upload raw source','Submit Spark ETL on YARN'),
      ('Write Parquet + aggregates','Evaluate daily forecast / anomalies'),('Run independent MapReduce','Compare results and execute tests'),
      ('Capture YARN / HDFS evidence','Generate offline reports'),('Stop project services on exit','Dashboard reads saved results')]
    s=''
    for i,(a,b) in enumerate(labels):
        x=70 if i%2==0 else 535; y=25+(i//2)*130
        s+=node(x,y,375,80,[f'{i+1:02} · {a}',b],i in [0,5])
        if i%2==0: s+=edge(445,y+40,535,y+40)
        elif i<5: s+=f'<path d="M910,{y+40} L940,{y+40} L940,{y+110} L35,{y+110} L35,{y+170} L70,{y+170}" stroke="#60cdb6" stroke-width="1.7" fill="none" marker-end="url(#arrow)"/>'
    return svg(s,405,'Run lifecycle from cluster initialization to service shutdown')

def main():
    root=Path(__file__).resolve().parents[1]
    source=(root/'README.md').read_text()
    diagrams=iter([logical(),deployment(),lifecycle()])
    source=re.sub(r'```mermaid\n.*?```',lambda _:next(diagrams),source,flags=re.S)
    renderer=markdown.Markdown(extensions=['tables','fenced_code','toc'])
    body=renderer.convert(source)
    document=f'''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>WattWise · Advanced Project Guide</title><style>
*{{box-sizing:border-box}}body{{margin:0;background:#0c1321;color:#edf2fa;font:15px/1.8 system-ui}}main{{max-width:1160px;margin:auto;padding:45px 28px}}h1{{font-size:48px;letter-spacing:-2px;line-height:1.15;color:#70e7cb}}h2{{font-size:27px;letter-spacing:-.5px;border-top:1px solid #30415b;margin-top:45px;padding-top:25px;scroll-margin-top:20px}}h3{{font-size:19px}}p,li{{color:#bccce0}}a{{color:#65e2ca;overflow-wrap:anywhere}}code{{font:13px ui-monospace;background:#17263b;border-radius:4px;padding:3px 5px;overflow-wrap:anywhere}}pre{{padding:20px;border:1px solid #30415b;background:#142034;border-radius:9px;overflow:auto;line-height:1.6}}pre code{{padding:0;background:transparent}}table{{border-collapse:collapse;width:100%;font-size:13px;margin:24px 0}}th,td{{padding:12px;border-bottom:1px solid #30415b;text-align:left;vertical-align:top}}th{{background:#1a2c44;color:#70e7cb}}tr:nth-child(even){{background:#111e31}}.diagram{{background:#102035;border:1px solid #30415b;padding:12px;border-radius:12px;overflow:auto;margin:25px 0}}.diagram svg{{display:block;width:100%;min-width:740px;font-family:system-ui}}blockquote{{border-left:3px solid #65e2ca;padding-left:20px}}@media(max-width:700px){{main{{padding:24px 14px}}h1{{font-size:36px}}table{{display:block;overflow-x:auto}}h2{{font-size:23px}}}}@media print{{body{{background:white;color:#172438}}main{{padding:0;max-width:none}}p,li{{color:#263a51}}h2{{break-after:avoid}}table{{font-size:10px}}h1,th,a{{color:#176d5d}}pre{{white-space:pre-wrap}}.diagram{{break-inside:avoid}}}}
</style></head><body><main>{body}</main></body></html>'''
    (root/'docs').mkdir(exist_ok=True)
    (root/'docs/ADVANCED_README.html').write_text(document)
    print(f'Advanced guide rendered: {root / "docs/ADVANCED_README.html"}')

if __name__=='__main__': main()
