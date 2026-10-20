"""Verify measured evidence before producing a portable, candid submission kit."""
from __future__ import annotations
import hashlib
import json
import re
import shutil
import math
from pathlib import Path
from typing import Any
from .runner import comparable, require_recovery, ROOT
from .effectiveness import TASKS
from .workload import STREAM_SCALES
from .analytics import ANALYTICS_SCALES


def _timing(value: dict, count: int) -> None:
    numbers=[value.get('p'+str(p)+'_ms') for p in (50,95,99)]
    if (type(value.get('samples')) is not int or value['samples']!=count or any(type(v) not in (int,float) or not math.isfinite(v) or v<0 for v in numbers)
            or numbers!=sorted(numbers)):
        raise ValueError('invalid measured percentile evidence')


def _measurement(value: Any, *, positive: bool = False, integer: bool = False) -> None:
    allowed=(int,) if integer else (int,float)
    if type(value) not in allowed or not math.isfinite(value) or value<0 or positive and value==0:
        raise ValueError('measurement must be correctly typed, finite and '+('positive' if positive else 'nonnegative'))


def acceptance_status(group_size: int | None) -> dict[str, Any]:
    if group_size is not None and (type(group_size) is not int or group_size < 1):
        raise ValueError('group size must be a positive integer')
    return {'human_study':'pending','narrated_video':'pending',
            'group_coverage':'unconfirmed' if group_size is None else 'covered' if 2*group_size<=len(TASKS) else 'insufficient',
            'group_size':group_size,'business_requirements':len(TASKS),'final_submission_ready':False}


def validate_runs(runs: list[dict[str, Any]]) -> dict[str, Any]:
    selected = {}
    for run in runs:
        if run.get('version') != 1: raise ValueError('unsupported evidence version')
        if run.get('warmup') or run.get('scale')=='analytics-smoke': continue
        kind=run.get('kind')
        if kind in ('stream','analytics'):
            if not run.get('correctness',{}).get('passed'): raise ValueError('failed correctness evidence')
            if not run.get('resources'): raise ValueError('missing resource observations')
            for resource in run['resources'].values():
                for field in ('samples','failed_samples','peak_sampled_working_set_bytes'):
                    _measurement(resource.get(field),integer=True)
                if resource['samples']<2: raise ValueError('insufficient resource observations')
                _measurement(resource.get('cpu_seconds_observed'))
                for field in ('coverage_seconds','interval_seconds'):
                    _measurement(resource.get(field),positive=True)
                if type(resource.get('cpu_reset_may_undercount_restart')) is not bool:
                    raise ValueError('missing CPU reset interpretation')
            if kind=='stream':
                _measurement(run['correctness'].get('count'),integer=True,positive=True)
                if run['correctness']['count']!=STREAM_SCALES.get(run['scale']): raise ValueError('stream scale/count mismatch')
                _timing(run['latency'],run['correctness']['count'])
                _measurement(run.get('elapsed_seconds'),positive=True)
                _measurement(run.get('producer_ack_seconds'))
                _measurement(run.get('throughput_inputs_per_second'),positive=True)
                if not math.isclose(run['throughput_inputs_per_second'],run['correctness']['count']/run['elapsed_seconds'],rel_tol=1e-9):
                    raise ValueError('throughput differs from count/elapsed observation')
            else:
                _measurement(run.get('row_count'),integer=True,positive=True)
                if run['row_count']!=ANALYTICS_SCALES.get(run['scale']): raise ValueError('analytical scale/count mismatch')
                _measurement(run.get('load_seconds'))
            key=(kind,run['engine'],run['scale'],bool(run.get('recovery')))
            if key in selected: raise ValueError('ambiguous duplicate benchmark selection')
            selected[key]=run
    for scale in ('eval-small','eval-medium'):
        pair=[]
        for engine in ('flink','streams'):
            run=selected.get(('stream',engine,scale,False))
            if run is None: raise ValueError('missing comparable stream run')
            pair.append(run['correctness'])
        comparable(*pair)
    for engine in ('flink','streams'):
        recovery=[r for key,r in selected.items() if key[0]=='stream' and key[1]==engine and key[3]]
        if not recovery: raise ValueError('missing engine recovery run')
        for run in recovery: require_recovery(run['recovery'],engine,True)
    for scale in ('analytics-1m','analytics-10m','analytics-20m'):
        for engine in ('clickhouse','postgres'):
            run=selected.get(('analytics',engine,scale,False))
            if run is None: raise ValueError('missing analytical scale/engine')
            if set(run['workloads'])!={'platform','campaign_day','filtered'}: raise ValueError('missing SQL workload')
            if any(w['samples']!=100 or w['warmups']!=20 or w['failures'] for w in run['workloads'].values()):
                raise ValueError('analytical timing requires 20 warmups, 100 correct measurements')
            for value in run['workloads'].values(): _timing(value,100)
    apps=[r for r in runs if r.get('kind')=='app']
    if len(apps)!=1 or {r['id'] for r in apps[0]['decisions'] if r.get('passed')}!={r['id'] for r in TASKS}:
        raise ValueError('missing ten successful application decisions')
    if len(apps[0]['decisions'])!=10 or apps[0].get('human_study')!='pending':
        raise ValueError('automated decisions cannot establish a human study')
    app=apps[0]
    if app.get('operational_configuration_preserved') is not True or app.get('role') not in ('operator','admin'):
        raise ValueError('application proof must preserve configuration and use an authorized role')
    if any(r.get('mode')!='automated_api_decision_readiness' or type(r.get('seconds')) not in (int,float)
           or not math.isfinite(r['seconds']) or r['seconds']<0 for r in app['decisions']):
        raise ValueError('invalid automated decision observation')
    _timing(app['dashboard_http'],100)
    if app['dashboard_http'].get('warmups')!=20: raise ValueError('missing dashboard warmups')
    return {'technical_evidence':'passed','selected_runs':len(selected),'decisions':10}


def verify_index(root: Path, files: dict[str,str]) -> None:
    for name, expected in files.items():
        path=(root/name).resolve()
        if not path.is_relative_to(root.resolve()) or not path.is_file(): raise ValueError('artifact escapes package or missing')
        if hashlib.sha256(path.read_bytes()).hexdigest()!=expected: raise ValueError('artifact content hash differs')


def performance_tables(runs: list[dict]) -> tuple[list[list[str]],list[list[str]]]:
    streams=[['Engine / scale','inputs/s','p50 ms','p95 ms','p99 ms']]
    analytical=[['Engine / scale / query','p50 ms','p95 ms','p99 ms']]
    for run in runs:
        if run.get('warmup'): continue
        if run.get('kind')=='stream' and not run.get('recovery'):
            streams.append([run['engine']+' / '+run['scale'],f"{run['throughput_inputs_per_second']:.1f}",
                            *[f"{run['latency']['p'+str(p)+'_ms']:.2f}" for p in (50,95,99)]])
        if run.get('kind')=='analytics' and run['scale']!='analytics-smoke':
            for name,value in run['workloads'].items():
                analytical.append([run['engine']+' / '+run['scale']+' / '+name,*[f"{value['p'+str(p)+'_ms']:.2f}" for p in (50,95,99)]])
    return streams,analytical


def resource_table(runs: list[dict]) -> list[list[str]]:
    result=[['Engine / scale','Process','CPU s','Peak MiB','Samples','Cover s']]
    for run in runs:
        if run.get('kind') not in ('stream','analytics') or run.get('warmup') or run.get('scale')=='analytics-smoke':continue
        label=run['engine']+'/'+run['scale']+('/recovery' if run.get('recovery') else '')
        for name,r in run['resources'].items():
            process='streams' if name.startswith('mp10-') else name.removeprefix('marketpulse-evaluation-').removesuffix('-1')
            result.append([label,process,f"{r['cpu_seconds_observed']:.2f}"+('*' if r.get('cpu_reset_may_undercount_restart') else ''),
                f"{r['peak_sampled_working_set_bytes']/1048576:.1f}",str(r['samples']),f"{r['coverage_seconds']:.1f}"])
    return result


def build_package(paths: list[Path], output: Path, group_size: int | None = None) -> dict[str, Any]:
    runs=[json.loads(path.read_text()) for path in paths]
    validation=validate_runs(runs);status=acceptance_status(group_size)
    output.mkdir(parents=True,exist_ok=True)
    evidence=output/'evidence';evidence.mkdir(exist_ok=True)
    for path in paths: shutil.copyfile(path,evidence/path.name)
    report=(ROOT/'docs/evaluation/report-template.md').read_text()
    for name in ('decision-tasks.md','video-storyboard.md'):
        shutil.copyfile(ROOT/'docs/evaluation'/name,output/name)
    streams,analytical=performance_tables(runs)
    resources=resource_table(runs)
    app=next(r for r in runs if r.get('kind')=='app')
    recovery=[f"{r['engine']}: {r['recovery']['seconds']:.3f}s to new committed output; "+
        (f"checkpoint {r['recovery']['restored_checkpoint']} restored" if r['engine']=='flink' else 'application ID/state retained')
        for r in runs if r.get('recovery')]
    tables='\n\n'+'\n'.join(' | '.join(row) for row in streams)+'\n\n'+'\n'.join(' | '.join(row) for row in analytical)
    final_report=report.replace('{{PERFORMANCE}}',tables).replace('{{RECOVERY}}','; '.join(recovery))
    machine=next(r['metadata']['docker'] for r in runs if r.get('metadata'))
    final_report=final_report.replace('{{MACHINE}}',f"Docker: {machine['NCPU']} CPUs; {machine['MemTotal']/1073741824:.2f} GiB visible memory; {machine['Architecture']}; engine {machine['ServerVersion']}.")
    final_report=final_report.replace('{{RESOURCES}}','\n'.join(' | '.join(row) for row in resources))
    group=('The real group size has not been supplied; requirement coverage remains unconfirmed.' if group_size is None else
           f'Reported group size: {group_size}; 2*n requirement coverage: {status["group_coverage"]}.')
    final_report=final_report.replace('{{GROUP}}',group)
    http=app['dashboard_http']
    http_text='; '.join(f'p{p}: {http["p"+str(p)+"_ms"]:.2f} ms' for p in (50,95,99))+'; 100 samples after 20 warmups.'
    final_report=final_report.replace('{{HTTP}}',http_text).replace('{{ACCEPTANCE}}',json.dumps(status,sort_keys=True))
    (output/'report.md').write_text(final_report)
    _pdf_report(output/'report.pdf',final_report,streams,analytical,resources)
    _pdf_slides(output/'slides.pdf',streams,analytical,recovery,app,status)
    files={str(p.relative_to(output)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(output.rglob('*'))
           if p.is_file() and p.name!='index.json'}
    verify_index(output,files)
    index={'version':1,**validation,'acceptance':status,'files':files,
           'submission_notice':'Draft filenames: rename with real group ID. Narrated 20–30 minute video and human observations still required.'}
    (output/'index.json').write_text(json.dumps(index,indent=2,sort_keys=True)+'\n')
    return index


def _pdf_report(path: Path, text: str, streams: list[list[str]], analytics: list[list[str]], resources: list[list[str]]) -> None:
    from reportlab.lib import colors
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib.enums import TA_LEFT
    from reportlab.platypus import SimpleDocTemplate,Paragraph,Spacer,Table,TableStyle,PageBreak
    from xml.sax.saxutils import escape
    styles=getSampleStyleSheet();styles['BodyText'].fontSize=10;styles['BodyText'].leading=15
    story=[]
    for line in text.splitlines():
        if not line.strip(): continue
        # Numeric tables are laid out as real tables below the methodology text.
        if ' | ' in line: continue
        if line.startswith('## '):
            if story:story.append(Spacer(1,16))
            story.append(Paragraph(escape(line[3:]),styles['Heading1']))
        elif line.startswith('# '): story.append(Paragraph(escape(line[2:]),styles['Title']))
        else: story.append(Paragraph(escape(line),styles['BodyText']))
    story.append(PageBreak());story.append(Paragraph('Measured comparison tables',styles['Heading1']))
    for rows in (streams,analytics,resources):
        widths=([235,65,65,65,65] if rows is streams else [275,73,73,74] if rows is analytics else [180,75,55,65,55,65])
        table=Table(rows,colWidths=widths,repeatRows=1,hAlign='LEFT')
        table.setStyle(TableStyle([('FONTNAME',(0,0),(-1,0),'Helvetica-Bold'),('FONTSIZE',(0,0),(-1,-1),8),
            ('BACKGROUND',(0,0),(-1,0),colors.HexColor('#dbeafe')),('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.white,colors.HexColor('#f1f5f9')]),
            ('TOPPADDING',(0,0),(-1,-1),7),('BOTTOMPADDING',(0,0),(-1,-1),7)]))
        story.extend([table,Spacer(1,20)])
    def footer(canvas,doc):
        canvas.setFont('Helvetica',8);canvas.drawString(50,25,'MarketPulse 360 | measured evidence; human/video acceptance pending')
        canvas.drawRightString(545,25,str(doc.page))
    SimpleDocTemplate(str(path),leftMargin=50,rightMargin=50,topMargin=45,bottomMargin=45,title='MarketPulse 360 technical report').build(story,onFirstPage=footer,onLaterPages=footer)


def _pdf_slides(path: Path, streams: list[list[str]], analytics: list[list[str]], recovery: list[str], app: dict, status: dict) -> None:
    from reportlab.pdfgen import canvas
    from reportlab.lib import colors
    from reportlab.platypus import Paragraph
    from reportlab.lib.styles import ParagraphStyle
    from xml.sax.saxutils import escape
    c=canvas.Canvas(str(path),pagesize=(960,540));c.setTitle('MarketPulse 360 evidence review')
    slides=[
        ('1 / Context — marketing decisions with trustworthy data',['Omnichannel retailer: ad platforms, CRM, web/mobile, commerce and email.','Ten decision tasks across CMO, Performance, CRM, Analyst and Product roles.','Scope: reproducibility, correctness, measured performance and application value.']),
        ('2 / Sources — reproducible, governed, synthetic',['Seed 360 fixture: 615 rows across 22 files; controlled duplicates, late and invalid cases.','Kafka transports events; Flink processes state. These are separate technologies.','Consent-aware published finance: gross 27,160,000; refund 4,967,000; net 22,193,000 VND.']),
        ('2 / Architecture — purpose for each component',['Flink: stateful event processing and checkpoint recovery.','MinIO + Iceberg: raw/history, governed snapshots and reproducible backfills.','ClickHouse: analytical serving. PostgreSQL: app operational data.','Dagster + dbt: batch publication gates. FastAPI + Next.js: decisions.']),
        ('3 / Correctness — evidence before timing',['Per-input oracle checks classify duplicates, invalid and late records; exact intermediate money.','Identical deterministic input/output hashes required across the two processing engines.','Published source/Gold certification and all three attribution models preserve finance.']),
        ('4 / Processing alternatives — bounded finance',['One Kafka partition; one processing lane; one-second transaction/checkpoint cadence.','Flink and Kafka Streams use native persistent stores and the same reducer.','Not full MP-03 session/funnel/watermark parity; not an event-to-dashboard SLA.','Engine / scale / inputs per second / p50 / p95 / p99 milliseconds',*[' / '.join(r) for r in streams[1:]]]),
        ('4 / Recovery and resources',recovery+['Producer active during interruption; input advances and new committed output follows.','Flink requires completed/restored checkpoint; Streams retains application ID/changelog.','JSON evidence includes CPU counter deltas, sampled working set, coverage and reset limits.']),
        ('4 / Analytical alternatives — measured p95',['Identical 1M/10M/20M Gold-shaped facts; integer answers checked independently.','20 warmups + 100 serial queries; warm buffers; ClickHouse result cache disabled.','Each baseline capped at 1.5 GiB; PostgreSQL campaign/day index.']+[' / '.join([r[0],r[2]+' ms']) for r in analytics[1:] if 'analytics-20m' in r[0]]),
        ('5 / Application — campaign operations',['Live KPI: inspect baseline and committed streaming snapshots.','Explain a historical alert; assess configured budget pacing.','Allocate VND 1,000 in a what-if scenario: projected net 35,282 minor units.','Source ROAS is a historical assumption, not a causal forecast.']),
        ('5 / Application — analysis and lifecycle',['Compare stage reach and first/last/linear attribution, with revenue reconciliation.','Inspect pseudonymous Customer 360 timeline and confidence.','Preview consent-eligible segments; inspect saved revisions and cohorts.','Identify high-value opportunity without assuming permission to activate.']),
        ('5 / Effectiveness — measured scope',[f"Automated readiness: {len(app['decisions'])}/10 decisions passed.",f"Authenticated dashboard p95: {app['dashboard_http']['p95_ms']:.2f} ms (100 samples).",'These are automated API checks; browser regressions are separate.','Representative human task times remain pending.']),
        ('6 / Reproduce and inspect',['Run the evaluation Compose profile; execute evaluate.py stream / analytics / app / package.','Portable evidence index hashes every copied result and artifact.','Production datasets and application configuration remain unchanged.','Use the evaluation runbook for commands, recovery and cleanup.']),
        ('6 / Conclusions and submission limits',['Keep Flink and ClickHouse as the primary architecture; interpret each workload separately.','Local shared-machine results do not establish production capacity or an SLA.',f"Group requirement coverage: {status['group_coverage']} (10 capabilities).",'Before submission: real group ID, representative human observations and a narrated 20–30 minute video.'])]
    body=ParagraphStyle('slide',fontName='Helvetica',fontSize=17,leading=23,textColor=colors.HexColor('#cbd5e1'))
    for index,(title,lines) in enumerate(slides,1):
        c.setFillColor(colors.HexColor('#0f172a'));c.rect(0,0,960,540,fill=1,stroke=0)
        c.setFillColor(colors.HexColor('#38bdf8'));c.setFont('Helvetica-Bold',27);c.drawString(45,478,title)
        y=425
        for line in lines:
            paragraph=Paragraph(escape(line),body);_,height=paragraph.wrap(860,y-55)
            if y-height<50: raise ValueError('slide content overflow')
            paragraph.drawOn(c,45,y-height);y-=height+15
        c.setFillColor(colors.HexColor('#94a3b8'));c.setFont('Helvetica',10);c.drawString(45,25,'MARKETPULSE 360 · MP-10 · DRAFT / human and video acceptance pending')
        c.drawRightString(915,25,f'{index:02d} / {len(slides)}');c.showPage()
    c.save()
