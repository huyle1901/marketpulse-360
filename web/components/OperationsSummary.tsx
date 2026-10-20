import {exact} from '../lib/format';
type RecordValue=Record<string,unknown>;
const record=(value:unknown):RecordValue=>value&&typeof value==='object'&&!Array.isArray(value)?value as RecordValue:{};
const label=(value:unknown)=>value==null?'Unavailable':String(value).replaceAll('_',' ');
const sources:Record<string,string>={kafka:'Kafka',flink:'Flink',clickhouse:'ClickHouse',quality:'Quality collector',probe:'Probe collector',app_postgres:'App PostgreSQL',batch_postgres:'Batch PostgreSQL',minio:'MinIO',dagster:'Dagster'};
export function PlatformSummary({value}:{value?:RecordValue}){
 const telemetry=record(value?.telemetry),samples=Array.isArray(telemetry.sources)?telemetry.sources.map(record):[];
 return <><p>Operations read: {label(value?.status)}. Source availability reflects the latest collector probe.</p><div className="service-grid">{Object.entries(sources).map(([key,name])=>{
 const sample=samples.find(s=>s.name==='mp_source_up'&&record(s.labels).source===key);
 const stamp=Number(samples.find(s=>s.name==='mp_source_observed_timestamp_seconds'&&record(s.labels).source===key)?.value);
 const age=Date.now()/1000-stamp,observed=Number.isFinite(stamp)&&stamp>0&&age>=-60;
 const known=telemetry.status==='available'&&observed&&sample&&['0','1'].includes(String(sample.value));
 const available=known&&String(sample.value)==='1'&&age<=(key==='quality'?180:60);
 const state=!known?'Unknown':available?'Available':'Unavailable / stale';
 return <article key={key} aria-label={name+' status'}><h3>{name}</h3><strong className={'health '+(!known?'unknown':available?'good':'bad')}>{state}</strong><small>{observed?'Observed '+new Date(stamp*1000).toISOString():'No observation available'}</small></article>;
 })}</div></>;
}
export function QualitySummary({value,probe}:{value?:unknown;probe?:unknown}){
 const report=record(value),live=report.scope==='live'?report:{};
 const state=live.status==='passed'?'Passed':live.status==='failed'?'Failed':'Unavailable';
 return <><section aria-label="Latest live quality" className="quality-summary"><h3>Latest live quality</h3><strong className={'health '+(state==='Passed'?'good':state==='Failed'?'bad':'unknown')}>{state}</strong><p>{label(live.reason)}</p><p>{live.observed_at?'Checked '+String(live.observed_at):'No live quality report available'}</p>{live.identity!=null&&<p className="source">Checked release {String(live.identity)}</p>}{Array.isArray(live.relations)&&<p>{live.relations.length} certified relations checked</p>}</section><p className={probe===true?'notice':'source'}>{probe===true?'Controlled probe alert active':probe===false?'No controlled probe alert active':'Controlled probe state unknown'}</p><p className="source">Controlled probe results are separate from live release quality.</p></>;
}
export function LineageSummary({value}:{value?:RecordValue}){
 if(!value?.identity)return <p className="empty">Validated lineage unavailable</p>;
 const projection=record(value.projection);
 return <><p className="source">Certified release {String(value.identity)}</p><div className="summary-grid"><article><strong>{Object.keys(record(value.source_snapshots)).length}</strong><span>source snapshots</span></article><article><strong>{Object.keys(record(value.output_snapshots)).length}</strong><span>output snapshots</span></article><article><strong>{projection.row_count==null?'Unavailable':exact(String(projection.row_count))} governed projection events</strong><span>Silver snapshot {label(projection.snapshot_id)}</span></article></div><p>{value.proof_digest?'Certification proof is recorded.':'Certification proof unavailable.'} Snapshot IDs and checksums are in technical details.</p></>;
}
export function RetentionSummary({value}:{value?:RecordValue}){
 if(!value||value.expired_sessions==null)return <p className="empty">Session retention unavailable</p>;
 return <><div className="summary-grid"><article><strong>{exact(String(value.expired_sessions))} expired sessions</strong><span>Up to {label(value.cleanup_limit)} removable per action</span></article><article><strong>{label(value.session_ttl_hours)} hours</strong><span>Session lifetime</span></article><article><strong>{label(value.protected_releases)} certified releases</strong><span>{label(value.protected_sidecars)} protected projections</span></article></div><p>{value.analytical_deletion_enabled===false?'Analytical deletion disabled; certified data and evidence are protected.':'Analytical retention policy unavailable.'}</p></>;
}
