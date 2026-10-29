'use client';
import {useEffect,useState} from 'react';
import {exact} from '../lib/format';
type RecordValue=Record<string,unknown>;
const record=(value:unknown):RecordValue=>value&&typeof value==='object'&&!Array.isArray(value)?value as RecordValue:{};
const label=(value:unknown)=>value==null?'Unavailable':String(value).replaceAll('_',' ');
const sources:Record<string,string>={kafka:'Kafka',flink:'Flink',clickhouse:'ClickHouse',quality:'Quality collector',probe:'Probe collector',app_postgres:'App PostgreSQL',batch_postgres:'Batch PostgreSQL',minio:'MinIO',dagster:'Dagster'};
function useObservationClock(){const [now,setNow]=useState(()=>Date.now());useEffect(()=>{const timer=setInterval(()=>setNow(Date.now()),10000);return()=>clearInterval(timer);},[]);return now;}
type SourceState={key:string;name:string;state:'Unknown'|'Available'|'Unavailable / stale';observed:number|null};
/** One derivation for every view: unknown unless observed, stale after 60s (quality collector 180s). */
export function sourceStates(value:RecordValue|undefined,now:number):SourceState[]{
 const telemetry=record(value?.telemetry),samples=Array.isArray(telemetry.sources)?telemetry.sources.map(record):[];
 return Object.entries(sources).map(([key,name])=>{
 const sample=samples.find(s=>s.name==='mp_source_up'&&record(s.labels).source===key);
 const stamp=Number(samples.find(s=>s.name==='mp_source_observed_timestamp_seconds'&&record(s.labels).source===key)?.value);
 const age=now/1000-stamp,observed=Number.isFinite(stamp)&&stamp>0&&age>=-60;
 const known=telemetry.status==='available'&&observed&&sample&&['0','1'].includes(String(sample.value));
 const available=known&&String(sample.value)==='1'&&age<=(key==='quality'?180:60);
 return {key,name,state:!known?'Unknown':available?'Available':'Unavailable / stale',observed:observed?stamp:null};
 });
}
const tone=(state:string)=>state==='Available'||state==='Passed'?'good':state==='Unknown'||state==='Unavailable'?'unknown':'bad';
export function PlatformSummary({value}:{value?:RecordValue}){
 const now=useObservationClock();
 return <><p>Operations read: {label(value?.status)}. Source availability reflects the latest collector probe.</p><div className="service-grid">{sourceStates(value,now).map(({key,name,state,observed})=>
 <article key={key} aria-label={name+' status'}><h3>{name}</h3><strong className={'health '+tone(state)}>{state}</strong><small>{observed?'Observed '+new Date(observed*1000).toISOString():'No observation available'}</small></article>)}</div></>;
}
/** Current only for the matching validated release and a report at most three minutes old. */
export function qualityState(value:unknown,currentIdentity:string|undefined,now:number){
 const report=record(value),live=report.scope==='live'?report:{};
 const recorded=live.status==='passed'?'Passed':live.status==='failed'?'Failed':'Unavailable';
 const timestamp=typeof live.observed_at==='string'?Date.parse(live.observed_at):NaN,age=now-timestamp;
 const qualification=!currentIdentity?'Current release unavailable; current quality is unknown.':live.identity!==currentIdentity?'Report belongs to a different release; current quality is unknown.':!Number.isFinite(timestamp)?'Report timestamp unavailable; current quality is unknown.':age< -60000?'Report timestamp is in the future; current quality is unknown.':age>180000?'Report is stale; current quality is unknown.':'';
 return {live,recorded,qualification,state:recorded==='Unavailable'?'Unavailable':qualification?'Unknown':recorded};
}
export function QualitySummary({value,probe,currentIdentity}:{value?:unknown;probe?:unknown;currentIdentity?:string}){
 const now=useObservationClock(),{live,recorded,qualification,state}=qualityState(value,currentIdentity,now);
 return <><section aria-label="Latest live quality" className="quality-summary"><h3>Latest live quality</h3><strong className={'health '+(state==='Passed'?'good':state==='Failed'?'bad':'unknown')}>{state}</strong>{recorded!=='Unavailable'&&<p>Recorded result: {recorded}</p>}<p>{qualification||label(live.reason)}</p><p>{live.observed_at?'Checked '+String(live.observed_at):'No live quality report available'}</p>{live.identity!=null&&<p className="source">Checked release {String(live.identity)}</p>}{Array.isArray(live.relations)&&<p>{live.relations.length} certified relations checked</p>}<small>Current quality requires the active release and a report within three minutes.</small></section><p className={probe===true?'notice':'source'}>{probe===true?'Controlled probe alert active':probe===false?'No controlled probe alert active':'Controlled probe state unknown'}</p><p className="source">Controlled probe results are separate from live release quality.</p></>;
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

export function HealthStrip({status,currentIdentity,certified}:{status?:RecordValue;currentIdentity?:string;certified?:{generation:string;identity:string;data_through?:string|null}|null}){
 const now=useObservationClock(),states=sourceStates(status,now),quality=qualityState(status?.quality,currentIdentity,now);
 const count=(state:string)=>states.filter(s=>s.state===state).length,failing=states.filter(s=>s.state==='Unavailable / stale').map(s=>s.name);
 const kafka=states.find(s=>s.key==='kafka')?.state||'Unknown';
 const matched=certified&&currentIdentity&&certified.identity===currentIdentity;
 const coverage=!certified?'Unknown':!matched?'Unknown':certified.data_through?'Published':'Empty';
 return <section className="status-grid health-strip" aria-label="Data health summary">
  <article className="panel" aria-label="Certified data"><span>Certified data</span><strong className={'health '+tone(coverage==='Published'?'Available':'Unknown')}>{coverage}</strong><small>{certified===undefined?'Loading certified release':!certified?'Certified release unavailable':!matched?'Served release does not match validated lineage; trust is unknown':`Through ${certified.data_through?.slice(0,10)||'no data'} · Generation ${certified.generation}`}</small></article>
  <article className="panel" aria-label="Live quality"><span>Live quality</span><strong className={'health '+tone(quality.state)}>{quality.state}</strong><small>{quality.qualification||(quality.live.observed_at?'Checked '+String(quality.live.observed_at):'No live report')}</small></article>
  <article className="panel" aria-label="Platform sources"><span>Platform sources</span><strong className={'health '+(failing.length?'bad':count('Unknown')?'unknown':'good')}>{failing.length?failing.length+' need attention':count('Unknown')?'Partly unknown':'All available'}</strong><small>{count('Available')} available · {failing.length} unavailable · {count('Unknown')} unknown</small>{failing.length>0&&<small>Needs attention: {failing.join(', ')}</small>}</article>
  <article className="panel" aria-label="Input & replay"><span>Input &amp; replay</span><strong className={'health '+tone(kafka)}>Kafka {kafka.toLowerCase()}</strong><small>Replay controller status unavailable in this deployment</small></article>
 </section>;
}
