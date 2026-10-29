'use client';
import {useEffect,useState} from 'react';
import {exact} from '../lib/format';
import {api} from '../app/customers/helpers';
import type {Profiles} from '../app/customers/types';

const types=[['customer','Customer profiles'],['anonymous','Anonymous device profiles'],['session','Session-only profiles']] as const;
const opportunities=[['high_value','High value','orders ≥1 and net ≥5,000,000 minor units'],['churn_risk','Churn risk','orders ≥1 and recency ≥30 days'],['cart_opportunity','Cart opportunity','cart events ≥1 and no orders']] as const;
type Count={total:string|null;identity:string};
const percent=(value:bigint,total:bigint)=>total>0n?Number(value*1000n/total)/10:0;

/** Exact population counts from the filtered profile totals of the displayed release (one-row reads, no sampling). */
export default function CustomerSummary({identity,profilesTotal,onOpportunity}:{identity:string;profilesTotal:string;onOpportunity:(value:string)=>void}){
 const [counts,setCounts]=useState<{identity:string;values:Record<string,Count>}|null>(null);const [error,setError]=useState('');
 useEffect(()=>{
  const controller=new AbortController();setError('');
  const keys=[...types.map(([key])=>['profile_type',key]),...opportunities.map(([key])=>['opportunity',key])];
  void Promise.all(keys.map(([field,key])=>api<Profiles>(`profiles?purpose=analytics&limit=1&${field}=${key}`,controller.signal).then(r=>[key,{total:r.status==='unavailable'?null:r.total,identity:r.provenance.identity}] as const)))
   .then(rows=>setCounts({identity,values:Object.fromEntries(rows)})).catch(e=>{if(!controller.signal.aborted)setError(e instanceof Error?e.message:'Summary unavailable');});
  return()=>controller.abort();
 },[identity]);
 const values=counts?.identity===identity?counts.values:null;
 const mismatch=values&&Object.values(values).some(v=>v.identity!==identity);
 const total=BigInt(profilesTotal);
 const state=error?<p className="empty">Summary unavailable: {error}</p>:!values?<p className="empty">Loading exact counts…</p>:mismatch?<p className="empty">Counts are from a different certified release; they will update with the next refresh.</p>:null;
 const opportunityMax=values?opportunities.reduce((m,[key])=>{const v=values[key]?.total;return v!=null&&BigInt(v)>m?BigInt(v):m;},1n):1n;
 return <div className="insight-grid">
  <section className="panel stage-panel" aria-label="Profile mix"><div className="section-heading"><div><h2>Profile mix</h2><p>Historical governed profiles by identity evidence · {exact(profilesTotal)} total</p></div></div>
   {state||types.map(([key,label])=>{const v=values![key].total;return v==null?<div className="channel-row" key={key}><span>{label}</span><span/><strong>Unavailable</strong></div>:
    <div className="channel-row" key={key}><span>{label}</span><div className="channel-track" role="img" aria-label={`${label}: ${exact(v)} of ${exact(profilesTotal)}`}><span className="stage" style={{width:percent(BigInt(v),total)+'%'}}/></div><strong>{exact(v)}<small> · {percent(BigInt(v),total)}%</small></strong></div>;})}
  </section>
  <section className="panel stage-panel" aria-label="Opportunity heuristics"><div className="section-heading"><div><h2>Opportunity heuristics</h2><p>Transparent rule-based, not predictions · Historical analytics population</p></div></div>
   {state||opportunities.map(([key,label,rule])=>{const v=values![key].total;return <div className="channel-row opportunity-row" key={key}><span>{label}<small>{rule}</small></span>{v==null?<span className="source">Event detail unavailable</span>:<div className="channel-track" aria-hidden="true"><span className="stage" style={{width:percent(BigInt(v),opportunityMax)+'%'}}/></div>}<strong>{v==null?'Unavailable':<button className="link-button" aria-label={'Show '+label.toLowerCase()+' profiles'} onClick={()=>onOpportunity(key)}>{exact(v)}</button>}</strong></div>;})}
  </section>
 </div>;
}
