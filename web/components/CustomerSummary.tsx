'use client';
import {useEffect,useState} from 'react';
import {exact} from '../lib/format';
import {BarList,Donut} from './charts';
import {api} from '../app/customers/helpers';
import type {Profiles} from '../app/customers/types';

const types=[['customer','Customer profiles','var(--cat-1)'],['anonymous','Anonymous device profiles','var(--cat-2)'],['session','Session-only profiles','var(--cat-3)']] as const;
const opportunities=[['high_value','High value','orders ≥1 and net ≥5,000,000 minor units'],['churn_risk','Churn risk','orders ≥1 and recency ≥30 days'],['cart_opportunity','Cart opportunity','cart events ≥1 and no orders']] as const;
type Count={total:string|null;identity:string};

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
 const state=error?<p className="empty">Summary unavailable: {error}</p>:!values?<p className="empty">Loading exact counts…</p>:mismatch?<p className="empty">Counts are from a different certified release; they will update with the next refresh.</p>:null;
 const opportunityRows=values?opportunities.map(([key,label,rule])=>({key,label,rule,total:values[key]?.total??null}))
  .sort((a,b)=>a.total==null?1:b.total==null?-1:Number(BigInt(b.total)-BigInt(a.total))):[];
 return <div className="insight-grid">
  <section className="panel stage-panel" aria-label="Profile mix"><div className="section-heading"><div><h2>Profile mix</h2><p>Historical governed profiles by identity evidence · {exact(profilesTotal)} total</p></div></div>
   {state||<Donut name="Profile mix" total={exact(profilesTotal)} caption="profiles" size={148}
    slices={types.map(([key,label,color])=>{const v=values![key].total;return {key,label,color,value:v==null?0:Number(v),display:v==null?'Unavailable':`${exact(v)} of ${exact(profilesTotal)}`};})}/>}
  </section>
  <section className="panel stage-panel" aria-label="Opportunity heuristics"><div className="section-heading"><div><h2>Opportunity heuristics</h2><p>Transparent rule-based, not predictions · Historical analytics population · select a count to list those profiles</p></div></div>
   {state||<BarList name="Opportunity heuristics ranked by profiles" color="var(--cat-4)" rows={opportunityRows.map(({key,label,rule,total:v})=>({key,label,sub:rule,value:v==null?0:Number(v),
    display:v==null?<span className="text-muted-foreground">Unavailable</span>:<button className="link-button" aria-label={'Show '+label.toLowerCase()+' profiles'} onClick={()=>onOpportunity(key)}>{exact(v)}</button>}))}/>}
  </section>
 </div>;
}
