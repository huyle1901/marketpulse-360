'use client';

import {useEffect,useRef,useState} from 'react';
import {exact,formatMoney} from '../../lib/format';
import {useUrlState} from '../../lib/useUrlState';
import {liveWindow} from '../../lib/live';
import {LiveView} from '../../components/LiveView';
import {BarList,ExactValues} from '../../components/charts';
import type {Dashboard} from '../types';

type LiveRow=Dashboard['streaming']['rows'][number];
const signature=(row:LiveRow)=>[row.processed_at,row.received_at,row.sessions,row.order_count,row.net_revenue_minor].join('|');
const latest=(values:string[])=>values.reduce<string|null>((a,b)=>a&&a>b?a:b,null);

export default function LiveActivity(){
 const [filters,setFilters,replaceFilters]=useUrlState({from_date:'',to_date:''});
 const [report,setReport]=useState<Dashboard|null>(null);
 const [error,setError]=useState('');
 const [lastPoll,setLastPoll]=useState('');
 const [changed,setChanged]=useState<Map<string,string>>(new Map());
 const seen=useRef(new Map<string,string>());
 const query=new URLSearchParams(Object.entries(filters).filter(([,value])=>!!value)).toString();
 useEffect(()=>{
  let active=true,inFlight=false;const controller=new AbortController();
  const load=async()=>{
   if(inFlight)return;inFlight=true;
   try{
    const response=await fetch('/api/v1/dashboard?'+query,{cache:'no-store',signal:controller.signal});
    if(!response.ok){const body=await response.json().catch(()=>({}));throw new Error(body.message||body.code||'Live snapshots unavailable');}
    const data:Dashboard=await response.json();if(!active)return;
    if(!filters.from_date||!filters.to_date){replaceFilters(liveWindow(data.certified.provenance.data_through,Date.now()));return;}
    const changes:[string,string][]=[];
    for(const row of data.streaming.rows){const previous=seen.current.get(row.metric_key),value=signature(row);if(previous!==undefined&&previous!==value)changes.push([row.metric_key,row.received_at]);seen.current.set(row.metric_key,value);}
    setReport(data);if(changes.length)setChanged(current=>new Map([...current,...changes]));setError('');setLastPoll(new Date().toISOString());
   }catch(e){if(active&&!controller.signal.aborted)setError(e instanceof Error?e.message:'Live snapshots unavailable');}
   finally{inFlight=false;}
  };
  void load();const timer=setInterval(()=>void load(),5000);
  return()=>{active=false;controller.abort();clearInterval(timer);};
 },[query]);
 const streaming=report?.streaming,rows=[...(streaming?.rows||[])].sort((a,b)=>b.max_event_time.localeCompare(a.max_event_time)||a.campaign_id.localeCompare(b.campaign_id));
 const name=(id:string)=>report?.campaigns.find(c=>c.campaign_id===id)?.name||id;
 const groups=new Map<string,{sessions:bigint;orders:bigint;net:bigint;keys:number}>();
 for(const row of rows){const key=row.currency||'';const total=groups.get(key)||{sessions:0n,orders:0n,net:0n,keys:0};total.sessions+=BigInt(row.sessions);total.orders+=BigInt(row.order_count);total.net+=BigInt(row.net_revenue_minor);total.keys++;groups.set(key,total);}
 // One bar per campaign and currency (a day without orders has no currency and takes its campaign's): the window's latest campaign-day snapshots summed; negatives are text, never drawn.
 const byCampaign=new Map<string,{campaign:string;currency:string;net:bigint;updated:boolean}>();
 for(const row of rows){const currency=row.currency||report?.campaigns.find(c=>c.campaign_id===row.campaign_id)?.currency||'';const key=row.campaign_id+'|'+currency;const item=byCampaign.get(key)||{campaign:row.campaign_id,currency,net:0n,updated:false};item.net+=BigInt(row.net_revenue_minor);item.updated||=changed.has(row.metric_key);byCampaign.set(key,item);}
 const campaignBars=[...byCampaign.entries()].sort(([ka,a],[kb,b])=>a.net===b.net?ka.localeCompare(kb):a.net>b.net?-1:1);
 const currencies=new Set(campaignBars.map(([,c])=>c.currency));
 return <main>
  <header className="masthead"><div><h1>Live View</h1><p>Who is on the store right now and what they are buying · Provisional, not certified finance</p></div><div className="status"><span className="dot"/> Checks every 5 seconds<br/><small>Last successful check: {lastPoll||'Waiting'} · a successful check is not new input</small></div></header>
  <LiveView/>
  <h2 className="mt-8">By campaign (provisional)</h2>
  <section className="filters" aria-label="Live window">
   <label>From (UTC)<input type="date" value={filters.from_date} onChange={e=>setFilters({...filters,from_date:e.target.value})}/></label>
   <label>To (exclusive UTC)<input type="date" value={filters.to_date} onChange={e=>setFilters({...filters,to_date:e.target.value})}/></label>
  </section>
  {error&&<div className="error" role="alert"><strong>{report?'Check failed; showing last successful snapshots':'Live snapshots unavailable'}</strong><p>{error}</p></div>}
  {!report&&!error&&<p className="empty">Loading committed snapshots…</p>}
  {report&&streaming&&<>
   <section className="status-grid" aria-label="Pipeline state">
    <article className="panel"><span>Processing projection</span><strong>{streaming.status}</strong><small>Flink → Kafka → serving worker</small></article>
    <article className="panel"><span>Latest business event</span><strong>{latest(rows.map(r=>r.max_event_time))||'None'}</strong><small>Event time in the source data</small></article>
    <article className="panel"><span>Last new input received</span><strong>{latest(rows.map(r=>r.received_at))||'None'}</strong><small>Actual UTC arrival in the projection</small></article>
    <article className="panel"><span>Replay</span><strong>Unknown</strong><small>Replay controller status unavailable in this deployment</small></article>
   </section>
   <section className="panel" aria-label="Provisional live summary"><div className="section-heading"><div><h2>Provisional totals</h2><p>Sum of latest keyed snapshots in the window · Not certified finance</p></div><span className="badge neutral">{rows.length} campaign-days</span></div>
    {!rows.length?<p className="empty">No committed streaming snapshot is available for this window.</p>:<div className="kpis">{[...groups.entries()].map(([currency,total])=><div className="kpi-group" key={currency}>
     <article><span>Sessions</span><strong>{exact(total.sessions.toString())}</strong><small>campaign-day session incidences, not unique visitors</small></article>
     <article><span>Orders</span><strong>{exact(total.orders.toString())}</strong><small>{currency||'No currency'} snapshots</small></article>
     <article><span>Provisional net{currency?' · '+currency:''}</span><strong>{currency?formatMoney(total.net.toString(),currency):exact(total.net.toString())}</strong><small>Before consent/bot governance · Not certified finance</small></article>
    </div>)}</div>}
   </section>
   {rows.length>0&&<section className="panel"><h2>Provisional net by campaign</h2><p className="source">Latest committed campaign-day snapshots in the window, summed per campaign · Negative net is shown as text, not drawn.</p>
    <BarList name="Provisional net by campaign" color="var(--live)" rows={campaignBars.map(([key,c])=>({key,value:c.net>0n?Number(c.net):0,
     display:c.currency?formatMoney(c.net.toString(),c.currency):exact(c.net.toString()),
     label:<>{name(c.campaign)}{currencies.size>1&&c.currency&&<small className="text-muted-foreground"> · {c.currency}</small>}{c.updated&&<span className="badge change">Updated</span>}</>}))}/>
    <ExactValues label="Exact campaign-day snapshots"><p className="source">Each row is the latest committed snapshot for one campaign and UTC day; replacing it is not a raw event.</p>
    <div className="table-scroll"><table aria-label="Latest campaign-day snapshots"><thead><tr><th>Campaign</th><th>UTC day</th><th>Sessions</th><th>Orders</th><th>Provisional net</th><th>Event / processed / received</th></tr></thead><tbody>{rows.map(row=><tr key={row.metric_key}>
     <td><strong>{name(row.campaign_id)}</strong>{changed.has(row.metric_key)&&<span className="badge change" title={'Snapshot replaced; received '+changed.get(row.metric_key)}>Updated</span>}<small>{row.mapped?'Mapped':'Unmapped'} · {row.stale?'Historical / stale':'Recent activity'}</small></td>
     <td>{row.metric_key.split('|')[1]||row.max_event_time.slice(0,10)}</td><td>{exact(row.sessions)}</td><td>{exact(row.order_count)}</td>
     <td>{row.currency?formatMoney(row.net_revenue_minor,row.currency):exact(row.net_revenue_minor)}</td>
     <td><small>{row.max_event_time}<br/>{row.processed_at}<br/>{row.received_at}</small></td></tr>)}</tbody></table></div>
    </ExactValues>
   </section>}
   <details className="provenance"><summary>Streaming source provenance</summary><p>{streaming.source}</p><p>{streaming.source_identity||'Unavailable'}</p><p>Latest selected offset bounds (gaps may exist): {JSON.stringify(streaming.offset_bounds||[])}</p><p>Window {report.filters.from_date} → {report.filters.to_date} (exclusive UTC)</p></details>
  </>}
 </main>;
}
