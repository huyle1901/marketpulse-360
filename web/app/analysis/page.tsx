'use client';
import {useUrlState,choice} from '../../lib/useUrlState';
import FilterActions from '../../components/FilterActions';
import {useSession} from '../../components/SessionGate';
import {useEffect,useState} from 'react';
import {stages,type Model,type Reports,type Provenance,type Credit} from './types';
const names:Record<Model,string>={first_touch:'First touch',last_touch:'Last touch',linear:'Linear'};
const stageNames=['Sessions','Page view','View item','Add to cart','Checkout','Purchase'];
const exact=(value:string)=>BigInt(value).toLocaleString('en-US');
async function read<T>(path:string,signal:AbortSignal):Promise<T>{
 const response=await fetch('/api/v1/analysis/'+path,{cache:'no-store',signal});
 const body=await response.json();if(!response.ok)throw new Error(body.message||body.detail||body.code||'Analysis unavailable');return body;
}
function Source({value,label}:{value:Provenance;label:string}){return <details className="provenance"><summary>{label} source &amp; freshness</summary><p>{value.model} · Serving relation {value.serving_relation} · Generation {value.generation}</p><p>Data through {value.data_through||'No selected data'} · {value.timestamp_resolution} resolution · Maximum event day {value.max_event_date||'None'}</p><p>Read at {value.observed_at}; polling does not advance event freshness.</p><p>Release {value.identity}</p><p>Certification proof {value.proof_digest}</p>{value.silver_snapshot_id&&<p>Silver snapshot {value.silver_snapshot_id} · {value.projection_row_count} governed events · {value.projection_version}<br/>Projection checksum {value.projection_checksum}</p>}</details>}
export default function Analysis(){
 const {canOperate}=useSession();
 const [filters,setFilters]=useUrlState({from_date:'',to_date:'',platform:'',campaign_id:'',objective:'',market:'',currency:'',model:'last_touch'},v=>({...v,platform:choice(v.platform,['google','meta','tiktok']),objective:choice(v.objective,['awareness','traffic','conversions']),model:choice(v.model,['last_touch','first_touch','linear'],'last_touch')}));
 const model=filters.model as Model;const setModel=(model:Model)=>setFilters(current=>({...current,model}));const [reports,setReports]=useState<(Omit<Reports,'journeys'>&{journeys:Reports['journeys']|null})|null>(null);
 const [displayed,setDisplayed]=useState('');const [error,setError]=useState('');const [retry,setRetry]=useState(0);const [success,setSuccess]=useState('');
 const query=new URLSearchParams(Object.entries(filters).filter(([key,v])=>!!v&&key!=='model')).toString();const context=query+'|'+model;
 useEffect(()=>{
  let active=true,inFlight=false;const controller=new AbortController();
  const load=async()=>{
   if(inFlight)return;inFlight=true;
   try{
    const [funnel,attribution,journeys]=await Promise.all([read<Reports['funnel']>('funnel?'+query,controller.signal),read<Reports['attribution']>('attribution?'+query,controller.signal),canOperate?read<Reports['journeys']>('journeys?'+query+'&model='+model,controller.signal):Promise.resolve(null)]);
    if(!active)return;
    if(new Set([funnel.provenance.identity,attribution.provenance.identity,...(journeys?[journeys.provenance.identity]:[])]).size!==1)throw new Error('Release changed between reads; refresh analysis again.');
    setReports({funnel,attribution,journeys});setDisplayed(context);setError('');setSuccess(new Date().toLocaleTimeString());
    setFilters(current=>current.from_date&&current.to_date?current:{...current,from_date:funnel.filters.from_date,to_date:funnel.filters.to_date});
   }catch(e){if(active&&!controller.signal.aborted)setError(e instanceof Error?e.message:'Analysis unavailable');}finally{inFlight=false;}
  };void load();const timer=setInterval(()=>void load(),5000);return()=>{active=false;controller.abort();clearInterval(timer);};
 },[query,model,context,retry,canOperate]);
 const update=(key:keyof typeof filters,value:string)=>setFilters(current=>({...current,[key]:value}));
 const options=reports?.funnel.campaign_options||[];const a=reports?.attribution;const f=reports?.funnel;const j=reports?.journeys;
 const comparison=(rows:Credit[],kind:'channel'|'campaign')=>{
  const keys=[...new Set(rows.map(row=>kind==='channel'?row.channel:row.campaign_id))];
  return <div className="table-scroll"><table><caption>{kind==='channel'?'Channel':'Campaign'} credited net comparison · {a?.currency} minor units</caption><thead><tr><th>{kind}</th>{Object.entries(names).map(([key,name])=><th key={key}>{name} net</th>)}</tr></thead><tbody>{keys.map(key=>{
   const first=rows.find(row=>(kind==='channel'?row.channel:row.campaign_id)===key)!;return <tr key={key}><td>{kind==='campaign'?<button className="drill" aria-label={'Explore campaign '+first.campaign_name} onClick={()=>update('campaign_id',key||'')}>{first.campaign_name}</button>:<strong>{key}</strong>}</td>{Object.keys(names).map(m=>{const row=rows.find(row=>row.model===m&&(kind==='channel'?row.channel:row.campaign_id)===key);return <td key={m}>{row?exact(row.net_revenue_minor):'0'}<small>{row?.allocated_order_count||'0.000000'} allocated orders · Net share {row?.net_contribution_ratio??'Unavailable'}</small></td>})}</tr>;
  })}</tbody></table></div>;
 };
 return <main>

 <header className="masthead"><div><div className="brand">MARKETPULSE <span>360</span></div><h1>Funnel &amp; Attribution</h1><p>Trace stage reach, compare credit, inspect representative journeys</p></div><div className="status"><span className="dot"/>Certified analytical release<br/>5-second refresh · Last success {success||'Pending'}<br/><button className="secondary" onClick={()=>setRetry(v=>v+1)}>Refresh analysis</button></div></header>
 <nav className="section-links" aria-label="Page sections"><a href="#funnel">Funnel</a><a href="#attribution">Attribution</a><a href="#journeys">Journeys</a></nav>
 <section className="filters analysis-filters" aria-label="Analysis filters">
 <label>From (UTC)<input type="date" value={filters.from_date} onChange={e=>update('from_date',e.target.value)}/></label><label>To (exclusive UTC)<input type="date" value={filters.to_date} onChange={e=>update('to_date',e.target.value)}/></label>
 <label>Paid channel<select value={filters.platform} onChange={e=>setFilters(current=>({...current,platform:e.target.value,campaign_id:''}))}><option value="">All channels</option>{['google','meta','tiktok'].map(c=><option key={c}>{c}</option>)}</select></label>
 <label>Campaign<select aria-label="Campaign" value={filters.campaign_id} onChange={e=>update('campaign_id',e.target.value)}><option value="">All campaigns</option>{options.filter(c=>!filters.platform||c.platform===filters.platform).map(c=><option value={c.campaign_id} key={c.campaign_id}>{c.name}</option>)}<option value="__unattributed__">Unattributed</option></select></label>
 <label>Objective<select value={filters.objective} onChange={e=>update('objective',e.target.value)}><option value="">All objectives</option>{['awareness','traffic','conversions'].map(v=><option key={v}>{v}</option>)}</select></label>
 <label>Configured market<input maxLength={2} placeholder="All / e.g. VN" value={filters.market} onChange={e=>update('market',e.target.value.toUpperCase())}/></label>
 <label>Currency<select value={filters.currency} onChange={e=>update('currency',e.target.value)}><option value="">All / declared currency</option>{[...new Set(options.map(c=>c.currency))].map(v=><option key={v}>{v}</option>)}</select></label>
 <label>Journey attribution model<select value={model} onChange={e=>setModel(e.target.value as Model)}>{Object.entries(names).map(([v,label])=><option key={v} value={v}>{label}</option>)}</select></label>
 </section>
 <FilterActions through={reports?.funnel.provenance.data_through} onDates={(from_date,to_date)=>setFilters(current=>({...current,from_date,to_date}))} onReset={()=>setFilters({from_date:'',to_date:'',platform:'',campaign_id:'',objective:'',market:'',currency:'',model:'last_touch'})}/>
 <div className="breadcrumbs"><button className="secondary" onClick={()=>setFilters(current=>({...current,platform:'',campaign_id:'',objective:'',market:'',currency:''}))}>All channels</button><span>{filters.platform||'All paid channels'} → {options.find(c=>c.campaign_id===filters.campaign_id)?.name||filters.campaign_id||'All campaigns'}</span></div>
 {reports&&displayed!==context&&<p className="notice" role="status">{error?'Selection unavailable; showing previous analysis':'Loading selection; showing previous analysis'} · {reports.funnel.filters.from_date} → {reports.funnel.filters.to_date}</p>}
 {error&&<div className="error" role="alert"><strong>{reports?'Refresh failed; showing last successful analysis':'Analysis unavailable'}</strong><p>{error}</p></div>}
 {!reports&&!error&&<p className="empty">Loading certified analysis…</p>}
 {f&&a&&<>
 <section id="funnel" className="panel"><div className="section-heading"><div><h2>Funnel stage reach</h2><p>{f.definition}</p></div><span className="badge">{f.provenance.historical?'Historical data':'Certified daily data'}</span></div><strong className="unit">{f.unit}</strong>
 {f.campaigns.length===0?<p className="empty">No funnel data matches this selection.</p>:<div className="stage-chart">{stages.map((stage,index)=>{const reach=stage==='sessions'?(f.totals.sessions==='0'?null:'1.000000'):f.reach_ratios[stage];return <div className="stage-row" key={stage}><span>{stageNames[index]}</span><div className="stage-track" aria-hidden="true"><div style={{width:reach?Math.min(100,Math.max(0,Number(reach)*100))+'%':'0%'}}/></div><strong>{exact(f.totals[stage])}</strong><small>{reach===null||reach===undefined?'Reach unavailable':reach+' of sessions'}</small></div>})}</div>}
 <div className="table-scroll"><table><caption>Stage reach by channel · summed campaign-day buckets</caption><thead><tr><th>Channel</th>{stageNames.map(s=><th key={s}>{s}</th>)}</tr></thead><tbody>{f.channels.map(row=><tr key={row.channel}><td>{['google','meta','tiktok'].includes(row.channel)?<button className="drill" aria-label={'Explore channel '+row.channel} onClick={()=>setFilters(current=>({...current,platform:row.channel,campaign_id:''}))}>{row.channel}</button>:row.channel}</td>{stages.map(s=><td key={s}>{exact(row[s])}</td>)}</tr>)}</tbody></table></div>
 <details className="campaign-reach"><summary>Campaign stage reach and drill-down</summary><div className="table-scroll"><table><thead><tr><th>Campaign</th>{stageNames.map(s=><th key={s}>{s}</th>)}</tr></thead><tbody>{f.campaigns.map(row=><tr key={row.campaign_id}><td>{row.campaign_name}</td>{stages.map(s=><td key={s}>{exact(row[s])}</td>)}</tr>)}</tbody></table></div></details><Source value={f.provenance} label="Funnel"/></section>
 <section id="attribution" className="panel"><div className="section-heading"><div><h2>Attribution comparison</h2><p>Selected campaign credit · {a.currency} minor units · Refunds recognized on refund date using original purchase touches.</p></div><span className="badge neutral">Inclusive 30-day paid-touch window</span></div>
 <div className="model-grid">{a.models.map(row=><article className="model-card" key={row.model}><h3>{names[row.model]}</h3><p>{row.definition}</p><dl><dt>Gross credit</dt><dd>{exact(row.gross_revenue_minor)}</dd><dt>Refund credit</dt><dd>{exact(row.refund_amount_minor)}</dd><dt>Net credit</dt><dd>{exact(row.net_revenue_minor)}</dd><dt>Allocated orders</dt><dd>{row.allocated_order_count}</dd></dl></article>)}</div>
 {a.campaigns.length===0?<p className="empty">No attributed credit matches this selection.</p>:<>{comparison(a.channels,'channel')}{comparison(a.campaigns,'campaign')}</>}
 <div className="reconciliation"><h3>Global financial reconciliation</h3><p>{a.global_reconciliation.scope}</p><p>Filtered credited amounts can differ from source campaign revenue.</p><p>Source gross {exact(a.global_reconciliation.source_totals.gross_revenue_minor)} − refunds {exact(a.global_reconciliation.source_totals.refund_amount_minor)} = net {exact(a.global_reconciliation.source_totals.net_revenue_minor)} {a.currency}</p>{a.global_reconciliation.model_totals.map(row=><p key={row.model}>{names[row.model]}: {exact(row.gross_revenue_minor)} − {exact(row.refund_amount_minor)} = {exact(row.net_revenue_minor)}</p>)}<strong>{a.global_reconciliation.balanced?'All three models reconcile':'Reconciliation unavailable'}</strong></div><Source value={a.provenance} label="Attribution"/></section>
 {j&&canOperate&&<section id="journeys" className="panel"><div className="section-heading"><div><h2>Representative order journeys</h2><p>Bounded descriptive examples · {j.rows.length} shown · No population or activation claim</p></div><span className="badge neutral">Sampling model: {names[j.model]}</span></div>
 <p>{j.candidates_scanned||'0'} candidates scanned · {j.context_unavailable||'0'} contexts unavailable · Scan {j.candidate_scan_capped?'capped at 100':'up to 100'} · Sample limit {j.sample_limit||'5'}</p><p className="source">{j.context_limit||'500 canonical touches / 100 financial events per order'} · Context may precede the report interval.</p>
 {j.status==='unavailable'?<p className="empty">{j.message}</p>:!j.rows.length?<p className="empty">No representative journey found within the scan bounds for this selection.</p>:<div className="journey-grid">{j.rows.map(row=><article className="journey-card" key={row.journey_id}><h3>Journey {row.journey_id.slice(0,12)}</h3><p>Original purchase {row.purchase_time}</p><p className="source">Paid-touch context {row.context_from} → {row.context_to}</p>
 <ol className="timeline">{row.touches.map(t=><li key={t.touch_ref}><strong>{t.campaign_name}</strong> · {t.channel}<small>{t.event_time}</small>{t.outside_report_interval&&<span className="context-tag">Outside report interval</span>}</li>)}{!row.touches.length&&<li>No eligible paid touch; credit is unattributed.</li>}{row.financial_events.map(e=><li key={e.event_ref}><strong>{e.event_name==='order_created'?'Purchase recognized':'Refund recognized'}</strong><small>{e.event_time}</small>{e.event_name==='order_created'?'Gross '+exact(e.gross_revenue_minor):'Refund '+exact(e.refund_amount_minor)} {j.currency}</li>)}</ol>
 <div className="table-scroll"><table><caption>All models · Credit recognized inside report interval</caption><thead><tr><th>Model / campaign / date</th><th>Gross</th><th>Refund</th><th>Net</th><th>Orders</th></tr></thead><tbody>{row.allocations.map((credit,index)=><tr key={index}><td>{names[credit.model]}<small>{credit.campaign_name} · {credit.event_date}</small></td><td>{exact(credit.allocated_gross_minor)}</td><td>{exact(credit.allocated_refund_minor)}</td><td>{exact(credit.allocated_net_minor)}</td><td>{credit.allocated_order_count}</td></tr>)}</tbody></table></div></article>)}</div>}<Source value={j.provenance} label="Journey"/>
 </section>}{!canOperate&&<p>Journey access requires operator role</p>}</>}
 <footer>MarketPulse 360 · MP-07 · Synthetic marketing data · Read-only analysis</footer>
 </main>;
}
