'use client';

import {useEffect,useRef,useState} from 'react';
import type {Alert,Campaign,Dashboard,Pace,Rule,Scenario,Setting} from './types';

const money=(value:string|null|undefined)=>value==null?'Unavailable':BigInt(value).toLocaleString('en-US');
async function api<T>(path:string,body?:unknown,method='GET',signal?:AbortSignal):Promise<T> {
  const response=await fetch('/api/v1/'+path,{method,body:body===undefined?undefined:JSON.stringify(body),headers:{'Content-Type':'application/json'},cache:'no-store',signal});
  if(!response.ok) {const error=await response.json();throw new Error(error.message||error.detail||error.code||'Request failed');}
  return response.json();
}

export default function CommandCenter() {
  const [report,setReport]=useState<Dashboard|null>(null);
  const [filters,setFilters]=useState({from_date:'',to_date:'',platform:'',campaign_id:'',objective:'',market:''});
  const [refreshError,setRefreshError]=useState('');
  const [lastSuccess,setLastSuccess]=useState('');
  const [settings,setSettings]=useState<Setting[]>([]);
  const [rules,setRules]=useState<Rule[]>([]);
  const [alerts,setAlerts]=useState<Alert[]>([]);
  const [campaignId,setCampaignId]=useState('');
  const [market,setMarket]=useState('');
  const [budget,setBudget]=useState('');
  const [start,setStart]=useState('');
  const [end,setEnd]=useState('');
  const [message,setMessage]=useState('');
  const [pace,setPace]=useState<Pace|null>(null);
  const [kind,setKind]=useState('refund_share');
  const [threshold,setThreshold]=useState('0');
  const [enabled,setEnabled]=useState(true);
  const [ruleId,setRuleId]=useState('');
  const [currency,setCurrency]=useState('');
  const [total,setTotal]=useState('');
  const [allocations,setAllocations]=useState<Record<string,string>>({});
  const [scenario,setScenario]=useState<Scenario|null>(null);
  const [scenarioError,setScenarioError]=useState('');
  const [retry,setRetry]=useState(0);
  const [busy,setBusy]=useState(false);
  const query=new URLSearchParams(Object.entries(filters).filter(([,value])=>!!value)).toString();
  const scenarioContext=useRef('');
  const scenarioRequest=useRef(0);
  scenarioContext.current=JSON.stringify([query,currency,total,allocations]);
  useEffect(()=>{setScenario(null);setScenarioError('');},[query,currency,total,allocations]);

  useEffect(()=>{
    let active=true,inFlight=false;
    const controller=new AbortController();
    const load=async()=>{
      if(inFlight) return;
      inFlight=true;
      try {
        const data=await api<Dashboard>('dashboard?'+query,undefined,'GET',controller.signal);
        if(active) {
          setReport(data);setRefreshError('');setLastSuccess(new Date().toLocaleTimeString());
          setFilters(current=>current.from_date&&current.to_date?current:{...current,...data.filters});
          setCampaignId(current=>current||data.campaigns[0]?.campaign_id||'');
          setCurrency(current=>current||data.campaigns[0]?.currency||'');
        }
        const items=await api<Alert[]>('alerts',undefined,'GET',controller.signal);
        if(active) setAlerts(items);
      } catch(error) {if(active&&!controller.signal.aborted) setRefreshError(error instanceof Error?error.message:'Request failed');}
      finally {inFlight=false;}
    };
    void load();const timer=setInterval(()=>void load(),5000);
    return ()=>{active=false;controller.abort();clearInterval(timer);};
  },[query,retry]);

  useEffect(()=>{void Promise.all([api<Setting[]>('campaign-settings'),api<Rule[]>('alert-rules')]).then(([items,r])=>{setSettings(items);setRules(r);}).catch(error=>setMessage(error.message));},[]);
  const campaigns=report?.campaigns||[];
  const campaign=campaigns.find(item=>item.campaign_id===campaignId);
  const saved=settings.find(item=>item.campaign_id===campaignId);
  useEffect(()=>{
    setMarket(saved?.market||'');setBudget(saved?.budget_minor||'');
    setStart(saved?.period_start||report?.filters.from_date||'');setEnd(saved?.period_end||report?.filters.to_date||'');
    setPace(null);
  },[campaignId,saved,report?.filters.from_date,report?.filters.to_date]);

  const action=async(operation:()=>Promise<void>)=>{
    setBusy(true);setMessage('');
    try {await operation();} catch(error) {setMessage(error instanceof Error?error.message:'Request failed');}
    finally {setBusy(false);}
  };
  const save=()=>action(async()=>{
    if(!campaign) throw new Error('Select a campaign');
    const item=await api<Setting>('campaign-settings/'+encodeURIComponent(campaignId),{market:market||null,currency:campaign.currency,period_start:start,period_end:end,budget_minor:budget,expected_version:Number(saved?.version||0)},'PUT');
    setSettings(current=>[...current.filter(row=>row.campaign_id!==campaignId),item]);setMessage('Campaign settings saved');setRetry(value=>value+1);
  });
  const saveRule=()=>action(async()=>{
    const previous=rules.find(row=>row.rule_id===ruleId);
    await api(ruleId?'alert-rules/'+encodeURIComponent(ruleId):'alert-rules',{campaign_id:campaignId,kind,threshold:kind==='budget_overrun'?'1':threshold,enabled,expected_version:Number(previous?.version||0)},ruleId?'PUT':'POST');
    setRules(await api<Rule[]>('alert-rules'));setMessage('Alert rule saved');
  });
  const simulate=async()=>{
    const context=scenarioContext.current;
    const request=++scenarioRequest.current;
    setScenarioError('');setScenario(null);
    try {
      const rows=campaigns.filter(row=>row.currency===currency);
      if(!/^[0-9]+$/.test(total)||BigInt(total)<=0n) throw new Error('Enter a positive scenario total');
      const values=rows.map(row=>({campaign_id:row.campaign_id,budget_minor:allocations[row.campaign_id]||'0'}));
      if(values.some(row=>!/^[0-9]+$/.test(row.budget_minor))) throw new Error('Use whole nonnegative allocations');
      if(values.reduce((sum,row)=>sum+BigInt(row.budget_minor),0n)!==BigInt(total)) throw new Error('Allocations must equal the scenario total');
      const result=await api<Scenario>('budget-simulation',{from_date:filters.from_date,to_date:filters.to_date,currency,total_budget_minor:total,allocations:values},'POST');
      if(context===scenarioContext.current&&request===scenarioRequest.current) setScenario(result);
    } catch(error) {if(context===scenarioContext.current&&request===scenarioRequest.current) setScenarioError(error instanceof Error?error.message:'Scenario failed');}
  };

  return <main>
    <header className="masthead"><div><div className="brand">MARKETPULSE <span>360</span></div><h1>Marketing Command Center</h1><p>Campaign performance, decisions and budget scenarios</p></div><div className="status"><span className="dot"/> Updates every 5 seconds<br/><small>Last successful read: {lastSuccess||'Waiting'}</small></div></header>
    <section className="filters" aria-label="Campaign filters">
      <label>From (UTC)<input type="date" value={filters.from_date} onChange={e=>setFilters({...filters,from_date:e.target.value})}/></label>
      <label>To (exclusive UTC)<input type="date" value={filters.to_date} onChange={e=>setFilters({...filters,to_date:e.target.value})}/></label>
      <label>Platform<select value={filters.platform} onChange={e=>setFilters({...filters,platform:e.target.value})}><option value="">All platforms</option>{['google','meta','tiktok'].map(value=><option key={value}>{value}</option>)}</select></label>
      <label>Campaign<select value={filters.campaign_id} onChange={e=>setFilters({...filters,campaign_id:e.target.value})}><option value="">All campaigns</option>{campaigns.map(row=><option key={row.campaign_id} value={row.campaign_id}>{row.name}</option>)}</select></label>
      <label>Objective<select value={filters.objective} onChange={e=>setFilters({...filters,objective:e.target.value})}><option value="">All objectives</option>{['awareness','traffic','conversions'].map(value=><option key={value}>{value}</option>)}</select></label>
      <label>Configured market<input maxLength={2} placeholder="All / e.g. VN" value={filters.market} onChange={e=>setFilters({...filters,market:e.target.value.toUpperCase()})}/></label>
    </section>
    {refreshError&&<div className="error" role="alert"><strong>{report?'Refresh failed; showing last successful data':'Data unavailable'}</strong><p>{refreshError}</p><button onClick={()=>setRetry(value=>value+1)}>Retry</button></div>}
    {!report&&!refreshError&&<p className="empty">Loading published campaign data…</p>}
    {report&&<>
      <section className="section-heading"><div><h2>Certified campaign performance</h2><p>Governed daily data · Money in minor units · UTC dates</p></div><span className="badge">{report.certified.freshness.historical?'Historical data':report.certified.freshness.stale?'Stale data':'Current snapshot'}</span></section>
      <div className="kpis">{report.certified.totals.map(row=><div className="kpi-group" key={row.currency}><article><span>Certified net revenue · {row.currency}</span><strong>{money(row.net_revenue_minor)}</strong></article><article><span>Ad spend · {row.currency}</span><strong>{money(row.spend_minor)}</strong></article><article><span>Orders</span><strong>{money(row.orders)}</strong></article><article><span>Clicks / impressions</span><strong>{money(row.clicks)} <em>/ {money(row.impressions)}</em></strong></article></div>)}</div>
      {report.certified.rows.length===0?<p className="empty">No campaign data matches these filters.</p>:<div className="table-scroll"><table><thead><tr><th>Campaign</th><th>Platform / market</th><th>Spend</th><th>Net revenue</th><th>Orders</th><th>Net ROAS</th></tr></thead><tbody>{report.certified.rows.map(row=><tr key={row.campaign_id+row.currency}><td><strong>{row.campaign_name||row.campaign_id}</strong><small>{row.objective} · {row.currency}</small></td><td>{row.platform||'Unmapped'}<small>{row.configured_market||'Market unknown'}</small></td><td>{money(row.spend_minor)}</td><td>{money(row.net_revenue_minor)}</td><td>{money(row.orders)}</td><td>{row.source_roas??'Unavailable'}</td></tr>)}</tbody></table></div>}
      <details className="provenance"><summary>Metric source &amp; freshness · {report.certified.provenance.model}</summary><p>Generation {report.certified.provenance.generation} · Data through {report.certified.provenance.data_through||'No data'} (day resolution)</p><p>Release {report.certified.provenance.identity}</p><p>Read at {report.certified.provenance.observed_at}; polling does not change data freshness.</p></details>
      <section className="panel"><div className="section-heading"><div><h2>Streaming activity</h2><p>Provisional Flink aggregates · Separate from certified finance</p></div><span className="badge neutral">{report.streaming.status}</span></div><p className="source">{report.streaming.source}</p>
        {report.streaming.rows.length===0?<p className="empty">No committed streaming snapshot is available for this selection.</p>:<div className="table-scroll"><table><thead><tr><th>Campaign</th><th>Sessions</th><th>Orders</th><th>Provisional net</th><th>Business event / processing / arrival</th></tr></thead><tbody>{report.streaming.rows.map(row=><tr key={row.metric_key}><td>{campaigns.find(item=>item.campaign_id===row.campaign_id)?.name||row.campaign_id}<small>{row.mapped?'Mapped':'Unmapped'} · {row.stale?'Historical / stale':'Recent activity'}</small></td><td>{money(row.sessions)}</td><td>{money(row.order_count)}</td><td>{money(row.net_revenue_minor)} {row.currency||''}</td><td><small>{row.max_event_time}<br/>{row.processed_at}<br/>{row.received_at}</small></td></tr>)}</tbody></table></div>}
      </section>
    </>}
    <div className="operations">
      <section className="panel"><h2>Campaign settings &amp; pacing</h2><p>Set an app budget for a UTC period. Source budgets are reference data.</p>
        <label>Campaign to configure<select disabled={busy} value={campaignId} onChange={e=>{setCampaignId(e.target.value);setRuleId('');}}>{campaigns.map(row=><option key={row.campaign_id} value={row.campaign_id}>{row.name}</option>)}</select></label>
        <div className="form-grid"><label>Campaign market<input value={market} maxLength={2} placeholder="Unassigned" onChange={e=>setMarket(e.target.value.toUpperCase())}/></label><label>Period budget (minor units)<input inputMode="numeric" value={budget} onChange={e=>setBudget(e.target.value)}/></label><label>Period start (UTC)<input type="date" value={start} onChange={e=>setStart(e.target.value)}/></label><label>Period end (exclusive UTC)<input type="date" value={end} onChange={e=>setEnd(e.target.value)}/></label></div>
        <p className="source">{campaign?.currency||'Select currency via campaign'} · Saved version {saved?.version||'none'}</p>
        <div className="actions"><button disabled={busy||!campaignId} onClick={()=>void save()}>Save campaign settings</button><button className="secondary" disabled={busy||!campaignId} onClick={()=>void action(async()=>setPace(await api<Pace>('budget-pacing/'+encodeURIComponent(campaignId))))}>View pacing</button></div>
        {pace&&<div className="result"><strong>{pace.status}</strong><dl><dt>Observed spend</dt><dd>{money(pace.spend_minor)}</dd><dt>Remaining budget</dt><dd>{money(pace.remaining_minor)}</dd><dt>Projected period spend</dt><dd>{money(pace.forecast_minor)}</dd><dt>Closed days</dt><dd>{pace.completed_days} / {pace.period_days}</dd></dl><p>As of {pace.data_through||'No complete dates'}; closed daily spend only.</p></div>}
      </section>
      <section className="panel"><h2>Alert rules</h2><p>Explainable thresholds on certified data for the selected campaign.</p>
        <label>Rule to edit<select value={ruleId} onChange={e=>{setRuleId(e.target.value);const row=rules.find(item=>item.rule_id===e.target.value);if(row){setKind(row.kind);setThreshold(row.threshold);setEnabled(row.enabled);}}}><option value="">New rule</option>{rules.filter(row=>row.campaign_id===campaignId).map(row=><option key={row.rule_id} value={row.rule_id}>{row.kind} · version {row.version}</option>)}</select></label>
        <div className="form-grid"><label>Rule type<select value={kind} onChange={e=>{setKind(e.target.value);setThreshold(e.target.value==='spend_spike'?'2':e.target.value==='budget_overrun'?'1':'0');}}><option value="refund_share">Refund share</option><option value="spend_spike">Daily spend spike</option><option value="budget_overrun">Period budget overrun</option></select></label><label>Threshold<input value={threshold} disabled={kind==='budget_overrun'} onChange={e=>setThreshold(e.target.value)}/></label></div>
        <label className="checkbox"><input type="checkbox" checked={enabled} onChange={e=>setEnabled(e.target.checked)}/> Enabled</label>
        <div className="actions"><button disabled={busy||!campaignId} onClick={()=>void saveRule()}>Save alert rule</button><button className="secondary" disabled={busy} onClick={()=>void action(async()=>{await api('alerts/evaluate',{},'POST');setAlerts(await api<Alert[]>('alerts'));setMessage('Alert evaluation complete');})}>Evaluate alerts</button></div>
      </section>
    </div>
    {message&&<p className="notice" role="status">{message}</p>}
    <section className="panel"><div className="section-heading"><div><h2>Alert evidence</h2><p>Immutable decisions tied to source release, rule and budget versions</p></div><span className="badge neutral">{alerts.length} occurrences</span></div>
      {!alerts.length?<p className="empty">No triggered alerts. Create a rule and evaluate certified data.</p>:<div className="alert-grid">{alerts.map(row=><article className="alert" key={row.evaluation_key}><strong>{row.evidence.explanation}</strong><p>{campaigns.find(item=>item.campaign_id===row.evidence.campaign_id)?.name||row.evidence.campaign_id}</p><small>{row.evidence.from_date} → {row.evidence.to_date} · {row.evidence.currency}</small><dl>{Object.entries(row.evidence.operands).map(([key,value])=><div key={key}><dt>{key.replaceAll('_',' ')}</dt><dd>{value}</dd></div>)}</dl><small>Release {row.evidence.provenance?.identity?.slice(0,12)||'recorded'} · {row.evaluated_at}</small></article>)}</div>}
    </section>
    <section className="panel"><div className="section-heading"><div><h2>What-if budget allocation</h2><p>Use the selected historical UTC interval. A scenario leaves saved budgets unchanged.</p></div><span className="badge neutral">Read-only scenario</span></div>
      <div className="form-grid"><label>Scenario currency<select value={currency} onChange={e=>{setCurrency(e.target.value);setScenario(null);}}>{Array.from(new Set(campaigns.map(row=>row.currency))).map(value=><option key={value}>{value}</option>)}</select></label><label>Scenario total (minor units)<input inputMode="numeric" value={total} onChange={e=>setTotal(e.target.value)}/></label></div>
      <div className="allocation-grid">{campaigns.filter(row=>row.currency===currency).map(row=><label key={row.campaign_id}>Allocation for {row.name}<input inputMode="numeric" value={allocations[row.campaign_id]||''} placeholder="0" onChange={e=>setAllocations({...allocations,[row.campaign_id]:e.target.value})}/></label>)}</div>
      <button disabled={!campaigns.length} onClick={()=>void simulate()}>Simulate allocation</button>
      {scenarioError&&<p className="error" role="alert">{scenarioError}</p>}
      {scenario&&<div className="result"><h3>Scenario projected net: {money(scenario.projected_net_minor)} {currency}</h3><p>{scenario.assumption}</p><div className="table-scroll"><table><thead><tr><th>Campaign</th><th>Historical net ROAS</th><th>Proposed allocation</th><th>Projected net</th></tr></thead><tbody>{scenario.rows.map(row=><tr key={row.campaign_id}><td>{campaigns.find(item=>item.campaign_id===row.campaign_id)?.name||row.campaign_id}</td><td>{row.historical_net_roas||'Insufficient baseline'}</td><td>{money(row.allocated_budget_minor)}</td><td>{money(row.projected_net_minor)}</td></tr>)}</tbody></table></div></div>}
    </section>
    <footer>MarketPulse 360 · MP-06 · Synthetic marketing source data · Application settings created by the operator</footer>
  </main>;
}
