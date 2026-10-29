'use client';
import {useEffect,useRef,useState} from 'react';
import {useUrlState,choice} from '../../lib/useUrlState';
import FilterActions from '../../components/FilterActions';
import CohortHeatmap from '../../components/CohortHeatmap';
import CustomerSummary from '../../components/CustomerSummary';
import ProfileDetail from '../../components/ProfileDetail';
import Segments from './segments';
import type {Cohorts,Profiles,Timeline,Provenance} from './types';
import {api,exact,consent} from './helpers';
function Source({value,label}:{value:Provenance;label:string}){return <details className="provenance"><summary>{label} source &amp; freshness</summary><p>{value.model} · {value.serving_relation} · Generation {value.generation}</p><p>As of {value.as_of} · Through {value.data_through||'No selected data'} · Read {value.observed_at}</p><p>Release {value.identity}</p><p>Proof {value.proof_digest}</p>{value.silver_snapshot_id&&<p>Silver snapshot {value.silver_snapshot_id} · Projection {value.projection_checksum}</p>}</details>}
export default function Customers(){
 const [filters,setFilters]=useUrlState({cohort_month:'',activity_month:'',profile_type:'',purpose:'analytics',opportunity:'',offset:'0',q:'',sort:'default'},v=>({...v,profile_type:choice(v.profile_type,['customer','anonymous','session']),purpose:choice(v.purpose,['analytics','activation'],'analytics'),opportunity:choice(v.opportunity,['high_value','churn_risk','cart_opportunity']),offset:/^\d{1,5}$/.test(v.offset)&&Number(v.offset)<=99980?String(Number(v.offset)):'0',sort:choice(v.sort,['default','net_desc','net_asc'],'default')}));
 const [reports,setReports]=useState<{cohorts:Cohorts;profiles:Profiles}|null>(null),[displayed,setDisplayed]=useState(''),[error,setError]=useState(''),[retry,setRetry]=useState(0),[success,setSuccess]=useState('');
 const [selected,setSelected]=useState(''),[timelineOffset,setTimelineOffset]=useState(0),[timeline,setTimeline]=useState<Timeline|null>(null),[timelineError,setTimelineError]=useState('');
 const query=new URLSearchParams(Object.entries(filters).filter(([key,value])=>!!value&&!['q','sort'].includes(key))).toString();const identity=reports?.cohorts.provenance.identity||'';
 useEffect(()=>{
  let active=true,inFlight=false;const controller=new AbortController();
  const load=async()=>{if(inFlight)return;inFlight=true;try{
   const [cohorts,profiles]=await Promise.all([api<Cohorts>('cohorts',controller.signal),api<Profiles>('profiles?'+query,controller.signal)]);
   if(!active)return;if(cohorts.provenance.identity!==profiles.provenance.identity)throw new Error('Release changed between reads; refresh customers again.');
   setReports({cohorts,profiles});setDisplayed(query);setError('');setSuccess(new Date().toLocaleTimeString());
  }catch(e){if(active&&!controller.signal.aborted)setError(e instanceof Error?e.message:'Customers unavailable');}finally{inFlight=false;}};
  void load();const timer=setInterval(()=>void load(),5000);return()=>{active=false;controller.abort();clearInterval(timer);};
 },[query,retry]);
 useEffect(()=>{setSelected('');setTimeline(null);setTimelineOffset(0);},[query,identity]);
 useEffect(()=>{
  if(!selected)return;let active=true;const controller=new AbortController();setTimeline(null);setTimelineError('');
  void api<Timeline>('profiles/'+selected+'/timeline?offset='+timelineOffset,controller.signal).then(data=>{
   if(!active)return;if(data.provenance.identity!==identity||data.profile.profile_ref!==selected)throw new Error('Profile dataset changed; select again after refresh.');setTimeline(data);
  }).catch(e=>{if(active&&!controller.signal.aborted)setTimelineError(e.message);});
  return()=>{active=false;controller.abort();};
 },[selected,timelineOffset,identity,query]);
 const update=(key:keyof typeof filters,value:string)=>setFilters(current=>({...current,[key]:value,offset:'0'}));
 const profileTrigger=useRef<HTMLButtonElement|null>(null);
 const closeTimeline=()=>{setSelected('');profileTrigger.current?.focus();};
 const c=reports?.cohorts,p=reports?.profiles,t=timeline?.profile.profile_ref===selected&&timeline.provenance.identity===identity?timeline:null;
 const profiles=(p?.rows||[]).filter(r=>(r.profile_ref+' '+r.profile_type).toLowerCase().includes(filters.q.trim().toLowerCase())).sort((a,b)=>{if(filters.sort==='default')return 0;const left=BigInt(a.net_revenue_minor),right=BigInt(b.net_revenue_minor);return left===right?0:(left<right?-1:1)*(filters.sort==='net_asc'?1:-1);});
 return <main>
 <header className="masthead"><div><div className="brand">MARKETPULSE <span>360</span></div><h1>Customers</h1><p>Understand historical retention, inspect governed profiles, build consent-aware segments</p></div><div className="status"><span className="dot"/>Certified release · 5-second refresh<br/>Last success {success||'Pending'}<br/><button className="secondary" onClick={()=>setRetry(v=>v+1)}>Refresh customers</button></div></header>
 <nav className="section-links" aria-label="Page sections"><a href="#cohorts">Retention</a><a href="#profiles">Profiles</a><a href="#segments">Segments</a></nav>
 <section className="filters" aria-label="Customer discovery filters">
 <label>Cohort month<input aria-label="Cohort month" type="month" value={filters.cohort_month.slice(0,7)} onChange={e=>update('cohort_month',e.target.value?e.target.value+'-01':'')}/></label><label>Activity month<input aria-label="Activity month" type="month" value={filters.activity_month.slice(0,7)} onChange={e=>update('activity_month',e.target.value?e.target.value+'-01':'')}/></label>
 <label>Profile type<select aria-label="Profile type" value={filters.profile_type} onChange={e=>update('profile_type',e.target.value)}><option value="">All identities</option>{['customer','anonymous','session'].map(v=><option key={v}>{v}</option>)}</select></label>
 <label>Discovery purpose<select value={filters.purpose} onChange={e=>update('purpose',e.target.value)}><option value="analytics">Historical analytics</option><option value="activation">Activation eligible only</option></select></label>
 <label>Opportunity heuristic<select value={filters.opportunity} onChange={e=>update('opportunity',e.target.value)}><option value="">All profiles</option><option value="high_value">High value</option><option value="churn_risk">Churn risk</option><option value="cart_opportunity">Cart opportunity</option></select></label><button className="secondary" onClick={()=>setFilters({cohort_month:'',activity_month:'',profile_type:'',purpose:'analytics',opportunity:'',offset:'0',q:'',sort:'default'})}>All cohorts</button>
 </section>
 <FilterActions onReset={()=>setFilters({cohort_month:'',activity_month:'',profile_type:'',purpose:'analytics',opportunity:'',offset:'0',q:'',sort:'default'})}/>
 {reports&&displayed!==query&&<p className="notice" role="status">{error?'Selection unavailable; showing previous customers':'Loading selection; showing previous customers'}</p>}
 {error&&<div className="error" role="alert"><strong>{reports?'Refresh failed; showing last successful customers':'Customers unavailable'}</strong><p>{error}</p></div>}
 {!reports&&!error&&<p className="empty">Loading certified customer data…</p>}
 {c&&p&&<>
 <p className="scope-note">{c.population}</p><p>As of {c.provenance.as_of} · {c.provenance.historical?'Historical data':'Certified data'} · {c.provenance.currency||p.provenance.currency||'Declared currency'} · Lifetime observed metrics, not predictions</p>
 <div className="customer-kpis kpi-group"><article><span>Historical profiles</span><strong>{exact(c.totals.profiles_total)}</strong></article><article><span>Eligible for activation in release</span><strong>{exact(c.totals.activation_eligible_total)}</strong></article><article><span>Matching discovery purpose: {p.purpose}</span><strong>{exact(p.total)}</strong></article></div>
 <CustomerSummary identity={c.provenance.identity} profilesTotal={c.totals.profiles_total} onOpportunity={value=>update('opportunity',value)}/>
 <section id="cohorts" className="panel"><div className="section-heading"><div><h2>Cohort retention</h2><p>Each cell retains its original cohort denominator. Missing/future cells are not invented zeros.</p></div><span className="badge neutral">Click an activity cell to drill</span></div>
 <CohortHeatmap rows={c.rows} onDrill={row=>setFilters(current=>({...current,cohort_month:row.cohort_month,activity_month:row.activity_month,profile_type:row.profile_type,opportunity:'',offset:'0'}))}/>
 <Source value={c.provenance} label="Cohort"/></section>
 <section id="profiles" className="panel"><div className="section-heading"><div><h2>Observed RFM &amp; behavior</h2><p>Recency from original purchase · Frequency excludes refunds · Monetary is net after refunds, in minor units</p></div><span className="badge">{p.purpose==='activation'?'Consent-gated activation':'Historical analytics'}</span></div>
 <details className="provenance"><summary>Heuristic definitions and identity limits</summary><p>High value: orders≥1 and net≥5,000,000 minor units. Churn risk: orders≥1 and recency≥30 days. Cart opportunity: cart events≥1 and orders=0. Null recency never passes a numeric rule.</p><p>Customer confidence means explicit customer ID or existing deterministic session stitch. Anonymous confidence is device-only; session confidence is session-only. No probability or new person merge is inferred.</p><p>{c.consent_policy} Consent is as of the certified release.</p></details>
 <div className="table-tools"><label>Search loaded profiles<input type="search" value={filters.q} onChange={e=>setFilters(current=>({...current,q:e.target.value}))} placeholder="Profile reference or identity type"/></label><label>Loaded profile order<select value={filters.sort} onChange={e=>setFilters(current=>({...current,sort:e.target.value}))}><option value="default">Source order</option><option value="net_desc">Net monetary: high to low</option><option value="net_asc">Net monetary: low to high</option></select></label></div><p className="source">Search and sort apply to this loaded page only.</p>
 <div className={'profile-explorer'+(selected?' has-selection':'')}><div>
 {p.status==='unavailable'?<p className="empty">{p.message}</p>:!profiles.length?<p className="empty">No profiles match this selection.</p>:<div className="table-scroll"><table aria-label="Customer profiles"><thead><tr><th>Profile / identity evidence</th><th>Recency days</th><th>Orders / refunds</th><th>Net monetary</th><th>Cart / email clicks</th><th>Latest CRM consent</th><th>Activation / opportunities</th></tr></thead><tbody>{profiles.map(row=><tr key={row.profile_ref}><td><button className="drill" aria-label={'Inspect profile '+row.profile_ref.slice(0,12)} onClick={e=>{profileTrigger.current=e.currentTarget;setSelected(row.profile_ref);setTimelineOffset(0);}}>{row.profile_ref.slice(0,12)}</button><small>{row.profile_type} · {row.confidence.replaceAll('_',' ')}</small></td><td>{row.recency_days===null?'No original purchase':exact(row.recency_days)}</td><td>{exact(row.order_count)} / {exact(row.refund_count)}</td><td>{exact(row.net_revenue_minor)}</td><td>{exact(row.add_to_cart_count)} / {exact(row.email_click_count)}</td><td>Analytics {consent(row.crm_consent_analytics)}<small>Marketing {consent(row.crm_consent_marketing)}</small></td><td>{row.activation_eligible?'Eligible':'Excluded'}<small>{row.activation_exclusion_reasons.join(', ')||'Both consent flags granted'}</small><small>{Object.entries(row.opportunities||{}).filter(([,value])=>value).map(([key])=>key).join(', ')||'No heuristic flag'}</small></td></tr>)}</tbody></table></div>}
 <div className="actions"><button className="secondary" disabled={filters.offset==='0'} onClick={()=>setFilters({...filters,offset:String(Math.max(0,Number(filters.offset)-20))})}>Previous profiles</button><button className="secondary" disabled={!p.has_more||Number(filters.offset)>=99980} onClick={()=>setFilters({...filters,offset:String(Number(filters.offset)+20)})}>Next profiles</button><small>Offset {p.offset} · {p.rows.length} shown / {p.total??'Unavailable'} matching</small></div><Source value={p.provenance} label="Profile"/></div>
 {selected&&<ProfileDetail profileRef={selected} onClose={closeTimeline}>
 {timelineError&&<p className="error" role="alert">{timelineError}</p>}{!t&&!timelineError&&<p role="status">Loading selected profile…</p>}
 {t&&<><div className="profile-evidence"><h3>Profile {t.profile.profile_ref.slice(0,12)}</h3><p>{t.profile.confidence}</p><p>{t.profile.resolution_basis}</p><p>Historical event analytics consent {consent(t.profile.consent_analytics)} · Latest CRM analytics {consent(t.profile.crm_consent_analytics)} · Latest CRM marketing {consent(t.profile.crm_consent_marketing)}</p><p>Original purchase {t.profile.last_purchase_at||'None'} · Net {exact(t.profile.net_revenue_minor)} · {t.profile.activation_eligible?'Activation eligible':'Excluded: '+t.profile.activation_exclusion_reasons.join(', ')}</p></div>
 {t.status!=='available'?<p className="empty">{t.message}</p>:<><ol className="timeline">{t.events.map(event=><li key={event.event_ref}><strong>{event.event_name}</strong><small>{event.event_time} · {event.source} · {event.campaign_name}</small><span>Recognized gross {exact(event.gross_revenue_minor)} · Refund {exact(event.refund_amount_minor)}</span></li>)}</ol><div className="actions"><button className="secondary" disabled={timelineOffset===0} onClick={()=>setTimelineOffset(Math.max(0,timelineOffset-50))}>Previous events</button><button className="secondary" disabled={!t.has_more||timelineOffset>=99950} onClick={()=>setTimelineOffset(timelineOffset+50)}>Next events</button><small>{t.events.length} shown / {t.total} total · Offset {t.offset}</small></div></>}<Source value={t.provenance} label="Timeline"/></>}
 </ProfileDetail>}
 </div>
 </section>
 </>}

 <div id="segments"><Segments identity={identity} onMismatch={()=>setRetry(v=>v+1)}/></div>
 <footer>MarketPulse 360 · MP-08 · Synthetic profiles · Activation preview has no delivery side effect</footer>
 </main>;
}
