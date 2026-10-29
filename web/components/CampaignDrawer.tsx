'use client';
import {useEffect,useRef,useState,type ReactNode} from 'react';
import {exact,formatMoney} from '../lib/format';
import type {Dashboard,Metric} from '../app/types';

const focusable='a[href],button:not([disabled]),input:not([disabled]),select:not([disabled]),textarea:not([disabled]),summary,[tabindex]:not([tabindex="-1"])';

/** Daily certified rows for one campaign in the displayed window, accepted only from the displayed release. */
function CampaignDaily({query,row,identity}:{query:string;row:Metric;identity:string}){
 const [state,setState]=useState<{key:string;report?:Dashboard;error?:string}|null>(null);
 const key=query+'|'+row.campaign_id;
 useEffect(()=>{
  const controller=new AbortController(),params=new URLSearchParams(query);params.set('campaign_id',row.campaign_id);
  void fetch('/api/v1/dashboard?'+params,{cache:'no-store',signal:controller.signal}).then(async r=>{if(!r.ok)throw new Error();setState({key,report:await r.json()});})
   .catch(()=>{if(!controller.signal.aborted)setState({key,error:'Daily results unavailable'});});
  return()=>controller.abort();
 },[key]);
 const report=state?.key===key?state.report:undefined,name=row.campaign_name||row.campaign_id;
 if(state?.key!==key)return <p className="empty">Loading daily results…</p>;
 if(state.error||!report)return <p className="empty">{state.error||'Daily results unavailable'}</p>;
 if(report.certified.provenance.identity!==identity)return <p className="empty">Daily results are from a different certified release; reopen after the next refresh.</p>;
 const days=(report.certified.daily_series||[]).filter(d=>d.currency===row.currency);
 return !days.length?<p className="empty">No certified days for this campaign in the selection.</p>:<div className="table-scroll"><table aria-label={'Daily results · '+name}><thead><tr><th>UTC day</th><th>Net revenue</th><th>Ad spend</th><th>Orders</th></tr></thead><tbody>{days.map(d=><tr key={d.event_date}><td>{d.event_date}</td><td title={d.net_revenue_minor+' minor units'}>{formatMoney(d.net_revenue_minor,row.currency)}<small>{d.net_revenue_minor}</small></td><td>{formatMoney(d.spend_minor,row.currency)}</td><td>{exact(d.orders)}</td></tr>)}</tbody></table></div>;
}

export default function CampaignDrawer({row,query,identity,onClose,children}:{row:Metric;query:string;identity:string;onClose:()=>void;children:ReactNode}){
 const panel=useRef<HTMLDivElement>(null),close=useRef(onClose);close.current=onClose;
 useEffect(()=>{
  panel.current?.querySelector<HTMLElement>('[data-autofocus]')?.focus();
  const key=(event:KeyboardEvent)=>{
   if(event.key==='Escape'){event.preventDefault();close.current();return;}
   if(event.key!=='Tab'||!panel.current)return;
   const items=[...panel.current.querySelectorAll<HTMLElement>(focusable)].filter(item=>item.offsetParent!==null);
   if(!items.length)return;
   const first=items[0],last=items.at(-1)!,active=document.activeElement;
   if(!panel.current.contains(active)){event.preventDefault();first.focus();}
   else if(event.shiftKey&&active===first){event.preventDefault();last.focus();}
   else if(!event.shiftKey&&active===last){event.preventDefault();first.focus();}
  };
  document.addEventListener('keydown',key);return()=>document.removeEventListener('keydown',key);
 },[]);
 const name=row.campaign_name||row.campaign_id;
 return <div className="drawer-backdrop" onMouseDown={e=>{if(e.target===e.currentTarget)onClose();}}>
  <div ref={panel} className="drawer" role="dialog" aria-modal="true" aria-labelledby="campaign-drawer-title">
   <header className="drawer-header"><div><h2 id="campaign-drawer-title">{name}</h2><p className="source">{row.platform||'Unmapped'} · {row.objective} · {row.currency} · {row.configured_market||'Market unknown'}</p></div><button data-autofocus className="secondary" aria-label="Close campaign details" onClick={onClose}>Close</button></header>
   <dl className="drawer-kpis"><div><dt>Net revenue</dt><dd>{formatMoney(row.net_revenue_minor,row.currency)}</dd></div><div><dt>Ad spend</dt><dd>{formatMoney(row.spend_minor,row.currency)}</dd></div><div><dt>Orders</dt><dd>{exact(row.orders)}</dd></div></dl>
   <section aria-label="Daily results"><h3>Daily results</h3><CampaignDaily query={query} row={row} identity={identity}/></section>
   {children}
  </div>
 </div>;
}
