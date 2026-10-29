'use client';
import Link from 'next/link';
import {useEffect,useState} from 'react';
import {exact} from '../lib/format';
import {stages,type Funnel} from '../app/analysis/types';

const labels:Record<typeof stages[number],string>={sessions:'Sessions',page_view_sessions:'Page view',view_item_sessions:'View item',add_to_cart_sessions:'Add to cart',checkout_sessions:'Checkout',purchase_sessions:'Purchase'};

/** Certified stage reach for the overview selection; drawn only for the release the overview shows. */
export default function StageReach({query,identity}:{query:string;identity:string}){
 const [state,setState]=useState<{query:string;funnel?:Funnel;error?:string}|null>(null);
 useEffect(()=>{
  const controller=new AbortController();
  void fetch('/api/v1/analysis/funnel?'+query,{cache:'no-store',signal:controller.signal}).then(async r=>{
   if(!r.ok)throw new Error('Stage reach unavailable');
   const body=await r.json();if(!body?.totals||!body?.provenance)throw new Error('Stage reach unavailable');
   setState({query,funnel:body});
  }).catch(()=>{if(!controller.signal.aborted)setState({query,error:'Stage reach unavailable for this selection.'});});
  return()=>controller.abort();
 },[query]);
 const funnel=state?.query===query?state.funnel:undefined;
 const values=funnel?stages.map(stage=>BigInt(funnel.totals[stage])):[];
 const max=values.reduce((a,b)=>a>b?a:b,0n)||1n;
 return <section className="panel stage-panel" aria-label="Stage reach"><div className="section-heading"><div><h2>Stage reach</h2><p>Certified sessions reaching each stage · campaign-day session incidences</p></div></div>
  {state?.query!==query?<p className="empty">Loading stage reach…</p>:
   state.error?<p className="empty">{state.error}</p>:
   funnel&&funnel.provenance.identity!==identity?<p className="empty">Stage reach is from a different certified release; it will update with the next refresh.</p>:
   funnel&&<>{stages.map((stage,i)=><div className="channel-row" key={stage}><span>{labels[stage]}</span><div className="channel-track" aria-hidden="true"><span className="stage" style={{width:(values[i]===0n?0:Math.max(2,Number(values[i]*100n/max)))+'%'}}/></div><strong>{exact(funnel.totals[stage])}</strong></div>)}
    <p className="source">Stages can be skipped, so this is stage reach, not a sequential funnel. One session can count in several campaign-days.</p>
    <Link className="drill" href={'/analysis?'+query}>Open journey analysis</Link></>}
 </section>;
}
