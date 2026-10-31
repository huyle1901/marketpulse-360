'use client';

import {useEffect,useState} from 'react';
import {ratePercent} from './Engagement';
import {exact,formatMoney} from '../lib/format';

type Count={visitors:string};
type Money={currency:string;orders:string;gross_minor:string;refunds_minor:string;net_minor:string};
export type LiveViewReport={status:string;message?:string;anchor:string;window_minutes:string;
 active:{visitors:string;by_source:({source:string}&Count)[];by_device:({device:string}&Count)[]};
 behaviour:{stage:string;sessions:string}[];
 today:{visitors:string;sessions:string;purchased_sessions:string;conversion:string|null;money:Money[]};
 series:{minute:string;visitors:string;orders:string}[];
 top_products:{label:string;category:string|null;units:string;carts:string;views:string}[];
 activity:{event_time:string;kind:string;source:string;device:string;label:string|null;amount_minor:string|null;currency:string|null}[];
 freshness:{anchor:string;last_received_at:string;seconds_since_receipt:string;receiving:boolean};
 provenance:{source:string;source_identity:string|null;rows:string;offset_bounds:{partition:string;from_offset:string;to_offset:string}[];note:string}};

// The shop floor: each station is a step of the buying journey, in order.
const STATIONS:Record<string,string>={browsing:'Browsing',viewing_product:'Viewing a product',cart:'Holding a cart',checkout:'Checking out',purchased:'Purchased'};
// Marker colour carries the kind of event: grey looks, amber intent, green money in, coral money out.
const KINDS:Record<string,{label:string;tone:string}>={browsing:{label:'Browsing',tone:'bg-muted-foreground/50'},viewed_product:{label:'Viewed a product',tone:'bg-muted-foreground'},
 added_to_cart:{label:'Added to cart',tone:'bg-intent'},started_checkout:{label:'Started checkout',tone:'bg-intent'},purchased:{label:'Purchased',tone:'bg-live'},
 order_placed:{label:'Order placed',tone:'bg-live'},order_refunded:{label:'Order refunded',tone:'bg-refund'}};
const plural=(value:string,one:string,many:string)=>`${exact(value)} ${value==='1'?one:many}`;
const SOURCES:Record<string,string>={web:'Web',mobile:'App',commerce:'Store',email:'Email'};
const MAX_DOTS=24;
const clock=(iso:string)=>iso.slice(11,16);
const day=(iso:string)=>new Date(iso).toLocaleDateString('en-US',{weekday:'short',month:'short',day:'numeric',year:'numeric',timeZone:'UTC'});
const ago=(iso:string,anchor:string)=>{const minutes=Math.round((Date.parse(anchor)-Date.parse(iso))/60000);return minutes<1?'just now':minutes<60?`${minutes} min ago`:`${Math.floor(minutes/60)} h ${minutes%60} min ago`;};
const valid=(body:any)=>body&&typeof body.status==='string'&&(body.status!=='available'||(body.active&&Array.isArray(body.behaviour)&&body.today&&Array.isArray(body.series)&&Array.isArray(body.top_products)&&Array.isArray(body.activity)));

function useLiveView(){
 const [data,setData]=useState<LiveViewReport|null>(null);const [error,setError]=useState('');
 useEffect(()=>{
  let active=true,inFlight=false;const controller=new AbortController();
  const load=async()=>{
   if(inFlight)return;inFlight=true;
   try{const response=await fetch('/api/v1/live/view',{cache:'no-store',signal:controller.signal});const body=await response.json();
    if(!response.ok)throw new Error(body?.message||body?.code||'Live View unavailable');if(!valid(body))throw new Error('Unexpected Live View response');
    if(active){setData(body);setError('');}}
   catch(e){if(active&&!controller.signal.aborted)setError(e instanceof Error?e.message:'Live View unavailable');}
   finally{inFlight=false;}
  };
  void load();const timer=setInterval(()=>void load(),5000);
  return()=>{active=false;controller.abort();clearInterval(timer);};
 },[]);
 return {data,error};
}

function ShopFloor({data}:{data:LiveViewReport}){
 const sources=new Map(data.active.by_source.map(row=>[row.source,Number(row.visitors)]));
 const web=sources.get('web')||0,app=sources.get('mobile')||0,split=web+app;
 return <section aria-label="Shop floor" className="live-floor relative overflow-hidden rounded-[22px] text-muted-foreground">
  <div className="grid gap-8 p-6 md:p-8 lg:grid-cols-[minmax(0,15rem)_minmax(0,1fr)]">
   <article aria-label="Visitors right now" className="min-w-0">
    <div className="flex items-center gap-2 text-sm"><span className={'live-beacon '+(data.freshness.receiving?'is-on':'')} aria-hidden/>
     <span>{data.freshness.receiving?'Live':'Waiting for events'}</span></div>
    <div className="live-figure mt-4 text-[var(--hero-figure)]">{exact(data.active.visitors)}</div>
    <p className="mt-1 text-sm text-muted-foreground">people on the store in the last {data.window_minutes} minutes</p>
    <div className="mt-5 flex h-1.5 overflow-hidden rounded-full bg-muted" aria-hidden>
     {split>0&&<><span className="bg-live" style={{width:web/split*100+'%'}}/><span className="bg-order" style={{width:app/split*100+'%'}}/></>}</div>
    <p className="mt-2 flex gap-4 text-xs text-muted-foreground"><span><i className="mr-1.5 inline-block size-2 rounded-full bg-live"/>Web {exact(String(web))}</span><span><i className="mr-1.5 inline-block size-2 rounded-full bg-order"/>App {exact(String(app))}</span></p>
   </article>
   <div className="min-w-0">
    <div className="flex flex-wrap items-baseline justify-between gap-x-6 gap-y-1">
     <h2 className="text-base font-medium text-foreground">Where shoppers are now</h2>
     <p className="text-xs text-muted-foreground">Replay time <span className="live-mono text-foreground">{clock(data.anchor)}</span> UTC · {day(data.anchor)}</p></div>
    <ol aria-label="Shopper behaviour" className="live-aisle mt-6 grid grid-cols-5">
     {data.behaviour.map((row,index)=>{const count=Number(row.sessions),shown=Math.min(count,MAX_DOTS);
      return <li key={row.stage} className="relative flex min-w-0 flex-col items-center text-center">
       <div className="flex h-16 w-full flex-wrap-reverse content-start items-end justify-center gap-1.5 px-1" aria-hidden>
        {Array.from({length:shown},(_,dot)=><span key={dot} className={'live-dot '+(row.stage==='purchased'?'is-bought':'')}/>)}
        {count>MAX_DOTS&&<span className="text-[10px] text-foreground/70">+{count-MAX_DOTS}</span>}</div>
       <span className={'live-station '+(count?'is-busy':'')} aria-hidden>{index+1}</span>
       <span className="mt-3 text-[11px] leading-tight sm:text-xs">{STATIONS[row.stage]||row.stage}</span>
       <span className="live-mono mt-1 text-lg text-foreground">{exact(row.sessions)}</span>
      </li>;})}
    </ol>
   </div>
  </div>
  <p className="border-t border-border px-6 py-3 text-[11px] text-muted-foreground md:px-8">Each dot is one active session at the furthest step it reached · Provisional · not certified · last event received {exact(data.freshness.seconds_since_receipt)}s ago</p>
 </section>;
}

function Today({data}:{data:LiveViewReport}){
 const money=data.today.money;
 const items:[string,string,string][]=[
  ['Net sales today',money.length?money.map(m=>formatMoney(m.net_minor,m.currency)).join(' · '):'No sales yet',money.length?money.map(m=>`${formatMoney(m.gross_minor,m.currency)} gross, ${formatMoney(m.refunds_minor,m.currency)} refunded`).join(' · '):'Since 00:00 UTC'],
  ['Orders today',exact(money.reduce((sum,m)=>sum+BigInt(m.orders),0n).toString()),'Placed since 00:00 UTC'],
  ['Conversion today',data.today.conversion==null?'No sessions yet':ratePercent(data.today.conversion),`${exact(data.today.purchased_sessions)} of ${exact(data.today.sessions)} sessions bought`],
  ['Visitors today',exact(data.today.visitors),plural(data.today.sessions,'session','sessions')]];
 return <section aria-label="Today so far" className="grid grid-cols-2 gap-y-5 border-y py-5 lg:grid-cols-4 lg:divide-x">
  {items.map(([name,value,hint])=><article key={name} aria-label={name} className="min-w-0 px-1 lg:px-6 lg:first:pl-0">
   <h3 className="text-sm text-muted-foreground">{name}</h3><p className="live-mono mt-1 truncate text-2xl text-foreground">{value}</p><p className="mt-0.5 text-xs text-muted-foreground">{hint}</p></article>)}
 </section>;
}

function Minutes({data}:{data:LiveViewReport}){
 const peak=Math.max(1,...data.series.map(point=>Number(point.visitors)));
 const orders=data.series.reduce((sum,point)=>sum+Number(point.orders),0),visitors=data.series.reduce((sum,point)=>sum+Number(point.visitors),0);
 return <section aria-labelledby="minutes-heading" className="min-w-0">
  <h2 id="minutes-heading" className="text-base font-medium">Minute by minute</h2>
  <p className="text-sm text-muted-foreground">Visitors per replay minute; a blue mark is a minute with an order.</p>
  <div role="img" aria-label="Visitors and orders per minute, last 30 minutes" className="mt-4">
   <div className="flex h-28 items-end gap-[3px] border-b">{data.series.map(point=>{const v=Number(point.visitors),o=Number(point.orders);
    return <div key={point.minute} className="relative flex h-full flex-1 flex-col justify-end" title={`${clock(point.minute)} UTC · ${v} visitors · ${o} orders`}>
     {o>0&&<span className="mx-auto mb-1 size-2 rounded-full bg-order"/>}
     <span className={'block rounded-t-[3px] '+(v?'bg-live':'bg-muted')} style={{height:v?Math.max(8,v/peak*100)+'%':'3px'}}/></div>;})}</div>
   <div className="mt-1 flex justify-between text-[11px] text-muted-foreground"><span>{clock(data.series[0]?.minute||data.anchor)}</span><span>{clock(data.series[data.series.length-1]?.minute||data.anchor)} UTC</span></div>
  </div>
  <p className="sr-only">{visitors} visitor-minutes and {orders} orders in the last {data.window_minutes} minutes.</p>
 </section>;
}

function Feed({data}:{data:LiveViewReport}){
 return <section aria-labelledby="feed-heading" className="min-w-0">
  <h2 id="feed-heading" className="text-base font-medium">Happening now</h2>
  <ol aria-label="Recent activity" className="mt-3">{data.activity.map((row,index)=>{const kind=KINDS[row.kind]||{label:row.kind,tone:'bg-muted-foreground/50'};
   return <li key={index} className="live-step relative flex gap-3 pb-3 pl-1 text-sm">
    <span className={'relative z-10 mt-1.5 size-2.5 shrink-0 rounded-full ring-4 ring-background '+kind.tone} aria-hidden/>
    <div className="min-w-0 flex-1"><p><span className="font-medium">{kind.label}</span>{row.label&&<span className="text-muted-foreground"> · {row.label}</span>}</p>
     <p className="text-xs text-muted-foreground">{ago(row.event_time,data.anchor)} · {SOURCES[row.source]||row.source}{row.device!=='unknown'?' · '+row.device:''}</p></div>
    {row.amount_minor&&row.currency&&<span className={'live-mono shrink-0 '+(row.kind==='order_refunded'?'text-refund':'text-live')}>{row.kind==='order_refunded'?'−':'+'}{formatMoney(row.amount_minor,row.currency)}</span>}
   </li>;})}</ol>
 </section>;
}

function Selling({data}:{data:LiveViewReport}){
 const score=(row:LiveViewReport['top_products'][number])=>Number(row.units)*100+Number(row.carts)*10+Number(row.views);
 const top=Math.max(1,...data.top_products.map(score));
 return <section aria-labelledby="selling-heading" className="min-w-0">
  <h2 id="selling-heading" className="text-base font-medium">Selling today</h2>
  {!data.top_products.length?<p className="mt-3 text-sm text-muted-foreground">Nothing has been viewed or ordered yet today; products appear here as shoppers arrive.</p>:
  <ol aria-label="Top products today" className="mt-3 space-y-3">{data.top_products.map((row,index)=><li key={index} className="grid grid-cols-[1.5rem_minmax(0,1fr)] gap-x-2">
   <span className="live-mono pt-0.5 text-sm text-muted-foreground">{index+1}</span>
   <div className="min-w-0"><div className="flex items-baseline justify-between gap-3"><span className="truncate font-medium">{row.label}</span><span className="shrink-0 text-xs capitalize text-muted-foreground">{row.category||'uncategorised'}</span></div>
    <div className="mt-1 h-1 rounded-full bg-muted"><div className="h-full rounded-full bg-primary" style={{width:score(row)/top*100+'%'}}/></div>
    <p className="mt-1 text-xs text-muted-foreground">{exact(row.units)} ordered · {exact(row.carts)} added to cart · {plural(row.views,'view','views')}</p></div></li>)}</ol>}
 </section>;
}

export function LiveView(){
 const {data,error}=useLiveView();
 if(error&&!data)return <div className="error" role="alert"><strong>Live View unavailable</strong><p>{error}</p></div>;
 if(!data)return <p className="empty">Loading Live View…</p>;
 if(data.status!=='available')return <p className="empty">{data.message||'No live event has been projected yet.'}</p>;
 return <div className="space-y-8">
  {error&&<div className="error" role="alert"><strong>The last check failed; showing the previous Live View</strong><p>{error}</p></div>}
  <ShopFloor data={data}/>
  <Today data={data}/>
  <div className="grid items-start gap-10 lg:grid-cols-[minmax(0,1.1fr)_minmax(0,1fr)] [&>*]:min-w-0">
   <div className="space-y-10"><Minutes data={data}/><Selling data={data}/></div>
   <Feed data={data}/>
  </div>
  <details className="provenance"><summary>Where these numbers come from</summary><p>{data.provenance.source}</p><p>{data.provenance.source_identity||'Unavailable'}</p>
   <p>{exact(data.provenance.rows)} stored events · offsets {data.provenance.offset_bounds.map(b=>`p${b.partition} ${b.from_offset}–${b.to_offset}`).join(', ')||'none'}</p></details>
 </div>;
}
