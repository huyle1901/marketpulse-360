'use client';

import {useEffect,useState} from 'react';
import {Area,AreaChart,CartesianGrid,XAxis,YAxis} from 'recharts';
import {Badge} from '@/components/ui/badge';
import {Card,CardContent,CardDescription,CardHeader,CardTitle} from '@/components/ui/card';
import {ChartContainer,ChartTooltip,ChartTooltipContent,type ChartConfig} from '@/components/ui/chart';
import {Table,TableBody,TableCell,TableHead,TableHeader,TableRow} from '@/components/ui/table';
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

const PROVISIONAL='Provisional · not certified';
const STAGES:Record<string,string>={browsing:'Browsing',viewing_product:'Viewing a product',cart:'Holding a cart',checkout:'Checking out',purchased:'Purchased'};
const KINDS:Record<string,string>={browsing:'Browsing',viewed_product:'Viewed a product',added_to_cart:'Added to cart',started_checkout:'Started checkout',purchased:'Purchased',order_placed:'Order placed',order_refunded:'Order refunded'};
const SOURCES:Record<string,string>={web:'Web',mobile:'App',commerce:'Store',email:'Email'};
const utcMinute=(iso:string)=>iso.replace('T',' ').slice(0,16)+' UTC';
const clock=(iso:string)=>iso.slice(11,16);
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

function Kpi({name,value,hint}:{name:string;value:string;hint:string}){
 return <Card role="article" aria-label={name} className="gap-1 py-3"><CardHeader className="px-4"><CardTitle className="text-xs font-medium text-muted-foreground">{name}</CardTitle></CardHeader>
  <CardContent className="px-4"><div className="text-3xl font-semibold tracking-tight tabular-nums">{value}</div><p className="mt-1 text-xs text-muted-foreground">{hint}</p></CardContent></Card>;
}

function PerMinute({data,measure,label,color}:{data:LiveViewReport['series'];measure:'visitors'|'orders';label:string;color:string}){
 const config={value:{label,color}} satisfies ChartConfig;
 const points=data.map(point=>({minute:clock(point.minute),value:Number(point[measure])}));
 return <div role="img" aria-label={label+' per minute, last 30 minutes'}><p className="mb-1 text-xs font-medium text-muted-foreground">{label} per minute</p>
  <ChartContainer config={config} className="aspect-auto h-[120px] w-full"><AreaChart data={points} margin={{left:0,right:8,top:6}}>
   <CartesianGrid vertical={false}/><XAxis dataKey="minute" tickLine={false} axisLine={false} minTickGap={36}/><YAxis tickLine={false} axisLine={false} width={28} allowDecimals={false}/>
   <ChartTooltip content={<ChartTooltipContent/>}/><Area dataKey="value" type="step" fill="var(--color-value)" fillOpacity={0.15} stroke="var(--color-value)" strokeWidth={2} isAnimationActive={false}/>
  </AreaChart></ChartContainer></div>;
}

export function LiveView(){
 const {data,error}=useLiveView();
 if(error&&!data)return <div className="error" role="alert"><strong>Live View unavailable</strong><p>{error}</p></div>;
 if(!data)return <p className="empty">Loading Live View…</p>;
 if(data.status!=='available')return <p className="empty">{data.message||'No live event has been projected yet.'}</p>;
 const sources=new Map(data.active.by_source.map(row=>[row.source,row.visitors]));
 const money=data.today.money,active=data.behaviour.reduce((sum,row)=>sum+BigInt(row.sessions),0n);
 return <div className="space-y-4">
  {error&&<div className="error" role="alert"><strong>Check failed; showing the last successful Live View</strong><p>{error}</p></div>}
  <p className="flex flex-wrap items-center gap-2 text-sm text-muted-foreground">
   <span className={'inline-block size-2.5 rounded-full '+(data.freshness.receiving?'animate-pulse bg-emerald-500':'bg-muted-foreground')} aria-hidden/>
   <span>Replay time {utcMinute(data.anchor)}</span><span>· {data.freshness.receiving?'Receiving events':'Waiting for new events'} · last received {exact(data.freshness.seconds_since_receipt)}s ago</span>
   <Badge variant="outline">{PROVISIONAL}</Badge></p>
  <section aria-label="Live totals" className="grid grid-cols-2 gap-3 lg:grid-cols-4">
   <Kpi name="Visitors right now" value={exact(data.active.visitors)} hint={`Web ${exact(sources.get('web')||'0')} · App ${exact(sources.get('mobile')||'0')} · last ${data.window_minutes} minutes`}/>
   <Kpi name="Net sales today" value={money.length?money.map(m=>formatMoney(m.net_minor,m.currency)).join(' · '):'No orders yet'} hint={money.length?money.map(m=>`Gross ${formatMoney(m.gross_minor,m.currency)} − refunds ${formatMoney(m.refunds_minor,m.currency)}`).join(' · '):'Replay day so far'}/>
   <Kpi name="Orders today" value={exact(money.reduce((sum,m)=>sum+BigInt(m.orders),0n).toString())} hint={`${exact(data.today.visitors)} visitors · ${exact(data.today.sessions)} sessions`}/>
   <Kpi name="Conversion today" value={data.today.conversion==null?'No sessions yet':ratePercent(data.today.conversion)} hint={`${exact(data.today.purchased_sessions)} purchasing sessions ÷ ${exact(data.today.sessions)} sessions`}/>
  </section>
  <Card><CardHeader><CardTitle>Shopper behaviour</CardTitle><CardDescription>Sessions active in the last {data.window_minutes} minutes, by the furthest step reached · {PROVISIONAL}</CardDescription></CardHeader>
   <CardContent><ol aria-label="Shopper behaviour" className="grid grid-cols-2 gap-2 sm:grid-cols-5">{data.behaviour.map(row=>{const share=active>0n?Number(BigInt(row.sessions)*1000n/active)/10:0;
    return <li key={row.stage} className="rounded-lg border bg-muted/30 p-3"><div className="text-xs text-muted-foreground">{STAGES[row.stage]||row.stage}</div><div className="text-2xl font-semibold tabular-nums">{exact(row.sessions)}</div>
     <div className="mt-2 h-1.5 overflow-hidden rounded-full bg-muted"><div className="h-full rounded-full bg-primary" style={{width:share+'%'}}/></div></li>;})}</ol></CardContent></Card>
  <Card><CardHeader><CardTitle>Last {data.window_minutes} minutes</CardTitle><CardDescription>Replay minutes ending {utcMinute(data.anchor)} · {PROVISIONAL}</CardDescription></CardHeader>
   <CardContent className="grid gap-4 md:grid-cols-2"><PerMinute data={data.series} measure="visitors" label="Visitors" color="var(--chart-1)"/><PerMinute data={data.series} measure="orders" label="Orders" color="var(--chart-2)"/></CardContent></Card>
  <div className="grid gap-4 lg:grid-cols-[minmax(0,3fr)_minmax(0,2fr)] [&>*]:min-w-0">
   <Card><CardHeader><CardTitle>Top products today</CardTitle><CardDescription>Ranked by units ordered, then add-to-carts and views · {PROVISIONAL}</CardDescription></CardHeader>
    <CardContent>{!data.top_products.length?<p className="empty">No product activity yet today.</p>:<Table aria-label="Top products today"><TableHeader><TableRow><TableHead>Product</TableHead><TableHead className="text-right">Ordered</TableHead><TableHead className="text-right">Carts</TableHead><TableHead className="text-right">Views</TableHead></TableRow></TableHeader>
     <TableBody>{data.top_products.map((row,index)=><TableRow key={index}><TableCell><div className="font-medium">{row.label}</div><div className="text-xs capitalize text-muted-foreground">{row.category||'No certified category'}</div></TableCell>
      <TableCell className="text-right tabular-nums">{exact(row.units)}</TableCell><TableCell className="text-right tabular-nums">{exact(row.carts)}</TableCell><TableCell className="text-right tabular-nums">{exact(row.views)}</TableCell></TableRow>)}</TableBody></Table>}</CardContent></Card>
   <Card><CardHeader><CardTitle>Recent activity</CardTitle><CardDescription>Latest events · no personal identifiers · {PROVISIONAL}</CardDescription></CardHeader>
    <CardContent><ol aria-label="Recent activity" className="divide-y">{data.activity.map((row,index)=><li key={index} className="flex items-start justify-between gap-3 py-2 text-sm">
     <div className="min-w-0"><div className="font-medium">{KINDS[row.kind]||row.kind}{row.label?<span className="font-normal text-muted-foreground"> · {row.label}</span>:null}</div>
      <div className="text-xs text-muted-foreground">{SOURCES[row.source]||row.source}{row.device!=='unknown'?' · '+row.device:''} · {clock(row.event_time)} UTC</div></div>
     {row.amount_minor&&row.currency?<span className="shrink-0 tabular-nums">{row.kind==='order_refunded'?'−':''}{formatMoney(row.amount_minor,row.currency)}</span>:null}</li>)}</ol></CardContent></Card>
  </div>
  <details className="provenance"><summary>Live View source</summary><p>{data.provenance.source}</p><p>{data.provenance.source_identity||'Unavailable'}</p>
   <p>{exact(data.provenance.rows)} stored events · offsets {data.provenance.offset_bounds.map(b=>`p${b.partition} ${b.from_offset}–${b.to_offset}`).join(', ')||'none'}</p></details>
 </div>;
}
