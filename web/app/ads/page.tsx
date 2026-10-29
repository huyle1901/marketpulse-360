'use client';

import {useEffect,useState} from 'react';
import {Bar,BarChart,CartesianGrid,XAxis,YAxis} from 'recharts';
import {Card,CardContent,CardDescription,CardHeader,CardTitle} from '@/components/ui/card';
import {ChartContainer,ChartLegend,ChartLegendContent,ChartTooltip,ChartTooltipContent,type ChartConfig} from '@/components/ui/chart';
import {Table,TableBody,TableCell,TableHead,TableHeader,TableRow} from '@/components/ui/table';
import {exact,formatMoney} from '../../lib/format';
import {multiple,percent,perUnit} from '../../lib/metrics';
import {useUrlState,choice} from '../../lib/useUrlState';
import type {Dashboard,Metric} from '../types';

const channelNames:Record<string,string>={google:'Google',meta:'Meta',tiktok:'TikTok'};
type Sums={spend:bigint;net:bigint;gross:bigint;orders:bigint;clicks:bigint;impressions:bigint};
const zero=():Sums=>({spend:0n,net:0n,gross:0n,orders:0n,clicks:0n,impressions:0n});
const add=(sum:Sums,row:Metric)=>{sum.spend+=BigInt(row.spend_minor);sum.net+=BigInt(row.net_revenue_minor);sum.gross+=BigInt(row.gross_revenue_minor??'0');sum.orders+=BigInt(row.orders);sum.clicks+=BigInt(row.clicks??'0');sum.impressions+=BigInt(row.impressions??'0');return sum;};
const config={spend:{label:'Ad spend',color:'var(--chart-5)'},net:{label:'Net revenue',color:'var(--chart-1)'}} satisfies ChartConfig;

function Efficiency({label,sums,currency}:{label:string;sums:Sums;currency:string}){
 const s=(v:bigint)=>v.toString(),money=(v:string|null)=>v==null?'—':formatMoney(v,currency);
 return <><TableCell className="font-medium">{label}</TableCell><TableCell className="text-right tabular-nums">{money(s(sums.spend))}</TableCell>
  <TableCell className="text-right tabular-nums">{exact(s(sums.impressions))}</TableCell><TableCell className="text-right tabular-nums">{exact(s(sums.clicks))}</TableCell>
  <TableCell className="text-right tabular-nums">{percent(s(sums.clicks),s(sums.impressions))??'—'}</TableCell><TableCell className="text-right tabular-nums">{money(perUnit(s(sums.spend),s(sums.clicks)))}</TableCell>
  <TableCell className="text-right tabular-nums">{exact(s(sums.orders))}</TableCell><TableCell className="text-right tabular-nums">{money(perUnit(s(sums.spend),s(sums.orders)))}</TableCell>
  <TableCell className="text-right tabular-nums">{money(s(sums.net))}</TableCell><TableCell className="text-right tabular-nums">{multiple(s(sums.net),s(sums.spend))??'—'}</TableCell></>;
}
const header=['Spend','Impressions','Clicks','CTR','CPC','Orders','Cost per order','Net revenue','Net ROAS'];

export default function AdPerformance(){
 const [filters,setFilters,replaceFilters]=useUrlState({from_date:'',to_date:'',platform:''},v=>({...v,platform:choice(v.platform,['google','meta','tiktok'])}));
 const [report,setReport]=useState<Dashboard|null>(null);const [error,setError]=useState('');
 const query=new URLSearchParams(Object.entries(filters).filter(([,v])=>!!v)).toString();
 useEffect(()=>{
  const controller=new AbortController();
  void fetch('/api/v1/dashboard?'+query,{cache:'no-store',signal:controller.signal}).then(async r=>{const body=await r.json();if(!r.ok)throw new Error(body.message||body.code||'Ad performance unavailable');return body as Dashboard;})
   .then(data=>{setReport(data);setError('');replaceFilters(c=>c.from_date&&c.to_date?c:{...c,from_date:data.filters.from_date,to_date:data.filters.to_date});})
   .catch(e=>{if(!controller.signal.aborted)setError(e instanceof Error?e.message:'Ad performance unavailable');});
  return()=>controller.abort();
 },[query]);
 const delivered=(report?.certified.rows||[]).filter(r=>r.platform);
 const undelivered=(report?.certified.rows||[]).filter(r=>!r.platform);
 const currencies=[...new Set(delivered.map(r=>r.currency))];
 return <main>
  <header className="masthead"><div><h1>Ad performance</h1><p>How efficiently paid media turns impressions into clicks, orders and revenue</p></div></header>
  <section className="filters" aria-label="Ad performance filters">
   <label>From (UTC)<input type="date" value={filters.from_date} onChange={e=>setFilters({...filters,from_date:e.target.value})}/></label>
   <label>To (exclusive UTC)<input type="date" value={filters.to_date} onChange={e=>setFilters({...filters,to_date:e.target.value})}/></label>
   <label>Platform<select value={filters.platform} onChange={e=>setFilters({...filters,platform:e.target.value})}><option value="">All platforms</option>{Object.entries(channelNames).map(([v,l])=><option key={v} value={v}>{l}</option>)}</select></label>
  </section>
  {error&&<div className="error" role="alert"><strong>Ad performance unavailable</strong><p>{error}</p></div>}
  {!report&&!error&&<p className="empty">Loading certified ad performance…</p>}
  {report&&!currencies.length&&<p className="empty">No paid campaign delivery for this selection.</p>}
  {currencies.map(currency=>{
   const rows=delivered.filter(r=>r.currency===currency),total=rows.reduce(add,zero());
   const byChannel=new Map<string,Sums>();for(const r of rows)byChannel.set(r.platform!,add(byChannel.get(r.platform!)||zero(),r));
   const channels=[...byChannel.entries()].sort(([a],[b])=>a.localeCompare(b));
   const t=(v:bigint)=>v.toString(),money=(v:string|null)=>v==null?'Unavailable':formatMoney(v,currency);
   const kpis:[string,string,string][]=[['Impressions',exact(t(total.impressions)),'Times ads were shown'],['Clicks',exact(t(total.clicks)),'Paid clicks to the site'],
    ['Click-through rate',percent(t(total.clicks),t(total.impressions))??'Unavailable','Clicks ÷ impressions'],['Cost per click',money(perUnit(t(total.spend),t(total.clicks))),'Ad spend ÷ clicks'],
    ['Cost per order',money(perUnit(t(total.spend),t(total.orders))),'Ad spend ÷ orders'],['Net ROAS',multiple(t(total.net),t(total.spend))??'Unavailable','Net revenue ÷ ad spend']];
   const chart=channels.map(([c,s])=>({channel:channelNames[c]||c,spend:Number(s.spend),net:Number(s.net)}));
   return <div key={currency} className="min-w-0 space-y-4">
    <section aria-label={'Ad efficiency · '+currency} className="grid grid-cols-2 gap-3 md:grid-cols-3 xl:grid-cols-6">{kpis.map(([name,value,hint])=><Card key={name} role="article" aria-label={name} className="gap-1 py-3"><CardHeader className="px-3.5"><CardTitle className="text-xs font-medium text-muted-foreground">{name}</CardTitle></CardHeader><CardContent className="px-3.5"><div className="text-2xl font-semibold tracking-tight tabular-nums">{value}</div><p className="mt-1 text-xs text-muted-foreground">{hint}</p></CardContent></Card>)}</section>
    <div className="grid gap-4 lg:grid-cols-[minmax(0,2fr)_minmax(0,3fr)] [&>*]:min-w-0">
     <Card><CardHeader><CardTitle>Spend vs revenue by channel</CardTitle><CardDescription>Certified source campaign finance · {currency}</CardDescription></CardHeader><CardContent>
      <div role="img" aria-label={'Ad spend and net revenue by channel · '+currency}><ChartContainer config={config} className="aspect-auto h-[240px] w-full"><BarChart data={chart} margin={{left:4,right:8}}><CartesianGrid vertical={false}/><XAxis dataKey="channel" tickLine={false} axisLine={false}/><YAxis tickLine={false} axisLine={false} width={60} tickFormatter={v=>new Intl.NumberFormat('en-US',{notation:'compact'}).format(Number(v))}/><ChartTooltip content={<ChartTooltipContent/>}/><ChartLegend content={<ChartLegendContent/>}/><Bar dataKey="spend" fill="var(--color-spend)" radius={4} isAnimationActive={false}/><Bar dataKey="net" fill="var(--color-net)" radius={4} isAnimationActive={false}/></BarChart></ChartContainer></div>
      <p className="mt-2 text-xs text-muted-foreground">Bars are proportional; exact amounts are in the tables.</p></CardContent></Card>
     <Card><CardHeader><CardTitle>Efficiency by channel</CardTitle><CardDescription>Which platform buys attention and orders most cheaply</CardDescription></CardHeader><CardContent><Table aria-label={'Efficiency by channel · '+currency}><TableHeader><TableRow><TableHead>Channel</TableHead>{header.map(h=><TableHead key={h} className="text-right">{h}</TableHead>)}</TableRow></TableHeader><TableBody>{channels.map(([c,s])=><TableRow key={c}><Efficiency label={channelNames[c]||c} sums={s} currency={currency}/></TableRow>)}</TableBody></Table></CardContent></Card>
    </div>
    <Card className="min-w-0"><CardHeader><CardTitle>Efficiency by campaign</CardTitle><CardDescription>Sorted by ad spend · open a campaign on the Overview for budgets and alerts</CardDescription></CardHeader><CardContent><Table aria-label={'Efficiency by campaign · '+currency}><TableHeader><TableRow><TableHead>Campaign</TableHead>{header.map(h=><TableHead key={h} className="text-right">{h}</TableHead>)}</TableRow></TableHeader><TableBody>{[...rows].sort((a,b)=>{const x=BigInt(a.spend_minor),y=BigInt(b.spend_minor);return x===y?(a.campaign_name||'').localeCompare(b.campaign_name||''):x>y?-1:1;}).map(r=><TableRow key={r.campaign_id}><Efficiency label={r.campaign_name||r.campaign_id} sums={add(zero(),r)} currency={currency}/></TableRow>)}</TableBody></Table></CardContent></Card>
   </div>;
  })}
  {undelivered.length>0&&<p className="source">Unattributed revenue has no ad delivery, so it is excluded from efficiency metrics; it remains in Overview totals.</p>}
  {report&&<details className="provenance"><summary>Data details</summary><p>{report.certified.provenance.model} · generation {report.certified.provenance.generation}</p><p>Release {report.certified.provenance.identity}</p><p>Window {report.filters.from_date} → {report.filters.to_date} (UTC, end exclusive)</p></details>}
 </main>;
}
