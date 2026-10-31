'use client';

import {useEffect,useState} from 'react';
import {Card,CardContent,CardDescription,CardHeader,CardTitle} from '@/components/ui/card';
import {StatStrip} from '../../components/StatStrip';
import {BarList,ExactValues,Legend} from '../../components/charts';
import {CampaignScatter} from '../../components/CampaignScatter';
import {Table,TableBody,TableCell,TableHead,TableHeader,TableRow} from '@/components/ui/table';
import {exact,formatMoney} from '../../lib/format';
import {multiple,percent,perUnit} from '../../lib/metrics';
import {useUrlState,choice} from '../../lib/useUrlState';
import type {Dashboard,Metric} from '../types';

const channelNames:Record<string,string>={google:'Google',meta:'Meta',tiktok:'TikTok'};
const channelColors:Record<string,string>={google:'var(--chart-1)',meta:'var(--chart-2)',tiktok:'var(--chart-3)'};
type Sums={spend:bigint;net:bigint;gross:bigint;orders:bigint;clicks:bigint;impressions:bigint};
const zero=():Sums=>({spend:0n,net:0n,gross:0n,orders:0n,clicks:0n,impressions:0n});
const add=(sum:Sums,row:Metric)=>{sum.spend+=BigInt(row.spend_minor);sum.net+=BigInt(row.net_revenue_minor);sum.gross+=BigInt(row.gross_revenue_minor??'0');sum.orders+=BigInt(row.orders);sum.clicks+=BigInt(row.clicks??'0');sum.impressions+=BigInt(row.impressions??'0');return sum;};
/** Exact share of a positive total as a one-decimal percent, truncated. */
const share=(part:bigint,total:bigint)=>{if(total<=0n||part<0n)return null;const tenths=part*1000n/total;return (tenths/10n).toString()+'.'+(tenths%10n).toString()+'%';};

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
   return <div key={currency} className="min-w-0 space-y-4">
    <StatStrip label={'Ad efficiency · '+currency} className="xl:grid-cols-6" items={kpis.map(([name,value,hint])=>({name,value,hint}))}/>
    <div className="grid gap-4 xl:grid-cols-2 [&>*]:min-w-0">
     <Card className="min-w-0"><CardHeader><CardTitle>Spend share vs revenue share</CardTitle><CardDescription>A channel earning a bigger share of revenue than of spend is outperforming · {currency}</CardDescription></CardHeader><CardContent>
      <Legend label="Share legend" items={[{key:'spend',label:'Ad spend share',color:'var(--chart-4)'},{key:'net',label:'Net revenue share',color:'var(--primary)'}]}/>
      <section aria-label={'Spend share vs revenue share · '+currency} className="mt-4 space-y-4">{channels.map(([c,sums])=>{const spend=share(sums.spend,total.spend),net=total.net>0n&&sums.net>=0n?share(sums.net,total.net):null;const name=channelNames[c]||c;
       return <div key={c} role="img" aria-label={`${name}: ${spend??'unavailable'} of ad spend, ${net??'unavailable'} of net revenue`} className="grid grid-cols-[72px_minmax(0,1fr)] items-center gap-3 text-sm"><span className="flex items-center gap-1.5 font-medium"><span className="inline-block size-2.5 rounded-[3px]" style={{background:channelColors[c]}} aria-hidden="true"/>{name}</span><div className="space-y-1.5">
        {([['Ad spend',spend,'var(--chart-4)'],['Net revenue',net,'var(--primary)']] as const).map(([label,value,color])=><div key={label} className="flex items-center gap-2"><div className="h-2.5 flex-1 overflow-hidden rounded-full bg-muted"><div className="h-full rounded-full" style={{width:value??'0%',background:color}}/></div><span className="w-28 shrink-0 text-right text-xs tabular-nums text-muted-foreground">{value??'—'} {label.toLowerCase()}</span></div>)}
       </div></div>;})}</section></CardContent></Card>
     <Card className="min-w-0"><CardHeader><CardTitle>Efficiency by channel</CardTitle><CardDescription>Which platform buys attention and orders most cheaply · {currency}</CardDescription></CardHeader><CardContent>
      <Legend label="Channel legend" items={channels.map(([c])=>({key:c,label:channelNames[c]||c,color:channelColors[c]}))}/>
      <div className="mt-4 grid gap-x-6 gap-y-5 sm:grid-cols-2">{([
        ['Click-through rate','higher is better',(s:Sums)=>s.impressions>0n?Number(s.clicks)/Number(s.impressions):0,(s:Sums)=>percent(t(s.clicks),t(s.impressions))??'—'],
        ['Net ROAS','higher is better',(s:Sums)=>s.spend>0n?Number(s.net)/Number(s.spend):0,(s:Sums)=>multiple(t(s.net),t(s.spend))??'—'],
        ['Cost per click','lower is better',(s:Sums)=>s.clicks>0n?Number(s.spend)/Number(s.clicks):0,(s:Sums)=>money(perUnit(t(s.spend),t(s.clicks)))],
        ['Cost per order','lower is better',(s:Sums)=>s.orders>0n?Number(s.spend)/Number(s.orders):0,(s:Sums)=>money(perUnit(t(s.spend),t(s.orders)))],
       ] as const).map(([metric,direction,value,display])=><div key={metric} className="min-w-0">
        <p className="mb-2 text-sm font-medium">{metric} <span className="font-normal text-xs text-muted-foreground">· {direction}</span></p>
        <BarList name={metric+' by channel · '+currency} rows={channels.map(([c,sums])=>({key:c,label:channelNames[c]||c,value:value(sums),display:display(sums),color:channelColors[c]}))}/></div>)}</div>
      <ExactValues label="Exact channel values"><Table aria-label={'Efficiency by channel · '+currency}><TableHeader><TableRow><TableHead>Channel</TableHead>{header.map(h=><TableHead key={h} className="text-right">{h}</TableHead>)}</TableRow></TableHeader><TableBody>{channels.map(([c,s])=><TableRow key={c}><Efficiency label={channelNames[c]||c} sums={s} currency={currency}/></TableRow>)}</TableBody></Table></ExactValues>
     </CardContent></Card>
    </div>
    <Card className="min-w-0"><CardHeader><CardTitle>Campaigns: spend vs return</CardTitle><CardDescription>Each bubble is a campaign; further right spends more, higher returns more net revenue per unit of spend, bigger has more orders · {currency}</CardDescription></CardHeader><CardContent>
     <CampaignScatter rows={rows} currency={currency}/>
     <ExactValues label="Exact campaign values"><Table aria-label={'Efficiency by campaign · '+currency}><TableHeader><TableRow><TableHead>Campaign</TableHead>{header.map(h=><TableHead key={h} className="text-right">{h}</TableHead>)}</TableRow></TableHeader><TableBody>{[...rows].sort((a,b)=>{const x=BigInt(a.spend_minor),y=BigInt(b.spend_minor);return x===y?(a.campaign_name||'').localeCompare(b.campaign_name||''):x>y?-1:1;}).map(r=><TableRow key={r.campaign_id}><Efficiency label={r.campaign_name||r.campaign_id} sums={add(zero(),r)} currency={currency}/></TableRow>)}</TableBody></Table></ExactValues>
    </CardContent></Card>
   </div>;
  })}
  {undelivered.length>0&&<p className="source">Unattributed revenue has no ad delivery, so it is excluded from efficiency metrics; it remains in Overview totals.</p>}
  {report&&<details className="provenance"><summary>Data details</summary><p>{report.certified.provenance.model} · generation {report.certified.provenance.generation}</p><p>Release {report.certified.provenance.identity}</p><p>Window {report.filters.from_date} → {report.filters.to_date} (UTC, end exclusive)</p></details>}
 </main>;
}
