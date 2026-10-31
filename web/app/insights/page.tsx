'use client';

import {Bar,BarChart,CartesianGrid,XAxis,YAxis} from 'recharts';
import {Card,CardContent,CardDescription,CardHeader,CardTitle} from '@/components/ui/card';
import {ChartContainer,ChartTooltip,ChartTooltipContent,type ChartConfig} from '@/components/ui/chart';
import {Table,TableBody,TableCell,TableHead,TableHeader,TableRow} from '@/components/ui/table';
import ActivityHeatmap,{type Activity} from '../../components/ActivityHeatmap';
import {ratePercent,useRead} from '../../components/Engagement';
import {BarList,Donut,ExactValues,Legend} from '../../components/charts';
import {compactMoney,exact,formatMoney} from '../../lib/format';

type OrderValues={status:string;message?:string;currency:string|null;orders:string;median_minor:string|null;p90_minor:string|null;bands:{from_minor:string;to_minor:string|null;orders:string;gross_minor:string}[]};
type Loyalty={status:string;currency:string|null;tiers:{tier:string;customers:string;orders:string;net_revenue_minor:string;net_per_customer_minor:string|null}[]};
type Channels={status:string;sources:{source:string;sessions:string;orders:string;conversion_rate:string|null}[];definition?:string};
const tierNames:Record<string,string>={gold:'Gold',silver:'Silver',bronze:'Bronze',none:'No CRM tier'};
const tierAccent:Record<string,string>={gold:'var(--intent)',silver:'var(--chart-4)',bronze:'var(--cat-2)',none:'var(--muted)'};
const sourceColor:Record<string,string>={web:'var(--chart-1)',mobile:'var(--chart-5)',unknown:'var(--chart-4)'};
const sourceNames:Record<string,string>={web:'Web',mobile:'App',unknown:'Unknown'};
const config={orders:{label:'Orders',color:'var(--primary)'}} satisfies ChartConfig;

function State({error,data,children}:{error:string;data:{status:string;message?:string}|null;children:()=>React.ReactNode}){
 if(error)return <p className="empty">Insight unavailable: {error}</p>;
 if(!data)return <p className="empty">Loading…</p>;
 if(data.status==='unavailable')return <p className="empty">{data.message||'Insight unavailable for this certified release.'}</p>;
 return <>{children()}</>;
}

export default function Insights(){
 const activity=useRead<Activity>('insights/activity',b=>Array.isArray(b.cells)&&b.cells.length===168);
 const values=useRead<OrderValues>('insights/order-values',b=>Array.isArray(b.bands));
 const loyalty=useRead<Loyalty>('insights/loyalty',b=>Array.isArray(b.tiers));
 const channels=useRead<Channels>('insights/channels',b=>Array.isArray(b.sources));
 return <main>
  <header className="masthead"><div><h1>Insights</h1><p>Patterns behind the numbers: when customers act, what they spend and who they are</p></div></header>
  <div className="space-y-4">
   <Card><CardHeader><CardTitle>When customers act</CardTitle><CardDescription>Sessions started and orders placed by weekday and hour · UTC · certified history</CardDescription></CardHeader>
    <CardContent><section aria-label="When customers act"><State {...activity}>{()=><ActivityHeatmap data={activity.data!}/>}</State></section></CardContent></Card>
   <div className="grid gap-4 lg:grid-cols-[minmax(0,3fr)_minmax(0,2fr)] [&>*]:min-w-0">
    <Card><CardHeader><CardTitle>Order value distribution</CardTitle><CardDescription>Original order gross amounts · refunds excluded</CardDescription></CardHeader><CardContent>
     <section aria-label="Order value distribution"><State {...values}>{()=>{const v=values.data!,c=v.currency||'VND';
      if(!v.bands.length)return <p className="empty">No orders in this certified release.</p>;
      const label=(b:OrderValues['bands'][number])=>b.to_minor==null?`${formatMoney(b.from_minor,c)} and above`:`${formatMoney(b.from_minor,c)} – ${formatMoney(b.to_minor,c)}`;
      const short=(b:OrderValues['bands'][number])=>b.to_minor==null?`${compactMoney(b.from_minor,c)}+`:compactMoney(b.from_minor,c);
      return <div className="space-y-4">
       <div className="grid grid-cols-3 gap-3">{([['Median order',formatMoney(v.median_minor,c)],['90th percentile',formatMoney(v.p90_minor,c)],['Orders',exact(v.orders)]] as const).map(([name,value])=><div key={name} role="article" aria-label={name} className="rounded-lg border bg-gradient-to-t from-primary/5 to-card p-3"><div className="text-xs text-muted-foreground">{name}</div><div className="mt-1 text-lg font-semibold tabular-nums">{value}</div></div>)}</div>
       <div role="img" aria-label="Orders by order value"><ChartContainer config={config} className="aspect-auto h-[220px] w-full"><BarChart data={v.bands.map(b=>({band:short(b),orders:Number(b.orders),range:label(b)}))} margin={{left:4,right:8}}><CartesianGrid vertical={false}/><XAxis dataKey="band" tickLine={false} axisLine={false} interval={0} fontSize={11}/><YAxis tickLine={false} axisLine={false} width={40}/><ChartTooltip content={<ChartTooltipContent labelKey="range"/>}/><Bar dataKey="orders" fill="var(--color-orders)" radius={[4,4,0,0]} isAnimationActive={false}/></BarChart></ChartContainer></div>
       <details className="provenance"><summary>Exact bands</summary><Table><TableHeader><TableRow><TableHead>Order value</TableHead><TableHead className="text-right">Orders</TableHead><TableHead className="text-right">Gross</TableHead></TableRow></TableHeader><TableBody>{v.bands.map(b=><TableRow key={b.from_minor}><TableCell>{label(b)}</TableCell><TableCell className="text-right tabular-nums">{exact(b.orders)}</TableCell><TableCell className="text-right tabular-nums">{formatMoney(b.gross_minor,c)}</TableCell></TableRow>)}</TableBody></Table></details>
      </div>;}}</State></section></CardContent></Card>
    <Card><CardHeader><CardTitle>Web vs app</CardTitle><CardDescription>Sessions, orders and conversion by where the purchase session started</CardDescription></CardHeader><CardContent>
     <State {...channels}>{()=>{const sources=channels.data!.sources;return <>
      <Legend label="Source legend" items={sources.map(x=>({key:x.source,label:sourceNames[x.source]||x.source,color:sourceColor[x.source]||'var(--chart-4)'}))}/>
      <div className="mt-4 space-y-5">{([['Sessions',(x:Channels['sources'][number])=>Number(x.sessions),(x:Channels['sources'][number])=>exact(x.sessions)],
        ['Orders',(x:Channels['sources'][number])=>Number(x.orders),(x:Channels['sources'][number])=>exact(x.orders)],
        ['Conversion',(x:Channels['sources'][number])=>Number(x.conversion_rate??0),(x:Channels['sources'][number])=>x.conversion_rate==null?'—':ratePercent(x.conversion_rate)]] as const).map(([metric,value,display])=><div key={metric}>
        <p className="mb-2 text-sm font-medium">{metric}</p><BarList name={metric+' by source'} rows={sources.map(x=>({key:x.source,label:sourceNames[x.source]||x.source,value:value(x),display:display(x),color:sourceColor[x.source]||'var(--chart-4)'}))}/></div>)}</div>
      <ExactValues label="Exact web vs app values"><Table aria-label="Web vs app"><TableHeader><TableRow><TableHead>Source</TableHead><TableHead className="text-right">Sessions</TableHead><TableHead className="text-right">Orders</TableHead><TableHead className="text-right">Conversion</TableHead></TableRow></TableHeader>
      <TableBody>{sources.map(s=><TableRow key={s.source}><TableCell className="font-medium">{sourceNames[s.source]||s.source}</TableCell><TableCell className="text-right tabular-nums">{exact(s.sessions)}</TableCell><TableCell className="text-right tabular-nums">{exact(s.orders)}</TableCell><TableCell className="text-right tabular-nums">{s.conversion_rate==null?'—':ratePercent(s.conversion_rate)}</TableCell></TableRow>)}</TableBody></Table></ExactValues>
      <p className="mt-3 text-xs text-muted-foreground">{channels.data!.definition}</p></>;}}</State></CardContent></Card>
   </div>
   <Card><CardHeader><CardTitle>Revenue by loyalty tier</CardTitle><CardDescription>Latest CRM tier as of the certified release · lifetime net revenue</CardDescription></CardHeader><CardContent>
    <section aria-label="Revenue by loyalty tier"><State {...loyalty}>{()=>{const l=loyalty.data!,c=l.currency||'VND';return <div className="grid items-center gap-6 lg:grid-cols-[minmax(0,1fr)_minmax(0,1.6fr)]">
     <Donut name="Net revenue by loyalty tier" total={compactMoney(l.tiers.reduce((sum,t)=>sum+(BigInt(t.net_revenue_minor)>0n?BigInt(t.net_revenue_minor):0n),0n).toString(),c)} caption="net revenue"
      slices={l.tiers.map(t=>({key:t.tier,label:tierNames[t.tier]||t.tier,value:Math.max(0,Number(t.net_revenue_minor)),color:tierAccent[t.tier]||'var(--muted)',display:compactMoney(t.net_revenue_minor,c)}))}/>
     <div className="grid grid-cols-2 gap-3">{l.tiers.map(t=>
     <div key={t.tier} role="article" aria-label={tierNames[t.tier]||t.tier} className="rounded-xl border bg-card p-4">
      <div className="flex items-center gap-1.5 text-xs font-medium text-muted-foreground"><span className="inline-block size-2.5 rounded-[3px]" style={{background:tierAccent[t.tier]}} aria-hidden="true"/>{tierNames[t.tier]||t.tier}</div>
      <div className="mt-1 text-2xl font-semibold tracking-tight tabular-nums" title={formatMoney(t.net_revenue_minor,c)}>{compactMoney(t.net_revenue_minor,c)}</div>
      <div className="mt-1 text-xs text-muted-foreground">{t.customers==='0'?'No customers':`${exact(t.customers)} profiles · ${exact(t.orders)} orders`}</div>
      {t.net_per_customer_minor&&<div className="mt-2 text-xs"><span className="text-muted-foreground">Per profile </span><span className="font-medium tabular-nums">{formatMoney(t.net_per_customer_minor,c)}</span></div>}
     </div>)}</div></div>;}}</State></section></CardContent></Card>
  </div>
 </main>;
}
