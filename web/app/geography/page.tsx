'use client';

import {Card,CardContent,CardDescription,CardHeader,CardTitle} from '@/components/ui/card';
import {Table,TableBody,TableCell,TableHead,TableHeader,TableRow} from '@/components/ui/table';
import {useRead} from '../../components/Engagement';
import {useState} from 'react';
import {BarList,ExactValues,Legend} from '../../components/charts';
import {StatStrip} from '../../components/StatStrip';
import {SizeLegend,VietnamMap} from '../../components/VietnamMap';
import {CITY_COORDINATES,CITY_REGION} from '../../lib/cities';
import {compactMoney,exact,formatMoney} from '../../lib/format';

type City={country:string;city:string;customers:string;orders:string;net_revenue_minor:string;net_per_customer_minor:string|null};
type Geography={status:string;message?:string;currency:string|null;definition?:string;total_net_revenue_minor:string;cities:City[];unknown:{profiles:string;orders:string;net_revenue_minor:string}};
const share=(part:bigint,total:bigint)=>total>0n&&part>=0n?Number(part*1000n/total)/10:0;

type Measure='net'|'customers'|'orders';
const MEASURES:Record<Measure,string>={net:'net revenue',customers:'customers',orders:'orders'};
const REGIONS=[['North','var(--cat-1)'],['Central','var(--cat-2)'],['South','var(--cat-3)'],['Other','var(--chart-4)']] as const;
const big=(value:string)=>BigInt(value);

export default function GeographyPage(){
 const {data,error}=useRead<Geography>('insights/geography',b=>Array.isArray(b.cities)&&b.unknown);
 const [measure,setMeasure]=useState<Measure>('net');const [active,setActive]=useState<string|null>(null);
 const currency=data?.currency||'VND';
 const cities=data?.cities||[];
 const raw=(c:City)=>measure==='net'?big(c.net_revenue_minor):measure==='customers'?big(c.customers):big(c.orders);
 const show=(c:City)=>measure==='net'?formatMoney(c.net_revenue_minor,currency):exact(measure==='customers'?c.customers:c.orders);
 const plotted=cities.filter(c=>CITY_COORDINATES[c.city]&&raw(c)>0n);
 const skipped=cities.filter(c=>!CITY_COORDINATES[c.city]||big(c.net_revenue_minor)<=0n).map(c=>`${c.city} (${CITY_COORDINATES[c.city]?'net revenue not positive':'no bundled coordinates'})`);
 const located=cities.reduce((sum,c)=>sum+(big(c.net_revenue_minor)>0n?big(c.net_revenue_minor):0n),0n);
 const unknownNet=data?.unknown?big(data.unknown.net_revenue_minor):0n,known=located+(unknownNet>0n?unknownNet:0n);
 const ranked=[...cities].sort((a,b)=>{const x=raw(a),y=raw(b);return x===y?a.city.localeCompare(b.city):x>y?-1:1;});
 const top=[...cities].sort((a,b)=>big(b.net_revenue_minor)>big(a.net_revenue_minor)?1:-1)[0];
 const perCustomer=cities.filter(c=>c.net_per_customer_minor!=null).sort((a,b)=>big(b.net_per_customer_minor!)>big(a.net_per_customer_minor!)?1:-1)[0];
 const regions=REGIONS.map(([region,color])=>({region,color,value:cities.filter(c=>(CITY_REGION[c.city]??'Other')===region).reduce((sum,c)=>sum+(big(c.net_revenue_minor)>0n?big(c.net_revenue_minor):0n),0n)})).filter(r=>r.value>0n);
 const max=plotted.reduce((m,c)=>Math.max(m,Number(raw(c))),1);
 const compact=(value:number)=>measure==='net'?compactMoney(String(Math.round(value)),currency):Math.round(value).toLocaleString('en-US');
 return <main>
  <header className="masthead"><div><h1>Geography</h1><p>Where identified customers live and how much each city brings</p></div></header>
  {error?<p className="empty">Geography unavailable: {error}</p>:!data?<p className="empty">Loading certified customer locations…</p>:data.status==='unavailable'?<p className="empty">{data.message}</p>:<div className="space-y-4">
  <StatStrip label="Geography summary" className="md:grid-cols-4" items={[
   {name:'Located net revenue',value:compactMoney(located.toString(),currency),hint:`${share(located,known).toFixed(1)}% of net revenue has a known city`},
   {name:'Top city',value:top?.city??'—',hint:top?`${compactMoney(top.net_revenue_minor,currency)} · ${share(big(top.net_revenue_minor),located).toFixed(1)}% of located revenue`:'No city'},
   {name:'Cities on the map',value:String(plotted.length),hint:`${exact(String(cities.reduce((s,c)=>s+Number(c.customers),0)))} identified customers`},
   {name:'Highest net per customer',value:perCustomer?formatMoney(perCustomer.net_per_customer_minor,currency):'—',hint:perCustomer?.city??'No customers'}]}/>
  <div className="grid gap-4 lg:grid-cols-[minmax(0,7fr)_minmax(0,5fr)] [&>*]:min-w-0">
   <section aria-label="Revenue map" className="feature-band rounded-[20px] p-5 md:p-6">
    <div className="flex flex-wrap items-start justify-between gap-3"><div><h2 className="text-base font-medium text-foreground">Where customers are</h2><p className="mt-0.5 text-sm text-muted-foreground">Circle area is proportional to {MEASURES[measure]} · {currency}</p></div>
     <div className="metric-switch" role="group" aria-label="Map measure">{(Object.keys(MEASURES) as Measure[]).map(m=><button key={m} aria-pressed={measure===m} onClick={()=>setMeasure(m)}>{m==='net'?'Net revenue':m==='customers'?'Customers':'Orders'}</button>)}</div></div>
    <div className="mt-2"><VietnamMap name={`Customer ${MEASURES[measure]} by city`} active={active} onActive={setActive}
     points={plotted.map(c=>({city:c.city,value:Number(raw(c)),display:measure==='net'?compactMoney(c.net_revenue_minor,currency):show(c),title:`${c.city}: ${formatMoney(c.net_revenue_minor,currency)} net · ${exact(c.customers)} customers · ${exact(c.orders)} orders`}))}/></div>
    <div className="mt-3 flex flex-wrap items-end justify-between gap-3"><SizeLegend max={max} format={compact}/>
     <p className="text-xs text-muted-foreground">City centres are approximate · Natural Earth outline</p></div>
    {skipped.length>0&&<p className="mt-2 text-xs text-muted-foreground">Not on the map: {skipped.join(', ')}.</p>}
   </section>
   <div className="space-y-4">
    <Card><CardHeader><CardTitle>Cities by {MEASURES[measure]}</CardTitle><CardDescription>{data.definition}</CardDescription></CardHeader><CardContent>
     <BarList name={`Cities ranked by ${MEASURES[measure]}`} onActive={setActive} rows={ranked.map((c,i)=>({key:c.city,label:<span className={active&&active!==c.city?'opacity-60':''}><span className="figure mr-2 text-muted-foreground">{i+1}</span>{c.city}</span>,value:Number(raw(c)),display:show(c),
      sub:`${exact(c.customers)} customers · ${exact(c.orders)} orders · ${c.net_per_customer_minor==null?'—':formatMoney(c.net_per_customer_minor,currency)} per customer`}))}/>
     <ExactValues label="Exact city values">
      <Table aria-label="Net revenue by city"><TableHeader><TableRow><TableHead>City</TableHead><TableHead className="text-right">Customers</TableHead><TableHead className="text-right">Orders</TableHead><TableHead className="text-right">Net revenue</TableHead><TableHead className="text-right">Share</TableHead></TableRow></TableHeader>
       <TableBody>{cities.map(c=><TableRow key={c.city}><TableCell><div className="font-medium">{c.city}</div><div className="text-xs text-muted-foreground">{CITY_REGION[c.city]??'Other'} · per customer {c.net_per_customer_minor==null?'—':formatMoney(c.net_per_customer_minor,currency)}</div></TableCell>
        <TableCell className="text-right tabular-nums">{exact(c.customers)}</TableCell><TableCell className="text-right tabular-nums">{exact(c.orders)}</TableCell><TableCell className="text-right tabular-nums">{formatMoney(c.net_revenue_minor,currency)}</TableCell><TableCell className="text-right tabular-nums">{big(c.net_revenue_minor)>0n?share(big(c.net_revenue_minor),located).toFixed(1)+'%':'—'}</TableCell></TableRow>)}</TableBody></Table>
     </ExactValues>
    </CardContent></Card>
   <Card><CardHeader><CardTitle>Net revenue by region</CardTitle><CardDescription>Located net revenue across Viet Nam's North, Central and South</CardDescription></CardHeader><CardContent>
    <section aria-label="Net revenue by region" className="space-y-3">
     <div className="flex h-3 gap-[2px] overflow-hidden rounded-full bg-muted" aria-hidden="true">{regions.map(r=><span key={r.region} style={{flexGrow:Number(r.value),background:r.color}}/>)}</div>
     <Legend label="Region legend" items={regions.map(r=>({key:r.region,label:r.region,color:r.color,value:`${compactMoney(r.value.toString(),currency)} · ${share(r.value,located).toFixed(1)}%`}))}/>
    </section>
   </CardContent></Card>
   <Card role="article" aria-label="Location unknown"><CardHeader><CardTitle>Location unknown</CardTitle><CardDescription>{exact(data.unknown.profiles)} profiles · {exact(data.unknown.orders)} orders · {formatMoney(data.unknown.net_revenue_minor,currency)} net · anonymous or without a CRM record, never assigned a city</CardDescription></CardHeader><CardContent>
    <section aria-label="Location coverage" className="space-y-3">
     <div className="flex h-3 gap-[2px] overflow-hidden rounded-full bg-muted" aria-hidden="true"><span style={{flexGrow:Number(located),background:'var(--primary)'}}/><span style={{flexGrow:Number(unknownNet>0n?unknownNet:0n),background:'var(--chart-4)'}}/></div>
     <Legend label="Coverage legend" items={[{key:'known',label:'Known city',color:'var(--primary)',value:`${share(located,known).toFixed(1)}%`},{key:'unknown',label:'Location unknown',color:'var(--chart-4)',value:`${share(unknownNet>0n?unknownNet:0n,known).toFixed(1)}%`}]}/>
    </section>
   </CardContent></Card>
   </div>
  </div>
  </div>}
 </main>;
}
