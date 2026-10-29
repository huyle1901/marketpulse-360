'use client';

import {Card,CardContent,CardDescription,CardHeader,CardTitle} from '@/components/ui/card';
import {Table,TableBody,TableCell,TableHead,TableHeader,TableRow} from '@/components/ui/table';
import {useRead} from '../../components/Engagement';
import {CITY_COORDINATES} from '../../lib/cities';
import {compactMoney,exact,formatMoney} from '../../lib/format';

type City={country:string;city:string;customers:string;orders:string;net_revenue_minor:string;net_per_customer_minor:string|null};
type Geography={status:string;message?:string;currency:string|null;definition?:string;total_net_revenue_minor:string;cities:City[];unknown:{profiles:string;orders:string;net_revenue_minor:string}};
// Equirectangular frame around Vietnam; latitude stretched by 1/cos(16°) so distances read naturally.
const FRAME={minLat:8,maxLat:23.6,minLon:102,maxLon:110,width:360,height:560};
const project=(lat:number,lon:number)=>({x:(lon-FRAME.minLon)/(FRAME.maxLon-FRAME.minLon)*FRAME.width,y:(FRAME.maxLat-lat)/(FRAME.maxLat-FRAME.minLat)*FRAME.height});
const share=(part:bigint,total:bigint)=>total>0n&&part>=0n?Number(part*1000n/total)/10:0;

export default function GeographyPage(){
 const {data,error}=useRead<Geography>('insights/geography',b=>Array.isArray(b.cities)&&b.unknown);
 const currency=data?.currency||'VND';
 const cities=data?.cities||[];
 const plotted=cities.filter(c=>CITY_COORDINATES[c.city]&&BigInt(c.net_revenue_minor)>0n);
 const skipped=cities.filter(c=>!plotted.includes(c)).map(c=>`${c.city} (${CITY_COORDINATES[c.city]?'net revenue not positive':'no bundled coordinates'})`);
 const max=plotted.reduce((m,c)=>BigInt(c.net_revenue_minor)>m?BigInt(c.net_revenue_minor):m,1n);
 const located=cities.reduce((sum,c)=>sum+(BigInt(c.net_revenue_minor)>0n?BigInt(c.net_revenue_minor):0n),0n);
 return <main>
  <header className="masthead"><div><h1>Geography</h1><p>Where identified customers live and how much net revenue each city brings</p></div></header>
  {error?<p className="empty">Geography unavailable: {error}</p>:!data?<p className="empty">Loading certified customer locations…</p>:data.status==='unavailable'?<p className="empty">{data.message}</p>:
  <div className="grid gap-4 lg:grid-cols-[minmax(0,2fr)_minmax(0,3fr)] [&>*]:min-w-0">
   <Card><CardHeader><CardTitle>Revenue map</CardTitle><CardDescription>Bubble area is proportional to certified net revenue · {currency}</CardDescription></CardHeader><CardContent>
    <div role="img" aria-label="Customer net revenue by city" className="mx-auto max-w-[360px]">
     <svg viewBox={`-20 -10 ${FRAME.width+40} ${FRAME.height+20}`} className="h-auto w-full">
      <defs><radialGradient id="bubble" cx="35%" cy="35%" r="75%"><stop offset="0%" stopColor="var(--primary)" stopOpacity="0.95"/><stop offset="100%" stopColor="var(--primary)" stopOpacity="0.55"/></radialGradient></defs>
      <rect x={0} y={0} width={FRAME.width} height={FRAME.height} rx={14} fill="var(--muted)" opacity="0.6"/>
      {[10,12,14,16,18,20,22].map(lat=><g key={lat}><line x1={0} x2={FRAME.width} y1={project(lat,FRAME.minLon).y} y2={project(lat,FRAME.minLon).y} stroke="var(--border)" strokeDasharray="3 4"/><text x={4} y={project(lat,FRAME.minLon).y-3} fontSize={9} fill="var(--muted-foreground)">{lat}°N</text></g>)}
      {[103,105,107,109].map(lon=><line key={lon} y1={0} y2={FRAME.height} x1={project(FRAME.minLat,lon).x} x2={project(FRAME.minLat,lon).x} stroke="var(--border)" strokeDasharray="3 4"/>)}
      {plotted.map(c=>{const {x,y}=project(CITY_COORDINATES[c.city].lat,CITY_COORDINATES[c.city].lon);const r=8+Math.sqrt(Number(BigInt(c.net_revenue_minor)*10000n/max)/10000)*46;
       return <g key={c.city}><circle data-city={c.city} cx={x} cy={y} r={r} fill="url(#bubble)" stroke="var(--card)" strokeWidth={2}><title>{`${c.city}: ${formatMoney(c.net_revenue_minor,currency)} from ${c.customers} customers`}</title></circle>
        <text x={x+r+6} y={y-2} fontSize={13} fontWeight={600} fill="var(--foreground)">{c.city}</text><text x={x+r+6} y={y+13} fontSize={11} fill="var(--muted-foreground)">{compactMoney(c.net_revenue_minor,currency)}</text></g>;})}
     </svg>
    </div>
    {skipped.length>0&&<p className="mt-2 text-xs text-muted-foreground">Not on the map: {skipped.join(', ')}.</p>}
    <p className="mt-1 text-xs text-muted-foreground">City centres are approximate; no basemap is loaded.</p>
   </CardContent></Card>
   <div className="space-y-4">
    <Card><CardHeader><CardTitle>Net revenue by city</CardTitle><CardDescription>{data.definition}</CardDescription></CardHeader><CardContent>
     <Table aria-label="Net revenue by city"><TableHeader><TableRow><TableHead>City</TableHead><TableHead className="text-right">Customers</TableHead><TableHead className="text-right">Orders</TableHead><TableHead className="text-right">Net revenue</TableHead><TableHead className="w-[28%]">Share of located</TableHead></TableRow></TableHeader>
      <TableBody>{cities.map(c=>{const pct=share(BigInt(c.net_revenue_minor),located);return <TableRow key={c.city}><TableCell><div className="font-medium">{c.city}</div><div className="text-xs text-muted-foreground">{c.country} · per customer {c.net_per_customer_minor==null?'—':formatMoney(c.net_per_customer_minor,currency)}</div></TableCell>
       <TableCell className="text-right tabular-nums">{exact(c.customers)}</TableCell><TableCell className="text-right tabular-nums">{exact(c.orders)}</TableCell><TableCell className="text-right tabular-nums">{formatMoney(c.net_revenue_minor,currency)}</TableCell>
       <TableCell><div className="flex items-center gap-2"><div className="h-2 flex-1 overflow-hidden rounded-full bg-muted"><div className="h-full rounded-full bg-primary" style={{width:pct+'%'}}/></div><span className="w-11 text-right text-xs tabular-nums text-muted-foreground">{BigInt(c.net_revenue_minor)>0n?pct.toFixed(1)+'%':'—'}</span></div></TableCell></TableRow>;})}</TableBody></Table>
    </CardContent></Card>
    <div role="article" aria-label="Location unknown" className="rounded-xl border bg-card p-4 shadow-[var(--shadow-card)]">
     <div className="text-xs font-medium text-muted-foreground">Location unknown</div>
     <div className="mt-1 text-2xl font-semibold tabular-nums" title={formatMoney(data.unknown.net_revenue_minor,currency)}>{formatMoney(data.unknown.net_revenue_minor,currency)}</div>
     <div className="mt-1 text-xs text-muted-foreground">{exact(data.unknown.profiles)} profiles · {exact(data.unknown.orders)} orders · anonymous or without a CRM record, never assigned a city</div>
    </div>
   </div>
  </div>}
 </main>;
}
