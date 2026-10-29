'use client';

import {ResponsiveContainer,Treemap} from 'recharts';
import {Card,CardContent,CardDescription,CardHeader,CardTitle} from '@/components/ui/card';
import {Table,TableBody,TableCell,TableHead,TableHeader,TableRow} from '@/components/ui/table';
import {ratePercent,useRead} from '../../components/Engagement';
import {compactMoney,exact,formatMoney} from '../../lib/format';

type Money={units:string;gross_revenue_minor:string;net_revenue_minor:string};
type Products={status:string;message?:string;currency:string|null;definition?:string;
 categories:(Money&{category:string;brands:(Money&{brand:string})[]})[];
 top_products:{product_id:string;product_name:string;category:string;brand:string;units:string;orders:string;net_revenue_minor:string}[];
 devices:(Money&{device_type:string;gross_share:string|null;avg_item_value_minor:string|null})[]};
/** Fixed identity colors from the validated categorical order (color follows the category, never its rank). */
const categoryColor:Record<string,string>={electronics:'#2a78d6',fashion:'#eb6834',home:'#1baf7a',sports:'#eda100',beauty:'#e87ba4'};
const title=(value:string)=>value.charAt(0).toUpperCase()+value.slice(1);
const deviceNames:Record<string,string>={desktop:'Desktop',mobile:'Mobile',tablet:'Tablet',unknown:'Unknown device'};

type Cell={x:number;y:number;width:number;height:number;depth:number;name:string;category?:string;root?:{name:string}};
function Tile(props:Cell){
 const {x,y,width,height,depth,name}=props;if(depth===0||width<=0||height<=0)return null;
 const category=depth===1?name:props.category||'',color=categoryColor[category]||'var(--chart-4)';
 return <g><rect x={x} y={y} width={width} height={height} rx={6} fill={color} fillOpacity={depth===1?0.18:0.92} stroke="var(--card)" strokeWidth={2}/>
  {depth===2&&width>56&&height>26&&<text x={x+8} y={y+18} fill="#ffffff" fontSize={12} fontWeight={600}>{name}</text>}
  {depth===1&&width>70&&<text x={x+8} y={y+height-8} fill="var(--foreground)" fontSize={11} fontWeight={700}>{title(name)}</text>}</g>;
}

export default function ProductsPage(){
 const {data,error}=useRead<Products>('insights/products',b=>Array.isArray(b.categories)&&Array.isArray(b.top_products)&&Array.isArray(b.devices));
 const currency=data?.currency||'VND';
 const positive=(data?.categories||[]).filter(c=>BigInt(c.net_revenue_minor)>0n);
 const negative=(data?.categories||[]).filter(c=>BigInt(c.net_revenue_minor)<=0n);
 const map=positive.map(c=>({name:c.category,children:c.brands.filter(b=>BigInt(b.net_revenue_minor)>0n).map(b=>({name:b.brand,category:c.category,size:Number(b.net_revenue_minor)}))}));
 return <main>
  <header className="masthead"><div><h1>Products</h1><p>Which categories, brands and products earn the revenue, and on which devices customers buy</p></div></header>
  {error?<p className="empty">Products unavailable: {error}</p>:!data?<p className="empty">Loading certified product commerce…</p>:data.status==='unavailable'?<p className="empty">{data.message}</p>:<div className="space-y-4">
   <Card className="min-w-0"><CardHeader><CardTitle>Net revenue by category and brand</CardTitle><CardDescription>Area is proportional to certified net revenue · {currency} · whole certified history</CardDescription></CardHeader><CardContent>
    <div role="img" aria-label="Net revenue by category and brand" className="h-[340px] w-full">
     <ResponsiveContainer width="100%" height="100%"><Treemap data={map} dataKey="size" isAnimationActive={false} content={<Tile x={0} y={0} width={0} height={0} depth={0} name=""/>}/></ResponsiveContainer>
     <div className="sr-only">{positive.map(c=>`${title(c.category)} ${formatMoney(c.net_revenue_minor,currency)}`).join(', ')}</div>
    </div>
    <div className="mt-3 flex flex-wrap gap-3 text-xs" aria-hidden="true">{positive.map(c=><span key={c.category} className="inline-flex items-center gap-1.5"><span className="size-2.5 rounded-sm" style={{background:categoryColor[c.category]||'var(--chart-4)'}}/>{title(c.category)}</span>)}</div>
    {negative.length>0&&<p className="mt-2 text-xs text-muted-foreground">Categories with negative net revenue are listed below the map; area cannot show a negative amount.</p>}
    <details className="provenance mt-2" open={negative.length>0}><summary>Exact category values</summary>
     <Table aria-label="Net revenue by category"><TableHeader><TableRow><TableHead>Category</TableHead><TableHead className="text-right">Units</TableHead><TableHead className="text-right">Gross</TableHead><TableHead className="text-right">Net</TableHead><TableHead>Top brand</TableHead></TableRow></TableHeader>
      <TableBody>{data.categories.map(c=><TableRow key={c.category}><TableCell className="font-medium">{title(c.category)}</TableCell><TableCell className="text-right tabular-nums">{exact(c.units)}</TableCell><TableCell className="text-right tabular-nums">{formatMoney(c.gross_revenue_minor,currency)}</TableCell><TableCell className="text-right tabular-nums">{formatMoney(c.net_revenue_minor,currency)}</TableCell><TableCell>{c.brands[0]?.brand??'—'}</TableCell></TableRow>)}</TableBody></Table></details>
   </CardContent></Card>
   <div className="grid gap-4 lg:grid-cols-[minmax(0,3fr)_minmax(0,2fr)] [&>*]:min-w-0">
    <Card><CardHeader><CardTitle>Top products</CardTitle><CardDescription>Ten highest net revenue products · orders are original orders containing the product</CardDescription></CardHeader><CardContent>
     <Table aria-label="Top products by net revenue"><TableHeader><TableRow><TableHead>Product</TableHead><TableHead className="text-right">Units</TableHead><TableHead className="text-right">Orders</TableHead><TableHead className="text-right">Net revenue</TableHead></TableRow></TableHeader>
      <TableBody>{data.top_products.map((p,i)=><TableRow key={p.product_id}><TableCell><div className="flex items-center gap-2"><span className="w-5 text-xs tabular-nums text-muted-foreground">{i+1}</span><span className="size-2 rounded-full" style={{background:categoryColor[p.category]||'var(--chart-4)'}} aria-hidden="true"/><span className="font-medium">{p.product_name}</span></div><div className="ml-7 text-xs text-muted-foreground">{p.brand} · {title(p.category)}</div></TableCell><TableCell className="text-right tabular-nums">{exact(p.units)}</TableCell><TableCell className="text-right tabular-nums">{exact(p.orders)}</TableCell><TableCell className="text-right tabular-nums" title={formatMoney(p.net_revenue_minor,currency)}>{compactMoney(p.net_revenue_minor,currency)}</TableCell></TableRow>)}</TableBody></Table>
    </CardContent></Card>
    <Card><CardHeader><CardTitle>Revenue by purchase device</CardTitle><CardDescription>Share of certified gross by the device of the purchase</CardDescription></CardHeader><CardContent>
     <section aria-label="Revenue by purchase device" className="space-y-3">{data.devices.map(d=><div key={d.device_type} role="article" aria-label={deviceNames[d.device_type]||d.device_type} className="rounded-lg border p-3">
      <div className="flex items-baseline justify-between gap-2"><span className="text-sm font-medium">{deviceNames[d.device_type]||d.device_type}</span><span className="text-lg font-semibold tabular-nums">{ratePercent(d.gross_share)}</span></div>
      <div className="mt-2 h-2 overflow-hidden rounded-full bg-muted"><div className="h-full rounded-full bg-primary" style={{width:ratePercent(d.gross_share).replace('Unavailable','0%')}}/></div>
      <div className="mt-2 flex flex-wrap justify-between gap-x-3 text-xs text-muted-foreground"><span>Gross {formatMoney(d.gross_revenue_minor,currency)}</span><span>{exact(d.units)} units · avg item {d.avg_item_value_minor==null?'—':formatMoney(d.avg_item_value_minor,currency)}</span></div>
     </div>)}</section>
     <p className="mt-3 text-xs text-muted-foreground">Order value by device is not shown: product-level rows cannot count multi-item orders once per device.</p>
    </CardContent></Card>
   </div>
   <p className="source">{data.definition}</p>
  </div>}
 </main>;
}
