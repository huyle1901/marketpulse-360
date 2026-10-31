'use client';
import {useEffect,useState} from 'react';
import {Bar,BarChart,CartesianGrid,Line,LineChart,XAxis,YAxis} from 'recharts';
import {Card,CardContent,CardDescription,CardHeader,CardTitle} from '@/components/ui/card';
import {StatStrip} from './StatStrip';
import {BarList,ExactValues} from './charts';
import {ChartContainer,ChartLegend,ChartLegendContent,ChartTooltip,ChartTooltipContent,type ChartConfig} from '@/components/ui/chart';
import {Table,TableBody,TableCell,TableHead,TableHeader,TableRow} from '@/components/ui/table';
import {exact,formatMoney} from '../lib/format';

/** Six-place certified ratio string to a one-decimal percent, truncated; null stays unavailable. */
export const ratePercent=(value:string|null|undefined)=>{
 if(value==null)return 'Unavailable';const match=/^(\d+)\.(\d{6})$/.exec(value);if(!match)return 'Unavailable';
 const tenths=BigInt(match[1]+match[2])/1000n;return (tenths/10n).toString()+'.'+(tenths%10n).toString()+'%';
};
const monthLabel=(value:string)=>new Date(value+'T00:00:00Z').toLocaleDateString('en-US',{month:'short',year:'2-digit',timeZone:'UTC'});
type Provenance={identity:string;generation:string;data_through?:string|null;projection_checksum?:string};
export type EmailReport={status:string;message?:string;definition?:string;provenance:Provenance;months:{month:string;sent:string;delivered:string;opened:string;clicked:string;unsubscribed:string;recipients:string;open_rate:string|null;click_rate:string|null;unsubscribe_rate:string|null}[];totals?:{sent:string;delivered:string;opened:string;clicked:string;unsubscribed:string;open_rate:string|null;click_rate:string|null;unsubscribe_rate:string|null}};
export type MixReport={status:string;message?:string;definition?:string;currency?:string;provenance:Provenance;months:{month:string;new_customers:string;returning_customers:string;new_orders:string;returning_orders:string;new_gross_minor:string;returning_gross_minor:string;returning_share:string|null}[]};

/** One validated read: a non-OK status, an error body or an unexpected shape is "unavailable", never a crash. */
export function useRead<T>(path:string,valid:(body:any)=>boolean):{data:T|null;error:string}{
 const [data,setData]=useState<T|null>(null);const [error,setError]=useState('');
 useEffect(()=>{const controller=new AbortController();
  void fetch('/api/v1/'+path,{cache:'no-store',signal:controller.signal}).then(async r=>{const body=await r.json();if(!r.ok)throw new Error(body?.message||body?.code||'Unavailable');if(!body||typeof body.status!=='string'||!body.provenance||(body.status==='available'&&!valid(body)))throw new Error('Unexpected response');setData(body);setError('');})
   .catch(e=>{if(!controller.signal.aborted)setError(e instanceof Error?e.message:'Unavailable');});
  return()=>controller.abort();},[path]);
 return {data,error};
}
export const useLifecycleRead=<T,>(path:string)=>useRead<T>('lifecycle/'+path,body=>Array.isArray(body.months));

const emailConfig={sent:{label:'Sent',color:'var(--chart-4)'},opened:{label:'Opened',color:'var(--chart-1)'},clicked:{label:'Clicked',color:'var(--chart-2)'}} satisfies ChartConfig;
export function EmailWorkspace(){
 const {data,error}=useLifecycleRead<EmailReport>('email-engagement');
 if(error)return <div className="error" role="alert"><strong>Email engagement unavailable</strong><p>{error}</p></div>;
 if(!data)return <p className="empty">Loading email engagement…</p>;
 if(data.status==='unavailable')return <p className="empty">{data.message||'Email engagement is unavailable for this certified release.'}</p>;
 const t=data.totals!;
 const cards:[string,string,string][]=[['Emails sent',exact(t.sent),'All lifecycle sends'],['Delivered',exact(t.delivered),'Reached an inbox'],['Open rate',ratePercent(t.open_rate),'Opened ÷ delivered'],['Click rate',ratePercent(t.click_rate),'Clicked ÷ delivered'],['Unsubscribe rate',ratePercent(t.unsubscribe_rate),'Unsubscribed ÷ delivered']];
 const chart=data.months.map(m=>({month:monthLabel(m.month),sent:Number(m.sent),opened:Number(m.opened),clicked:Number(m.clicked)}));
 return <div className="space-y-4">
  <StatStrip label="Email totals" className="md:grid-cols-5" items={cards.map(([name,value,hint])=>({name,value,hint}))}/>
  <Card className="min-w-0"><CardHeader><CardTitle>Monthly engagement</CardTitle><CardDescription>Governed email events by month · {data.definition}</CardDescription></CardHeader><CardContent>
   <div role="img" aria-label="Monthly emails sent, opened and clicked"><ChartContainer config={emailConfig} className="aspect-auto h-[260px] w-full"><LineChart data={chart} margin={{left:4,right:12,top:8}}><CartesianGrid vertical={false}/><XAxis dataKey="month" tickLine={false} axisLine={false} minTickGap={24}/><YAxis tickLine={false} axisLine={false} width={44}/><ChartTooltip content={<ChartTooltipContent/>}/><ChartLegend content={<ChartLegendContent/>}/>{(['sent','opened','clicked'] as const).map(k=><Line key={k} dataKey={k} type="monotone" stroke={'var(--color-'+k+')'} strokeWidth={2} dot={false} isAnimationActive={false}/>)}</LineChart></ChartContainer></div>
  </CardContent></Card>
  <Card className="min-w-0"><CardHeader><CardTitle>Email funnel</CardTitle><CardDescription>How many lifecycle emails reached each step across the certified history</CardDescription></CardHeader><CardContent>
   <BarList name="Email funnel" rows={([['sent','Sent',t.sent,'var(--chart-4)'],['delivered','Delivered',t.delivered,'var(--chart-1)'],['opened','Opened',t.opened,'var(--chart-5)'],['clicked','Clicked',t.clicked,'var(--chart-3)'],['unsubscribed','Unsubscribed',t.unsubscribed,'var(--chart-2)']] as const).map(([key,label,value,color])=>({key,label,value:Number(value),display:exact(value),color}))}/>
   <ExactValues label="Exact monthly email values"><Table aria-label="Email engagement by month"><TableHeader><TableRow>{['Month','Sent','Recipients','Delivered','Opened','Clicked','Unsubscribed','Open rate','Click rate'].map(h=><TableHead key={h} className={h==='Month'?'':'text-right'}>{h}</TableHead>)}</TableRow></TableHeader><TableBody>{[...data.months].reverse().map(m=><TableRow key={m.month}><TableCell className="font-medium">{m.month.slice(0,7)}</TableCell>{[m.sent,m.recipients,m.delivered,m.opened,m.clicked,m.unsubscribed].map((v,i)=><TableCell key={i} className="text-right tabular-nums">{exact(v)}</TableCell>)}<TableCell className="text-right tabular-nums">{ratePercent(m.open_rate)}</TableCell><TableCell className="text-right tabular-nums">{ratePercent(m.click_rate)}</TableCell></TableRow>)}</TableBody></Table></ExactValues>
  </CardContent></Card>
  <p className="source">Email is not credited with revenue here: the certified attribution policy credits paid clicks only, so inventing an email revenue model would contradict it.</p>
  <details className="provenance"><summary>Data details</summary><p>Release {data.provenance.identity} · generation {data.provenance.generation}</p><p>Projection checksum {data.provenance.projection_checksum||'Unavailable'}</p></details>
 </div>;
}

const mixConfig={new_customers:{label:'New',color:'var(--chart-1)'},returning_customers:{label:'Returning',color:'var(--chart-3)'}} satisfies ChartConfig;
export function CustomerMix(){
 const {data,error}=useLifecycleRead<MixReport>('customer-mix');
 return <section className="panel" aria-label="New vs returning customers"><div className="section-heading"><div><h2>New vs returning customers</h2><p>Ordering customers per month · {data?.definition||'new means the month of a first original order'}</p></div></div>
  {error?<p className="empty">New vs returning unavailable: {error}</p>:!data?<p className="empty">Loading…</p>:data.status==='unavailable'?<p className="empty">{data.message}</p>:!data.months.length?<p className="empty">No original orders in this release.</p>:<>
   <div role="img" aria-label="Monthly new and returning ordering customers"><ChartContainer config={mixConfig} className="aspect-auto h-[220px] w-full"><BarChart data={data.months.map(m=>({month:monthLabel(m.month),new_customers:Number(m.new_customers),returning_customers:Number(m.returning_customers)}))} margin={{left:4,right:8}}><CartesianGrid vertical={false}/><XAxis dataKey="month" tickLine={false} axisLine={false} minTickGap={20}/><YAxis tickLine={false} axisLine={false} width={40}/><ChartTooltip content={<ChartTooltipContent/>}/><ChartLegend content={<ChartLegendContent/>}/><Bar dataKey="new_customers" stackId="a" fill="var(--color-new_customers)" isAnimationActive={false}/><Bar dataKey="returning_customers" stackId="a" fill="var(--color-returning_customers)" radius={[4,4,0,0]} isAnimationActive={false}/></BarChart></ChartContainer></div>
   <details><summary>Exact monthly values</summary><Table aria-label="New and returning customers by month"><TableHeader><TableRow>{['Month','New customers','Returning customers','Returning share','New orders','Returning orders','New gross','Returning gross'].map(h=><TableHead key={h}>{h}</TableHead>)}</TableRow></TableHeader><TableBody>{data.months.map(m=><TableRow key={m.month}><TableCell>{m.month.slice(0,7)}</TableCell><TableCell>{exact(m.new_customers)}</TableCell><TableCell>{exact(m.returning_customers)}</TableCell><TableCell>{ratePercent(m.returning_share)}</TableCell><TableCell>{exact(m.new_orders)}</TableCell><TableCell>{exact(m.returning_orders)}</TableCell><TableCell>{data.currency?formatMoney(m.new_gross_minor,data.currency):exact(m.new_gross_minor)}</TableCell><TableCell>{data.currency?formatMoney(m.returning_gross_minor,data.currency):exact(m.returning_gross_minor)}</TableCell></TableRow>)}</TableBody></Table></details></>}
 </section>;
}
