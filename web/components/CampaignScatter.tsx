'use client';

import {CartesianGrid,Scatter,ScatterChart,XAxis,YAxis,ZAxis} from 'recharts';
import {ChartContainer,ChartTooltip,type ChartConfig} from '@/components/ui/chart';
import {Legend} from './charts';
import {compactMoney,formatMoney} from '../lib/format';
import {multiple} from '../lib/metrics';
import type {Metric} from '../app/types';

const CHANNELS:[string,string,string][]=[['google','Google','var(--chart-1)'],['meta','Meta','var(--chart-2)'],['tiktok','TikTok','var(--chart-3)']];

/** Spend (x) against net ROAS (y), bubble area by orders; one colour per channel, named in the legend. */
export function CampaignScatter({rows,currency}:{rows:Metric[];currency:string}){
 const present=CHANNELS.filter(([key])=>rows.some(r=>r.platform===key));
 const config=Object.fromEntries(present.map(([key,label,color])=>[key,{label,color}])) satisfies ChartConfig;
 const points=(key:string)=>rows.filter(r=>r.platform===key&&BigInt(r.spend_minor)>0n).map(r=>({name:r.campaign_name||r.campaign_id,
  spend:Number(r.spend_minor),roas:Number(r.net_revenue_minor)/Number(r.spend_minor),orders:Number(r.orders),
  spendLabel:formatMoney(r.spend_minor,currency),roasLabel:multiple(r.net_revenue_minor,r.spend_minor)??'—'}));
 return <div className="space-y-3">
  <Legend label="Campaign channel legend" items={present.map(([key,label,color])=>({key,label,color}))}/>
  <div role="img" aria-label={`Campaign ad spend against net ROAS · ${currency}`}>
   <ChartContainer config={config} className="aspect-auto h-[300px] w-full"><ScatterChart margin={{left:8,right:16,top:12,bottom:4}}>
    <CartesianGrid strokeDasharray="3 4"/>
    <XAxis type="number" dataKey="spend" name="Ad spend" tickLine={false} axisLine={false} tickFormatter={v=>compactMoney(String(Math.round(Number(v))),currency)} domain={['auto','auto']}/>
    <YAxis type="number" dataKey="roas" name="Net ROAS" tickLine={false} axisLine={false} width={44} tickFormatter={v=>Number(v).toFixed(0)+'×'} domain={['auto','auto']}/>
    <ZAxis type="number" dataKey="orders" range={[60,420]}/>
    <ChartTooltip cursor={{strokeDasharray:'3 3'}} content={({active,payload})=>{const p=payload?.[0]?.payload as {name:string;spendLabel:string;roasLabel:string;orders:number}|undefined;
     return active&&p?<div className="rounded-lg border bg-popover px-3 py-2 text-xs shadow-md"><div className="font-medium text-foreground">{p.name}</div><div className="text-muted-foreground">Spend {p.spendLabel} · ROAS {p.roasLabel} · {p.orders} orders</div></div>:null;}}/>
    {present.map(([key,,color])=><Scatter key={key} name={key} data={points(key)} fill={color} fillOpacity={0.8} stroke="var(--card)" strokeWidth={1.5} isAnimationActive={false}/>)}
   </ScatterChart></ChartContainer>
  </div>
 </div>;
}
