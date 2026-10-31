'use client';

import type {ReactNode} from 'react';
import {Cell,Pie,PieChart} from 'recharts';
import {ChartContainer,ChartTooltip,ChartTooltipContent,type ChartConfig} from '@/components/ui/chart';

export type Slice={key:string;label:string;value:number;color:string;display:string};

/** Legend with a colour mark per series; identity never relies on colour alone because every item is labelled. */
export function Legend({items,label='Legend'}:{items:{key:string;label:string;color:string;value?:string}[];label?:string}){
 return <ul aria-label={label} className="flex flex-wrap gap-x-4 gap-y-1.5 text-xs text-muted-foreground">
  {items.map(item=><li key={item.key} className="flex items-center gap-1.5"><span className="inline-block size-2.5 shrink-0 rounded-[3px]" style={{background:item.color}} aria-hidden="true"/>
   <span className="text-foreground">{item.label}</span>{item.value&&<span className="tabular-nums">{item.value}</span>}</li>)}
 </ul>;
}

/** Part-to-whole for a handful of named parts; the centre carries the total, the legend carries every exact share. */
export function Donut({name,slices,total,caption,size=168}:{name:string;slices:Slice[];total:string;caption:string;size?:number}){
 const config=Object.fromEntries(slices.map(s=>[s.key,{label:s.label,color:s.color}])) satisfies ChartConfig;
 const sum=slices.reduce((a,s)=>a+Math.max(0,s.value),0);
 return <figure className="flex flex-col items-center gap-3 sm:flex-row sm:items-center sm:gap-6">
  <div role="img" aria-label={name+': '+slices.map(s=>`${s.label} ${s.display}`).join(', ')} className="relative shrink-0" style={{width:size,height:size}}>
   <ChartContainer config={config} className="aspect-square" style={{width:size,height:size}}><PieChart>
    <ChartTooltip content={<ChartTooltipContent hideLabel nameKey="key"/>}/>
    <Pie data={slices.filter(s=>s.value>0)} dataKey="value" nameKey="key" innerRadius={Math.round(size/3)} outerRadius={Math.round(size/2.1)} paddingAngle={2} stroke="var(--card)" strokeWidth={2} isAnimationActive={false}>
     {slices.filter(s=>s.value>0).map(s=><Cell key={s.key} fill={s.color}/>)}
    </Pie></PieChart></ChartContainer>
   <div className="pointer-events-none absolute inset-0 grid place-items-center text-center"><div><div className="figure text-lg text-foreground">{total}</div><div className="text-[11px] text-muted-foreground">{caption}</div></div></div>
  </div>
  <figcaption className="min-w-0 flex-1"><ul aria-label={name+' legend'} className="space-y-2">{slices.map(s=><li key={s.key} className="grid grid-cols-[minmax(0,1fr)_auto] items-center gap-2 text-sm">
   <span className="flex min-w-0 items-center gap-2"><span className="inline-block size-2.5 shrink-0 rounded-[3px]" style={{background:s.color}} aria-hidden="true"/><span className="truncate">{s.label}</span></span>
   <span className="tabular-nums text-muted-foreground"><span className="text-foreground">{s.display}</span> · {sum>0&&s.value>0?(s.value/sum*100).toFixed(1):'0.0'}%</span></li>)}</ul></figcaption>
 </figure>;
}

/** Ranked horizontal bars for one measure; bars never encode negatives, which are shown as text. */
export function BarList({name,rows,color='var(--primary)'}:{name:string;rows:{key:string;label:ReactNode;value:number;display:ReactNode;sub?:ReactNode;color?:string}[];color?:string}){
 const top=Math.max(1,...rows.map(r=>r.value));
 return <ol role="list" aria-label={name} className="space-y-3">{rows.map(row=><li key={row.key} className="min-w-0">
  <div className="flex items-baseline justify-between gap-3 text-sm"><span className="min-w-0 truncate">{row.label}</span><span className="figure shrink-0 text-sm text-foreground">{row.display}</span></div>
  <div className="mt-1.5 h-2 rounded-full bg-muted"><div className="h-full rounded-full" style={{width:Math.max(0,row.value)/top*100+'%',background:row.color??color}}/></div>
  {row.sub&&<div className="mt-1 text-xs text-muted-foreground">{row.sub}</div>}</li>)}</ol>;
}

/** Exact numbers stay one click away under every chart. */
export function ExactValues({label,children}:{label:string;children:ReactNode}){
 return <details className="group mt-4 rounded-lg border bg-background/40 px-3 py-2 text-sm"><summary className="cursor-pointer text-muted-foreground hover:text-foreground">{label}</summary><div className="mt-3">{children}</div></details>;
}
