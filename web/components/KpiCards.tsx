import {ArrowDownRight,ArrowUpRight,Minus} from 'lucide-react';
import {Card,CardContent,CardHeader,CardTitle} from '@/components/ui/card';
import {compactMoney,exact,formatMoney} from '../lib/format';
import {aggregate,defaultGranularity} from '../lib/series';
import {multiple,percentChange,perUnit,type Totals} from '../lib/metrics';
import type {Dashboard} from '../app/types';

type Kpi={name:string;value:string;exactValue?:string;hint:string;change:string|null|undefined;higherIsBetter:boolean|null;series:(string|null)[]};

/** Minimal sparkline; points are presentation-normalized, exact values live in the trend table. */
function Sparkline({name,values}:{name:string;values:(string|null)[]}){
 const known=values.filter((v):v is string=>v!=null).map(v=>BigInt(v));
 if(known.length<2)return null;
 const low=known.reduce((a,b)=>a<b?a:b),high=known.reduce((a,b)=>a>b?a:b),range=high-low||1n;
 const points=values.map((v,i)=>v==null?null:`${(i/(values.length-1))*100},${28-Number((BigInt(v)-low)*2400n/range)/100}`);
 const path=points.reduce<string[][]>((runs,p)=>{if(p==null)runs.push([]);else runs[runs.length-1].push(p);return runs;},[[]]).filter(r=>r.length>1);
 return <svg role="img" aria-label={name} viewBox="0 0 100 30" preserveAspectRatio="none" className="mt-2 h-6 w-full text-primary">{path.map((run,i)=><polyline key={i} points={run.join(' ')} fill="none" stroke="currentColor" strokeWidth="1.6" vectorEffect="non-scaling-stroke"/>)}</svg>;
}

function Change({kpi}:{kpi:Kpi}){
 if(kpi.change===undefined)return null;
 if(kpi.change==null)return <span className="truncate text-xs text-muted-foreground" title="The previous equal-length period is not fully published">No comparable prior period</span>;
 const up=kpi.change.startsWith('+'),flat=!up&&!kpi.change.startsWith('-');
 const good=kpi.higherIsBetter==null||flat?null:up===kpi.higherIsBetter;
 const Icon=flat?Minus:up?ArrowUpRight:ArrowDownRight;
 return <span className={'inline-flex items-center gap-0.5 rounded-md px-1.5 py-0.5 text-xs font-medium '+(good==null?'bg-muted text-muted-foreground':good?'bg-emerald-50 text-emerald-700':'bg-rose-50 text-rose-700')}><Icon className="size-3" aria-hidden="true"/>{kpi.change}<span className="sr-only"> vs previous period</span></span>;
}

export default function KpiCards({report}:{report:Dashboard}){
 const {totals,comparison,daily_series:series=[]}=report.certified;
 const comparable=comparison?.status==='complete'&&comparison.current_status==='complete';
 return <>{totals.map(row=>{
  const t=row as Totals,prior=comparable?comparison!.totals.find(p=>p.currency===row.currency) as Totals|undefined:undefined;
  const interval=defaultGranularity(report.filters.from_date,report.filters.to_date),days=aggregate(series.filter(d=>d.currency===row.currency),interval);
  const money=(v:string|null)=>v==null?'Unavailable':formatMoney(v,row.currency),short=(v:string|null)=>v==null?'Unavailable':compactMoney(v,row.currency);
  const kpis:Kpi[]=[
   {name:'Net revenue',value:short(t.net_revenue_minor),exactValue:money(t.net_revenue_minor),hint:'After refunds',change:percentChange(t.net_revenue_minor,prior?.net_revenue_minor),higherIsBetter:true,series:days.map(d=>d.net_revenue_minor)},
   {name:'Ad spend',value:short(t.spend_minor),exactValue:money(t.spend_minor),hint:'Google, Meta and TikTok',change:percentChange(t.spend_minor,prior?.spend_minor),higherIsBetter:null,series:days.map(d=>d.spend_minor)},
   {name:'Net ROAS',value:multiple(t.net_revenue_minor,t.spend_minor)??'Unavailable',hint:'Net revenue per unit of ad spend',change:undefined,higherIsBetter:true,series:[]},
   {name:'Orders',value:exact(t.orders),hint:'Original purchases',change:percentChange(t.orders,prior?.orders),higherIsBetter:true,series:days.map(d=>d.orders??null)},
   {name:'Avg. order value',value:short(perUnit(t.gross_revenue_minor,t.orders)),exactValue:money(perUnit(t.gross_revenue_minor,t.orders)),hint:'Gross revenue per order',change:percentChange(perUnit(t.gross_revenue_minor,t.orders),perUnit(prior?.gross_revenue_minor,prior?.orders)),higherIsBetter:true,series:[]},
   {name:'Cost per order',value:short(perUnit(t.spend_minor,t.orders)),exactValue:money(perUnit(t.spend_minor,t.orders)),hint:'Ad spend per order',change:percentChange(perUnit(t.spend_minor,t.orders),perUnit(prior?.spend_minor,prior?.orders)),higherIsBetter:false,series:[]},
  ];
  return <section key={row.currency} aria-label={'Key metrics · '+row.currency} className="mb-4 grid grid-cols-2 gap-3 md:grid-cols-3 xl:grid-cols-6">
   {kpis.map(kpi=><Card key={kpi.name} role="article" aria-label={kpi.name} title={kpi.hint} className="gap-1 bg-gradient-to-t from-primary/5 to-card py-3 shadow-[var(--shadow-card)]">
    <CardHeader className="px-3.5"><CardTitle className="text-xs font-medium text-muted-foreground">{kpi.name}{kpi.name==='Net ROAS'||kpi.name==='Ad spend'||kpi.name==='Net revenue'?' · '+row.currency:''}</CardTitle></CardHeader>
    <CardContent className="px-3.5"><div className="text-2xl font-semibold tracking-tight tabular-nums" title={kpi.exactValue??kpi.value}>{kpi.value}</div>
     <div className="mt-1 flex min-h-5 items-center gap-1.5 truncate"><Change kpi={kpi}/>{kpi.change&&<span className="truncate text-xs text-muted-foreground">vs prev.</span>}</div>
     <Sparkline name={kpi.name+' by '+({daily:'day',weekly:'week',monthly:'month'} as const)[interval]} values={kpi.series}/></CardContent>
   </Card>)}
  </section>;
 })}</>;
}
