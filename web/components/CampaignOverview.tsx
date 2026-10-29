import type {Alert,Dashboard} from '../app/types';
import {useState} from 'react';
import {Area,AreaChart,CartesianGrid,XAxis,YAxis} from 'recharts';
import {ChartContainer,ChartTooltip,type ChartConfig} from '@/components/ui/chart';
import {changePercent,exact,formatMoney} from '../lib/format';
import {aggregate,defaultGranularity,nextBucket,type Bucket,type Granularity} from '../lib/series';
import type {DailyMetric} from '../app/types';
type TrendMetric='net_revenue_minor'|'spend_minor'|'orders';
const metricLabel:Record<TrendMetric,string>={net_revenue_minor:'net revenue',spend_minor:'ad spend',orders:'orders'};
const intervalLabel:Record<Granularity,string>={daily:'Daily',weekly:'Weekly',monthly:'Monthly'};
const tones:Record<TrendMetric,string>={net_revenue_minor:'var(--chart-1)',spend_minor:'var(--chart-5)',orders:'var(--chart-3)'};
function Trend({rows,currency,metric,granularity}:{rows:DailyMetric[];currency:string;metric:TrendMetric;granularity:Granularity}){
 const buckets=aggregate(rows,granularity),interval=intervalLabel[granularity],plotted=buckets.filter(b=>b[metric]!=null);
 const show=(value:string|null)=>metric==='orders'?exact(value):formatMoney(value,currency);
 const name=metric==='orders'?`${interval} orders for ${currency} campaigns`:`${interval} ${metricLabel[metric]} in ${currency}`;
 const byStart=new Map(plotted.map(b=>[b.start,b]));
 // Every period between the first and last observation is a point; unobserved periods are null gaps, never zero.
 const points:{start:string;label:string;value:number|null;bucket?:Bucket}[]=[];
 for(let start=plotted[0]?.start;start&&start<=plotted.at(-1)!.start;start=nextBucket(start,granularity)){const b=byStart.get(start);points.push({start,label:b?.label??start,value:b?Number(b[metric]):null,bucket:b});}
 const segments=points.reduce((n,p,i)=>p.value!=null&&(i===0||points[i-1].value==null)?n+1:n,0);
 const config={value:{label:metricLabel[metric],color:tones[metric]}} satisfies ChartConfig;
 return <figure className="trend" data-segments={segments}><figcaption><strong>{metric==='net_revenue_minor'?'Net revenue':metric==='spend_minor'?'Ad spend':'Orders'} trend · {currency}</strong><span className="legend">{interval} totals · hollow points are partial periods · gaps mean no published data</span></figcaption>
 {!plotted.length?<p className="empty">{interval} {metricLabel[metric]} unavailable for this selection.</p>:
 <div role="img" aria-label={name}><ChartContainer config={config} className="aspect-auto h-[200px] w-full">
  <AreaChart data={points} margin={{left:4,right:12,top:8}}>
   <defs><linearGradient id={'fill-'+metric} x1="0" y1="0" x2="0" y2="1"><stop offset="5%" stopColor="var(--color-value)" stopOpacity={0.35}/><stop offset="95%" stopColor="var(--color-value)" stopOpacity={0.03}/></linearGradient></defs>
   <CartesianGrid vertical={false}/>
   <XAxis dataKey="label" tickLine={false} axisLine={false} tickMargin={8} minTickGap={32}/>
   <YAxis tickLine={false} axisLine={false} width={64} tickFormatter={v=>compact(Number(v))}/>
   <ChartTooltip cursor={false} content={({active,payload})=>{const p=payload?.[0]?.payload as typeof points[number]|undefined;return active&&p?.bucket?<div className="rounded-lg border bg-background px-3 py-2 text-xs shadow-md"><div className="font-medium">{p.label}</div><div className="tabular-nums">{show(p.bucket[metric])}</div><div className="text-muted-foreground">{p.bucket.days} of {p.bucket.expected} days published{p.bucket.days<p.bucket.expected?' · partial period':''}</div></div>:null;}}/>
   <Area dataKey="value" type="monotone" connectNulls={false} fill={'url(#fill-'+metric+')'} stroke="var(--color-value)" strokeWidth={2} dot={(props:{cx?:number;cy?:number;index?:number})=>{const b=points[props.index??0]?.bucket;if(props.cx==null||props.cy==null||!b)return <g key={props.index}/>;const partial=b.days<b.expected;return partial||points.length<40?<circle key={props.index} cx={props.cx} cy={props.cy} r={partial?3.5:3} fill={partial?'var(--background)':'var(--color-value)'} stroke="var(--color-value)" strokeWidth={1.5}/>:<g key={props.index}/>;}} isAnimationActive={false}/>
  </AreaChart></ChartContainer></div>}
 <details><summary>Exact {interval.toLowerCase()} values · {currency}</summary><div className="table-scroll"><table aria-label={interval+' certified values · '+currency}><thead><tr><th>UTC period</th><th>Net revenue</th><th>Ad spend</th><th>Orders</th><th>Exact net / spend minor units</th><th>Coverage</th></tr></thead><tbody>{buckets.map(b=><tr key={b.start}><td>{b.label}</td><td>{formatMoney(b.net_revenue_minor,currency)}</td><td>{formatMoney(b.spend_minor,currency)}</td><td>{exact(b.orders)}</td><td>{b.net_revenue_minor} / {b.spend_minor}</td><td>{b.days} of {b.expected} days{b.days<b.expected?' · partial':''}</td></tr>)}</tbody></table></div></details></figure>;
}
const compact=(value:number)=>new Intl.NumberFormat('en-US',{notation:'compact',maximumFractionDigits:1}).format(value);
const DAY=86400000,isoDay=(stamp:number)=>new Date(stamp).toISOString().slice(0,10);
/** All = the read contract's 731-day maximum; an explicit window so the long-history 90-day opening never overrides it. */
const ranges=[['7D',7],['30D',30],['90D',90],['12M',365],['All',731]] as const;
export default function CampaignOverview({report,alerts,interactive,onInvestigate,onDates,requested}:{report:Dashboard;alerts:Alert[];interactive:boolean;onInvestigate:(id:string)=>void;onDates?:(from:string,to:string)=>void;requested?:{from_date:string;to_date:string}}){
 const through=report.certified.provenance.data_through?.slice(0,10),want=requested??report.filters,span=(Date.parse(want.to_date)-Date.parse(want.from_date))/DAY;
 const [metric,setMetric]=useState<TrendMetric>('net_revenue_minor');
 const range=report.filters.from_date+'|'+report.filters.to_date;
 const [chosen,setChosen]=useState<{range:string;value:Granularity}|null>(null);
 const granularity=chosen?.range===range?chosen.value:defaultGranularity(report.filters.from_date,report.filters.to_date);
 const certified=report.certified,series=certified.daily_series||[],comparison=certified.comparison;
 const relevant=alerts.filter(a=>a.evidence.outcome==='triggered'&&a.evidence.provenance?.identity===certified.provenance.identity&&a.evidence.from_date<report.filters.to_date&&a.evidence.to_date>report.filters.from_date&&certified.rows.some(r=>r.campaign_id===a.evidence.campaign_id)).sort((a,b)=>(b.evaluated_at||'').localeCompare(a.evaluated_at||''));
 const byCampaign=new Map<string,Alert[]>();for(const a of relevant)byCampaign.set(a.evidence.campaign_id,[...(byCampaign.get(a.evidence.campaign_id)||[]),a]);
 const attention=[...byCampaign.values()].slice(0,3);
 return <section className="decision-overview" aria-label="Decision overview">
 <div className="overview-grid"><section className="panel"><div className="overview-title flex flex-wrap items-start justify-between gap-3"><div className="min-w-48"><h2>Certified trends</h2><p className="source whitespace-nowrap">{report.filters.from_date} to {isoDay(Date.parse(report.filters.to_date)-DAY)} · data through {through?isoDay(Date.parse(through)-DAY):'unavailable'}</p></div>{onDates&&through&&<div className="metric-switch" role="group" aria-label="Date range">{ranges.map(([label,days])=><button key={label} aria-pressed={want.to_date===through&&span===days} onClick={()=>onDates(isoDay(Math.max(Date.UTC(1900,0,1),Date.parse(through)-days*DAY)),through)}>{label}</button>)}</div>}<div className="metric-switch" role="group" aria-label="Trend metric"><button aria-label="Show revenue trend" aria-pressed={metric==='net_revenue_minor'} onClick={()=>setMetric('net_revenue_minor')}>Revenue</button><button aria-label="Show spend trend" aria-pressed={metric==='spend_minor'} onClick={()=>setMetric('spend_minor')}>Spend</button><button aria-label="Show orders trend" aria-pressed={metric==='orders'} onClick={()=>setMetric('orders')}>Orders</button></div><div className="metric-switch" role="group" aria-label="Trend interval">{(['daily','weekly','monthly'] as const).map(value=><button key={value} aria-pressed={granularity===value} onClick={()=>setChosen({range,value})}>{intervalLabel[value]}</button>)}</div></div>
 {!series.length?<p className="empty">Daily trends unavailable for this selection.</p>:[...new Set(series.map(r=>r.currency))].map(currency=><Trend key={currency} currency={currency} metric={metric} granularity={granularity} rows={series.filter(r=>r.currency===currency)}/>)}
 <div className="period-comparison"><h3>Previous period</h3>{comparison&&<p>{comparison.from_date} → {comparison.to_date} (exclusive UTC) · {comparison.observed_days}/{comparison.expected_days} published days</p>}
 {comparison&&<p>Current period: {comparison.current_observed_days??'Unknown'}/{comparison.current_expected_days??'Unknown'} published days</p>}
 {comparison?.status==='complete'&&comparison.current_status==='complete'?certified.totals.map(row=>{
 const prior=comparison.totals.find(p=>p.currency===row.currency);
 return <div key={row.currency} className="comparison-row"><strong>{row.currency}</strong>{(['net_revenue_minor','spend_minor'] as const).map(field=>{
  const baseline=prior?.[field],change=baseline==null?null:changePercent(row[field],baseline);
  return <p key={field}>{field==='net_revenue_minor'?'Net revenue':'Ad spend'}: {formatMoney(row[field],row.currency)} vs {formatMoney(baseline,row.currency)} · <strong>{baseline==null?'Baseline unavailable for this currency and campaign selection':change??'Percentage unavailable: nonpositive baseline'}</strong></p>;
 })}</div>;
 }):<p className="empty">{comparison?.current_status==='partial'?'Current period partially covered; comparison unavailable':comparison?.current_status==='unavailable'?'Current period unavailable; comparison unavailable':comparison?.previous_status==='partial'?'Previous period partially covered; comparison unavailable':'Previous period unavailable'}</p>}</div></section>
 <aside className="panel attention"><h2>Needs attention</h2><p>Matching alert evidence for this release and selected campaigns/period.</p>{!attention.length?<p className="empty">No matching alert evidence for this selection.</p>:attention.map(items=>{const a=items[0],name=report.campaigns.find(c=>c.campaign_id===a.evidence.campaign_id)?.name||a.evidence.campaign_id;return <article key={a.evidence.campaign_id}><div><h3>{name}</h3>{items.length>1&&<span className="badge neutral">{items.length} occurrences</span>}</div><p>{a.evidence.explanation}</p><small>{a.evidence.from_date} → {a.evidence.to_date}</small><button disabled={!interactive} className="secondary" onClick={()=>onInvestigate(a.evidence.campaign_id)}>Investigate {name}</button></article>})}<a href="#alert-evidence" onClick={()=>{const d=document.getElementById('alert-evidence');if(d instanceof HTMLDetailsElement)d.open=true;}}>View all alert evidence</a><p className="source">Evidence supports investigation; it does not change campaigns or budgets.</p></aside></div></section>;
}
