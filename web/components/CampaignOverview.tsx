import type {Alert,Dashboard} from '../app/types';
import {useState} from 'react';
import {changePercent,exact,formatMoney} from '../lib/format';
import {aggregate,defaultGranularity,nextBucket,type Bucket,type Granularity} from '../lib/series';
import type {DailyMetric} from '../app/types';
type TrendMetric='net_revenue_minor'|'spend_minor'|'orders';
const metricLabel:Record<TrendMetric,string>={net_revenue_minor:'net revenue',spend_minor:'ad spend',orders:'orders'};
const intervalLabel:Record<Granularity,string>={daily:'Daily',weekly:'Weekly',monthly:'Monthly'};
const day=(value:string)=>Date.parse(value+'T00:00:00Z');
function Trend({rows,currency,metric,granularity}:{rows:DailyMetric[];currency:string;metric:TrendMetric;granularity:Granularity}){
 const buckets=aggregate(rows,granularity),interval=intervalLabel[granularity],plotted=buckets.filter(b=>b[metric]!=null);
 const show=(value:string|null)=>metric==='orders'?exact(value):formatMoney(value,currency);
 const name=metric==='orders'?`${interval} orders for ${currency} campaigns`:`${interval} ${metricLabel[metric]} in ${currency}`;
 const values=plotted.map(b=>BigInt(b[metric]!));
 const low=values.reduce((a,b)=>a<b?a:b,0n),high=values.reduce((a,b)=>a>b?a:b,0n),range=high-low||1n;
 const start=plotted.length?day(plotted[0].start):0,end=plotted.length?day(plotted.at(-1)!.start):0;
 const x=(b:Bucket)=>end===start?320:40+(day(b.start)-start)/(end-start)*560;
 const y=(value:string)=>180-Number((BigInt(value)-low)*150000n/range)/1000;
 const segments:Bucket[][]=[];plotted.forEach(b=>{const last=segments.at(-1);if(last&&nextBucket(last.at(-1)!.start,granularity)===b.start)last.push(b);else segments.push([b]);});
 return <figure className="trend"><figcaption><strong>{metric==='net_revenue_minor'?'Net revenue':metric==='spend_minor'?'Ad spend':'Orders'} trend · {currency}</strong><span className="legend">Certified {interval.toLowerCase()} totals of observed days</span></figcaption>
 {!plotted.length?<p className="empty">{interval} {metricLabel[metric]} unavailable for this selection.</p>:
 <svg role="img" aria-label={name} viewBox="0 0 640 230"><title>Observed certified periods; gaps break the line. Exact values are in the table below.</title><line x1="40" x2="600" y1={y('0')} y2={y('0')} stroke="#c8d6df"/><g className={metric==='spend_minor'?'spend-line':metric==='orders'?'orders-line':'net-line'}>{segments.map((segment,i)=><g key={i}>{segment.length>1&&<polyline fill="none" stroke="currentColor" strokeWidth="3" points={segment.map(b=>`${x(b)},${y(b[metric]!)}`).join(' ')}/>} {segment.map(b=><circle key={b.start} cx={x(b)} cy={y(b[metric]!)} r="4" fill="currentColor"><title>{b.label}: {show(b[metric])}</title></circle>)}</g>)}</g><text x="40" y="217">{plotted[0].label}</text>{plotted.length>1&&<text x="600" y="217" textAnchor="end">{plotted.at(-1)!.label}</text>}</svg>}
 <details><summary>Exact {interval.toLowerCase()} values · {currency}</summary><div className="table-scroll"><table aria-label={interval+' certified values · '+currency}><thead><tr><th>UTC period</th><th>Net revenue</th><th>Ad spend</th><th>Orders</th><th>Exact net / spend minor units</th><th>Coverage</th></tr></thead><tbody>{buckets.map(b=><tr key={b.start}><td>{b.label}</td><td>{formatMoney(b.net_revenue_minor,currency)}</td><td>{formatMoney(b.spend_minor,currency)}</td><td>{exact(b.orders)}</td><td>{b.net_revenue_minor} / {b.spend_minor}</td><td>{b.days} observed {b.days===1?'day':'days'}</td></tr>)}</tbody></table></div></details></figure>;
}
export default function CampaignOverview({report,alerts,interactive,onInvestigate}:{report:Dashboard;alerts:Alert[];interactive:boolean;onInvestigate:(id:string)=>void}){
 const [metric,setMetric]=useState<TrendMetric>('net_revenue_minor');
 const range=report.filters.from_date+'|'+report.filters.to_date;
 const [chosen,setChosen]=useState<{range:string;value:Granularity}|null>(null);
 const granularity=chosen?.range===range?chosen.value:defaultGranularity(report.filters.from_date,report.filters.to_date);
 const certified=report.certified,series=certified.daily_series||[],comparison=certified.comparison;
 const relevant=alerts.filter(a=>a.evidence.outcome==='triggered'&&a.evidence.provenance?.identity===certified.provenance.identity&&a.evidence.from_date<report.filters.to_date&&a.evidence.to_date>report.filters.from_date&&certified.rows.some(r=>r.campaign_id===a.evidence.campaign_id)).sort((a,b)=>(b.evaluated_at||'').localeCompare(a.evaluated_at||''));
 const byCampaign=new Map<string,Alert[]>();for(const a of relevant)byCampaign.set(a.evidence.campaign_id,[...(byCampaign.get(a.evidence.campaign_id)||[]),a]);
 const attention=[...byCampaign.values()].slice(0,3);
 return <section className="decision-overview" aria-label="Decision overview">
 <div className="overview-grid"><section className="panel"><div className="overview-title"><div><h2>Certified trends</h2><p className="source">{report.filters.from_date} → {report.filters.to_date} (exclusive UTC) · Through {certified.provenance.data_through||'unavailable'} · Missing periods are gaps; each currency and unit has its own scale</p></div><div className="metric-switch" role="group" aria-label="Trend metric"><button aria-label="Show revenue trend" aria-pressed={metric==='net_revenue_minor'} onClick={()=>setMetric('net_revenue_minor')}>Revenue</button><button aria-label="Show spend trend" aria-pressed={metric==='spend_minor'} onClick={()=>setMetric('spend_minor')}>Spend</button><button aria-label="Show orders trend" aria-pressed={metric==='orders'} onClick={()=>setMetric('orders')}>Orders</button></div><div className="metric-switch" role="group" aria-label="Trend interval">{(['daily','weekly','monthly'] as const).map(value=><button key={value} aria-pressed={granularity===value} onClick={()=>setChosen({range,value})}>{intervalLabel[value]}</button>)}</div></div>
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
