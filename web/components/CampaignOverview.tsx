import type {Alert,Dashboard} from '../app/types';
import {changePercent,formatMoney} from '../lib/format';
type Daily=NonNullable<Dashboard['certified']['daily_series']>[number];
const day=(value:string)=>Date.parse(value+'T00:00:00Z');
function Trend({rows,currency}:{rows:Daily[];currency:string}){
 const values=rows.flatMap(r=>[BigInt(r.net_revenue_minor),BigInt(r.spend_minor)]);
 const low=values.reduce((a,b)=>a<b?a:b,0n),high=values.reduce((a,b)=>a>b?a:b,0n),range=high-low||1n;
 const start=day(rows[0].event_date),end=day(rows.at(-1)!.event_date);
 const x=(r:Daily)=>end===start?320:40+(day(r.event_date)-start)/(end-start)*560;
 const y=(value:string)=>180-Number((BigInt(value)-low)*150000n/range)/1000;
 const segments:Daily[][]=[];rows.forEach(r=>{const last=segments.at(-1);if(last&&day(r.event_date)-day(last.at(-1)!.event_date)===86400000)last.push(r);else segments.push([r]);});
 return <figure className="trend"><figcaption><strong>{currency}</strong><span className="legend"><i className="net"/> Net revenue <i className="spend"/> Ad spend</span></figcaption>
 <svg role="img" aria-label={'Daily net revenue and ad spend in '+currency} viewBox="0 0 640 230"><title>Observed certified days; gaps break the lines. Exact values are in the table below.</title><line x1="40" x2="600" y1={y('0')} y2={y('0')} stroke="#c8d6df"/>{(['net_revenue_minor','spend_minor'] as const).map(field=><g key={field} className={field==='spend_minor'?'spend-line':'net-line'}>{segments.map((segment,i)=><g key={i}>{segment.length>1&&<polyline fill="none" stroke="currentColor" strokeWidth="3" points={segment.map(r=>`${x(r)},${y(r[field])}`).join(' ')}/>} {segment.map(r=><circle key={r.event_date} cx={x(r)} cy={y(r[field])} r="4" fill="currentColor"><title>{r.event_date}: {formatMoney(r[field],currency)}</title></circle>)}</g>)}</g>)}<text x="40" y="217">{rows[0].event_date}</text>{rows.length>1&&<text x="600" y="217" textAnchor="end">{rows.at(-1)!.event_date}</text>}</svg>
 <details><summary>Exact daily values · {currency}</summary><div className="table-scroll"><table aria-label={'Daily certified values · '+currency}><thead><tr><th>UTC day</th><th>Net revenue</th><th>Ad spend</th><th>Exact net / spend minor units</th></tr></thead><tbody>{rows.map(r=><tr key={r.event_date}><td>{r.event_date}</td><td>{formatMoney(r.net_revenue_minor,currency)}</td><td>{formatMoney(r.spend_minor,currency)}</td><td>{r.net_revenue_minor} / {r.spend_minor}</td></tr>)}</tbody></table></div></details></figure>;
}
export default function CampaignOverview({report,alerts,interactive,onInvestigate}:{report:Dashboard;alerts:Alert[];interactive:boolean;onInvestigate:(id:string)=>void}){
 const certified=report.certified,series=certified.daily_series||[],comparison=certified.comparison;
 const relevant=alerts.filter(a=>a.evidence.outcome==='triggered'&&a.evidence.provenance?.identity===certified.provenance.identity&&a.evidence.from_date<report.filters.to_date&&a.evidence.to_date>report.filters.from_date&&certified.rows.some(r=>r.campaign_id===a.evidence.campaign_id)).sort((a,b)=>(b.evaluated_at||'').localeCompare(a.evaluated_at||'')).slice(0,3);
 return <section className="decision-overview" aria-label="Decision overview">
 <div className="overview-grid"><section className="panel"><h2>Daily certified trends</h2><p>{report.filters.from_date} → {report.filters.to_date} (exclusive UTC). Data through {certified.provenance.data_through||'Unavailable'}.</p><p className="source">Observed days only; missing dates are gaps, not zero activity. Each chart uses its own currency scale.</p>
 {!series.length?<p className="empty">Daily trends unavailable for this selection.</p>:[...new Set(series.map(r=>r.currency))].map(currency=><Trend key={currency} currency={currency} rows={series.filter(r=>r.currency===currency)}/>)}
 <div className="period-comparison"><h3>Previous period</h3>{comparison&&<p>{comparison.from_date} → {comparison.to_date} (exclusive UTC) · {comparison.observed_days}/{comparison.expected_days} published days</p>}
 {comparison&&<p>Current period: {comparison.current_observed_days??'Unknown'}/{comparison.current_expected_days??'Unknown'} published days</p>}
 {comparison?.status==='complete'&&comparison.current_status==='complete'?certified.totals.map(row=>{
 const prior=comparison.totals.find(p=>p.currency===row.currency);
 return <div key={row.currency} className="comparison-row"><strong>{row.currency}</strong>{(['net_revenue_minor','spend_minor'] as const).map(field=>{
  const baseline=prior?.[field],change=baseline==null?null:changePercent(row[field],baseline);
  return <p key={field}>{field==='net_revenue_minor'?'Net revenue':'Ad spend'}: {formatMoney(row[field],row.currency)} vs {formatMoney(baseline,row.currency)} · <strong>{baseline==null?'Baseline unavailable for this currency and campaign selection':change??'Percentage unavailable: nonpositive baseline'}</strong></p>;
 })}</div>;
 }):<p className="empty">{comparison?.current_status==='partial'?'Current period partially covered; comparison unavailable':comparison?.current_status==='unavailable'?'Current period unavailable; comparison unavailable':comparison?.previous_status==='partial'?'Previous period partially covered; comparison unavailable':'Previous period unavailable'}</p>}</div></section>
 <aside className="panel attention"><h2>Needs attention</h2><p>Matching alert evidence for this release and selected campaigns/period.</p>{!relevant.length?<p className="empty">No matching alert evidence for this selection.</p>:relevant.map(a=>{const name=report.campaigns.find(c=>c.campaign_id===a.evidence.campaign_id)?.name||a.evidence.campaign_id;return <article key={a.evaluation_key}><h3>{name}</h3><p>{a.evidence.explanation}</p><small>{a.evidence.from_date} → {a.evidence.to_date}</small><button disabled={!interactive} className="secondary" onClick={()=>onInvestigate(a.evidence.campaign_id)}>Investigate {name}</button></article>})}<a href="#alert-evidence">View all alert evidence</a><p className="source">Evidence supports investigation; it does not change campaigns or budgets.</p></aside></div></section>;
}
