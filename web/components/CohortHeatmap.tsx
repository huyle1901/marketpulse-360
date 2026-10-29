import type {Cohort} from '../app/customers/types';
import {exact} from '../lib/format';
export default function CohortHeatmap({rows,onDrill}:{rows:Cohort[];onDrill?:(row:Cohort)=>void}){
 if(!rows.length)return <p className="empty">No historical cohort activity is available.</p>;
 const months=[...new Set(rows.map(r=>r.month_number))].sort((a,b)=>Number(a)-Number(b));
 const groups=new Map<string,Cohort[]>();
 for(const row of rows){const key=row.cohort_month+'|'+row.profile_type;groups.set(key,[...(groups.get(key)||[]),row]);}
 return <div className="table-scroll heatmap-scroll"><table className="heatmap" aria-label="Retention heatmap"><caption>Months since cohort start · Color indicates observed retention · Unavailable cells have no observation</caption><thead><tr><th scope="col">Cohort / identity</th>{months.map(month=><th key={month} scope="col">Month {month}</th>)}</tr></thead><tbody>{[...groups.entries()].sort(([a],[b])=>a.localeCompare(b)).map(([key,group])=><tr key={key}><th scope="row">{group[0].cohort_month}<small>{group[0].profile_type}</small></th>{months.map(month=>{
 const row=group.find(r=>r.month_number===month);
 if(!row)return <td key={month} className="heatmap-missing"><span aria-hidden="true">—</span><small>Unavailable</small></td>;
 const rate=Number(row.retained_rate),percent=new Intl.NumberFormat('en-US',{style:'percent',maximumFractionDigits:1}).format(rate);
 const content=<><strong>{percent}</strong><small>{exact(row.active_profiles)} / {exact(row.cohort_size)} active</small><small>{row.activity_month.slice(0,7)}</small></>;
 return <td key={month} style={{backgroundColor:`rgba(0,127,121,${.06+Math.min(1,Math.max(0,rate))*.22})`}}>{onDrill?<button className="heatmap-value" aria-label={`Explore cohort ${row.cohort_month} ${row.profile_type} active ${row.activity_month}`} aria-description={`${percent} retained; ${row.active_profiles} active of original cohort ${row.cohort_size}`} onClick={()=>onDrill(row)}>{content}</button>:<div className="heatmap-value">{content}</div>}</td>;
 })}</tr>)}</tbody></table></div>;
}
