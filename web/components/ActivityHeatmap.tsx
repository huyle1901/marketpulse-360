'use client';
import {useState} from 'react';
import {exact} from '../lib/format';

export type Activity={status:string;message?:string;timezone:string;cells:{weekday:string;hour:string;sessions:string;orders:string}[];busiest:{weekday:string;hour:string;sessions:string}|null};
const days=['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'];
/** Validated sequential ramp (blue 150→700); zero uses the muted surface so "none" never reads as "some". */
const ramp=['#b7d3f6','#86b6ef','#5598e7','#2a78d6','#1c5cab','#104281','#0d366b'];
const slot=(hour:number)=>`${String(hour).padStart(2,'0')}:00–${String((hour+1)%24).padStart(2,'0')}:00`;

export default function ActivityHeatmap({data}:{data:Activity}){
 const [measure,setMeasure]=useState<'sessions'|'orders'>('sessions');
 const value=(c:Activity['cells'][number])=>BigInt(c[measure]);
 const max=data.cells.reduce((m,c)=>value(c)>m?value(c):m,0n);
 const grid=new Map(data.cells.map(c=>[c.weekday+'|'+c.hour,c]));
 const tone=(v:bigint)=>v===0n||max===0n?'var(--muted)':ramp[Math.min(ramp.length-1,Number((v*BigInt(ramp.length))/(max+1n)))];
 const unit=measure==='sessions'?'sessions':'orders';
 const busiest=data.busiest?`${days[Number(data.busiest.weekday)-1]} ${slot(Number(data.busiest.hour))} UTC`:null;
 return <div className="space-y-3">
  <div className="flex flex-wrap items-center justify-between gap-3">
   <p className="text-sm">{busiest?<><span className="text-muted-foreground">Busiest: </span><strong>{busiest}</strong><span className="text-muted-foreground"> · {exact(data.busiest!.sessions)} sessions</span></>:'No sessions in this release'}</p>
   <div className="metric-switch" role="group" aria-label="Heatmap measure"><button aria-pressed={measure==='sessions'} onClick={()=>setMeasure('sessions')}>Show sessions</button><button aria-pressed={measure==='orders'} onClick={()=>setMeasure('orders')}>Show orders</button></div>
  </div>
  <div className="overflow-x-auto">
   <table aria-label={`${measure==='sessions'?'Sessions':'Orders'} by weekday and hour (UTC)`} className="activity-grid w-full min-w-[640px] table-fixed border-separate border-spacing-[3px]">
    <thead><tr><th className="w-11 bg-transparent p-0"/>{Array.from({length:24},(_,h)=><th key={h} scope="col" className="bg-transparent p-0 text-center text-[10px] font-normal text-muted-foreground">{h%3===0?String(h).padStart(2,'0'):''}</th>)}</tr></thead>
    <tbody>{days.map((day,d)=><tr key={day}><th scope="row" className="bg-transparent p-0 pr-2 text-left text-xs font-medium text-muted-foreground">{day.slice(0,3)}</th>
     {Array.from({length:24},(_,h)=>{const cell=grid.get(`${d+1}|${h}`);const v=cell?value(cell):0n;const label=`${day} ${slot(h)} UTC: ${exact(v.toString())} ${unit}`;
      return <td key={h} aria-label={label} title={label} className="h-7 rounded-[4px] p-0 transition-transform hover:scale-110" style={{background:tone(v)}}/>;})}</tr>)}</tbody>
   </table>
  </div>
  <div className="flex items-center gap-2 text-xs text-muted-foreground" aria-hidden="true"><span>Fewer</span>{['var(--muted)',...ramp].map(c=><span key={c} className="h-3 w-5 rounded-sm" style={{background:c}}/>)}<span>More {unit} (max {exact(max.toString())})</span></div>
  <details className="provenance"><summary>Exact values</summary><div className="table-scroll"><table aria-label={`Exact ${unit} by weekday and hour`}><thead><tr><th>Weekday</th>{Array.from({length:24},(_,h)=><th key={h}>{String(h).padStart(2,'0')}</th>)}</tr></thead><tbody>{days.map((day,d)=><tr key={day}><td>{day}</td>{Array.from({length:24},(_,h)=>{const c=grid.get(`${d+1}|${h}`);return <td key={h}>{c?exact(c[measure]):'0'}</td>;})}</tr>)}</tbody></table></div></details>
 </div>;
}
