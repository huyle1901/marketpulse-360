'use client';
import {useState} from 'react';
import {exact} from '../lib/format';

export type Activity={status:string;message?:string;timezone:string;cells:{weekday:string;hour:string;sessions:string;orders:string}[];busiest:{weekday:string;hour:string;sessions:string}|null};
const days=['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'];
/** Validated sequential ramp (blue 150→700); zero uses the muted surface so "none" never reads as "some". */
const ramp=[1,2,3,4,5,6,7].map(step=>`var(--heat-${step})`);
const slot=(hour:number)=>`${String(hour).padStart(2,'0')}:00–${String((hour+1)%24).padStart(2,'0')}:00`;

// Vietnam has no daylight saving, so its hour buckets are the UTC buckets shifted by 7 hours, carrying into the next weekday.
const ZONES={vn:{label:'Vietnam time',shift:7},utc:{label:'UTC',shift:0}} as const;
const shifted=(weekday:number,hour:number,shift:number)=>{const h=hour+shift;return {weekday:h>=24?weekday%7+1:weekday,hour:h%24};};

export default function ActivityHeatmap({data}:{data:Activity}){
 const [measure,setMeasure]=useState<'sessions'|'orders'>('sessions');
 const [zone,setZone]=useState<keyof typeof ZONES>('vn');const {label:zoneLabel,shift}=ZONES[zone];
 const value=(c:Activity['cells'][number])=>BigInt(c[measure]);
 const max=data.cells.reduce((m,c)=>value(c)>m?value(c):m,0n),min=data.cells.reduce((m,c)=>value(c)<m?value(c):m,max);
 const grid=new Map(data.cells.map(c=>{const at=shifted(Number(c.weekday),Number(c.hour),shift);return [at.weekday+'|'+at.hour,c];}));
 // Steps span the observed min..max (stated in the legend) so near-uniform activity still reveals its pattern; a true zero stays muted.
 const tone=(v:bigint)=>v===0n||max===0n?'var(--muted)':ramp[Math.min(ramp.length-1,Number(((v-min)*BigInt(ramp.length))/(max-min+1n)))];
 const unit=measure==='sessions'?'sessions':'orders';
 const peak=data.busiest?shifted(Number(data.busiest.weekday),Number(data.busiest.hour),shift):null;
 const busiest=peak?`${days[peak.weekday-1]} ${slot(peak.hour)} ${zoneLabel}`:null;
 return <div className="space-y-3">
  <div className="flex flex-wrap items-center justify-between gap-3">
   <p className="text-sm">{busiest?<><span className="text-muted-foreground">Busiest: </span><strong>{busiest}</strong><span className="text-muted-foreground"> · {exact(data.busiest!.sessions)} sessions</span></>:'No sessions in this release'}</p>
   <div className="flex flex-wrap gap-2"><div className="metric-switch" role="group" aria-label="Time zone">{(Object.keys(ZONES) as (keyof typeof ZONES)[]).map(z=><button key={z} aria-pressed={zone===z} onClick={()=>setZone(z)}>{ZONES[z].label}</button>)}</div>
   <div className="metric-switch" role="group" aria-label="Heatmap measure"><button aria-pressed={measure==='sessions'} onClick={()=>setMeasure('sessions')}>Show sessions</button><button aria-pressed={measure==='orders'} onClick={()=>setMeasure('orders')}>Show orders</button></div></div>
  </div>
  <div className="overflow-x-auto">
   <table aria-label={`${measure==='sessions'?'Sessions':'Orders'} by weekday and hour (${zoneLabel})`} className="activity-grid w-full min-w-[640px] table-fixed border-separate border-spacing-[3px]">
    <thead><tr><th className="w-11 bg-transparent p-0"/>{Array.from({length:24},(_,h)=><th key={h} scope="col" className="bg-transparent p-0 text-center text-[10px] font-normal text-muted-foreground">{h%3===0?String(h).padStart(2,'0'):''}</th>)}</tr></thead>
    <tbody>{days.map((day,d)=><tr key={day}><th scope="row" className="bg-transparent p-0 pr-2 text-left text-xs font-medium text-muted-foreground">{day.slice(0,3)}</th>
     {Array.from({length:24},(_,h)=>{const cell=grid.get(`${d+1}|${h}`);const v=cell?value(cell):0n;const label=`${day} ${slot(h)} ${zoneLabel}: ${exact(v.toString())} ${unit}`;
      return <td key={h} aria-label={label} title={label} className="h-7 rounded-[4px] p-0 transition-transform hover:scale-110" style={{background:tone(v)}}/>;})}</tr>)}</tbody>
   </table>
  </div>
  <div className="flex items-center gap-2 text-xs text-muted-foreground" aria-hidden="true"><span>Fewer</span>{['var(--muted)',...ramp].map(c=><span key={c} className="h-3 w-5 rounded-sm" style={{background:c}}/>)}<span>More</span></div>
  <p className="text-xs text-muted-foreground">Color scale: {exact(min.toString())} to {exact(max.toString())} {unit} per hour</p>
  <details className="provenance"><summary>Exact values</summary><div className="table-scroll"><table aria-label={`Exact ${unit} by weekday and hour`}><thead><tr><th>Weekday</th>{Array.from({length:24},(_,h)=><th key={h}>{String(h).padStart(2,'0')}</th>)}</tr></thead><tbody>{days.map((day,d)=><tr key={day}><td>{day}</td>{Array.from({length:24},(_,h)=>{const c=grid.get(`${d+1}|${h}`);return <td key={h}>{c?exact(c[measure]):'0'}</td>;})}</tr>)}</tbody></table></div></details>
 </div>;
}
