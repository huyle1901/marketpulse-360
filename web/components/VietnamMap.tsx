'use client';

import {geoMercator,geoPath} from 'd3-geo';
import type {Feature} from 'geojson';
import vietnam from '../lib/vietnam.json';
import {ARCHIPELAGOS,CITY_COORDINATES} from '../lib/cities';

export type MapPoint={city:string;value:number;display:string;title:string};
const W=560,H=620,MAX_R=30;
// Mercator fitted to mainland Viet Nam plus Hoàng Sa and Trường Sa, so the archipelagos always stay in frame.
const projection=geoMercator().fitExtent([[20,20],[W-20,H-20]],{type:'MultiPoint',coordinates:[[102.1,8.2],[116.2,23.4]]});
const outline=geoPath(projection)(vietnam as Feature)??'';
const at=(lon:number,lat:number)=>projection([lon,lat])??[0,0];

/** Proportional-symbol map: circle area follows the value, largest drawn first, labels on leader lines. */
export function VietnamMap({name,points,active,onActive}:{name:string;points:MapPoint[];active:string|null;onActive:(city:string|null)=>void}){
 const max=Math.max(1,...points.map(p=>p.value));
 const radius=(value:number)=>Math.max(3,MAX_R*Math.sqrt(value/max));
 const ordered=[...points].sort((a,b)=>b.value-a.value);
 return <div role="img" aria-label={name+': '+points.map(p=>`${p.city} ${p.display}`).join(', ')} className="mx-auto w-full max-w-[560px]">
  <svg viewBox={`0 0 ${W} ${H}`} className="h-auto w-full" aria-hidden="true">
   <path data-country="VN" d={outline} fill="var(--muted)" stroke="var(--border)" strokeWidth={1.2} strokeLinejoin="round"/>
   {ARCHIPELAGOS.map(a=>{const [x,y]=at(a.lon,a.lat);return <g key={a.name}>
    {a.islands.map(([lon,lat],i)=>{const [ix,iy]=at(lon,lat);return <circle key={i} cx={ix} cy={iy} r={1.8} fill="var(--muted-foreground)" opacity={0.7}/>;})}
    <text x={x} y={y-16} textAnchor="middle" fontSize={11} fill="var(--muted-foreground)">{a.name} (Việt Nam)</text></g>;})}
   {ordered.map(p=>{const c=CITY_COORDINATES[p.city];const [x,y]=at(c.lon,c.lat);const r=radius(p.value);const left=c.label==='left';
    const end=left?x-r-26:x+r+26,on=active===p.city,dim=active!==null&&!on;
    return <g key={p.city} onMouseEnter={()=>onActive(p.city)} onMouseLeave={()=>onActive(null)} style={{cursor:'default'}}>
     <circle data-city={p.city} data-active={on?'true':'false'} cx={x} cy={y} r={r} fill="var(--primary)" fillOpacity={dim?0.18:0.42} stroke="var(--primary)" strokeWidth={on?3:1.5}><title>{p.title}</title></circle>
     <circle cx={x} cy={y} r={2.5} fill="var(--primary)"/>
     <line x1={left?x-r:x+r} y1={y} x2={end} y2={y} stroke="var(--muted-foreground)" strokeWidth={1} opacity={dim?0.35:0.8}/>
     <text x={left?end-4:end+4} y={y-3} textAnchor={left?'end':'start'} fontSize={14} fontWeight={600} fill="var(--foreground)" opacity={dim?0.45:1}>{p.city}</text>
     <text x={left?end-4:end+4} y={y+13} textAnchor={left?'end':'start'} fontSize={12} fill="var(--muted-foreground)" opacity={dim?0.45:1}>{p.display}</text>
    </g>;})}
  </svg>
 </div>;
}

/** Three reference circles (the maximum, half and a quarter) drawn on the map's area scale. */
export function SizeLegend({max,format}:{max:number;format:(value:number)=>string}){
 const steps=[max,max/2,max/4];
 return <ul aria-label="Bubble size legend" className="flex flex-wrap items-end gap-4 text-xs text-muted-foreground">
  {steps.map((value,i)=>{const r=Math.max(3,MAX_R*Math.sqrt(value/Math.max(1,max)));return <li key={i} className="flex items-end gap-1.5">
   <svg width={r*2+2} height={r*2+2} aria-hidden="true"><circle cx={r+1} cy={r+1} r={r} fill="var(--primary)" fillOpacity={0.42} stroke="var(--primary)" strokeWidth={1.2}/></svg>
   <span className="tabular-nums text-foreground">{format(value)}</span></li>;})}
 </ul>;
}
