'use client';

import {geoMercator,geoPath} from 'd3-geo';
import type {Feature} from 'geojson';
import vietnam from '../lib/vietnam.json';
import {ARCHIPELAGOS,CITY_COORDINATES} from '../lib/cities';

export type MapPoint={city:string;value:number;display:string;title:string;region:string;color:string};
const W=560,H=620,MAX_R=30,CALLOUT={width:150,height:44};
// Mercator fitted to mainland Viet Nam plus Hoàng Sa and Trường Sa, so the archipelagos always stay in frame.
const projection=geoMercator().fitExtent([[20,20],[W-20,H-20]],{type:'MultiPoint',coordinates:[[102.1,8.2],[116.2,23.4]]});
const outline=geoPath(projection)(vietnam as Feature)??'';
const at=(lon:number,lat:number)=>projection([lon,lat])??[0,0];
const key=(region:string)=>region.toLowerCase().replace(/[^a-z]/g,'');

/**
 * Proportional-symbol map. Circle area follows the value and the colour names the region; each
 * bubble is a soft halo, a lit disc and a surface ring so overlapping circles stay separable.
 * Callouts sit at the end of leader lines for the largest cities (others on hover); hovering one city fades the others.
 */
export function VietnamMap({name,points,active,onActive,callouts=6}:{name:string;points:MapPoint[];active:string|null;onActive:(city:string|null)=>void;callouts?:number}){
 const max=Math.max(1,...points.map(p=>p.value));
 const radius=(value:number)=>Math.max(3,MAX_R*Math.sqrt(value/max));
 const ordered=[...points].sort((a,b)=>b.value-a.value);
 const labelled=new Set(ordered.slice(0,callouts).map(p=>p.city));
 const regions=[...new Map(points.map(p=>[p.region,p.color])).entries()];
 return <div role="img" aria-label={name+': '+points.map(p=>`${p.city} ${p.display}`).join(', ')} className="vn-map mx-auto w-full max-w-[560px]">
  <svg viewBox={`0 0 ${W} ${H}`} className="h-auto w-full" aria-hidden="true">
   <defs>
    {regions.map(([region,color])=><g key={region}>
     <radialGradient id={'vn-glow-'+key(region)}><stop offset="45%" style={{stopColor:color,stopOpacity:0.35}}/><stop offset="100%" style={{stopColor:color,stopOpacity:0}}/></radialGradient>
     <radialGradient id={'vn-fill-'+key(region)} cx="35%" cy="30%" r="75%"><stop offset="0%" style={{stopColor:`color-mix(in oklab, ${color} 55%, white)`}}/><stop offset="100%" style={{stopColor:color}}/></radialGradient>
    </g>)}
   </defs>
   <path data-country="VN" d={outline} fill="var(--muted)" stroke="var(--border)" strokeWidth={1.2} strokeLinejoin="round"/>
   {ARCHIPELAGOS.map(a=>{const [x,y]=at(a.lon,a.lat);return <g key={a.name}>
    {a.islands.map(([lon,lat],i)=>{const [ix,iy]=at(lon,lat);return <circle key={i} cx={ix} cy={iy} r={1.8} fill="var(--muted-foreground)" opacity={0.7}/>;})}
    <text x={x} y={y-16} textAnchor="middle" className="vn-map-place">{a.name} (Việt Nam)</text></g>;})}
   {ordered.map((p,index)=>{const c=CITY_COORDINATES[p.city];const [x,y]=at(c.lon,c.lat);const r=radius(p.value);const left=c.label==='left';
    const shown=labelled.has(p.city)||active===p.city,dy=c.dy,cy=y+dy;
    const end=left?x-r-22:x+r+22,on=active===p.city,dim=active!==null&&!on,k=key(p.region);
    return <g key={p.city} className="vn-map-city" style={{opacity:dim?0.35:1}} onMouseEnter={()=>onActive(p.city)} onMouseLeave={()=>onActive(null)}>
     <circle cx={x} cy={y} r={r*1.9} fill={`url(#vn-glow-${k})`} className="vn-map-halo"/>
     {index===0&&<circle cx={x} cy={y} r={r} fill="none" style={{stroke:p.color}} strokeWidth={1.5} className="vn-map-pulse"/>}
     <circle data-city={p.city} data-region={p.region} data-active={on?'true':'false'} cx={x} cy={y} r={r} fill={`url(#vn-fill-${k})`} fillOpacity={0.88}
      style={{stroke:p.color,animationDelay:`${index*90}ms`}} strokeWidth={on?3:1.5} className="vn-map-bubble"><title>{p.title}</title></circle>
     <circle cx={x} cy={y} r={r+1.5} fill="none" stroke="var(--card)" strokeWidth={1.5} opacity={0.9} pointerEvents="none"/>
     {shown&&<><line x1={left?x-r:x+r} y1={y} x2={end} y2={cy} style={{stroke:p.color}} strokeWidth={1.2} opacity={0.7}/>
     <foreignObject x={left?end-CALLOUT.width:end} y={cy-CALLOUT.height/2} width={CALLOUT.width} height={CALLOUT.height} overflow="visible">
      <div data-callout={p.city} className={'vn-map-callout '+(left?'is-left':'')}>
       <span className="vn-map-callout-dot" style={{background:p.color}} aria-hidden="true"/>
       <span className="min-w-0"><span className="block truncate text-[12.5px] font-semibold leading-tight text-foreground">{p.city}</span><span className="block text-[11.5px] leading-tight text-muted-foreground tabular-nums">{p.display}</span></span>
      </div>
     </foreignObject></>}
    </g>;})}
  </svg>
 </div>;
}

/** Three reference circles (the maximum, half and a quarter) drawn on the map's area scale. */
export function SizeLegend({max,format}:{max:number;format:(value:number)=>string}){
 const steps=[max,max/2,max/4];
 return <ul aria-label="Bubble size legend" className="flex flex-wrap items-end gap-4 text-xs text-muted-foreground">
  {steps.map((value,i)=>{const r=Math.max(3,MAX_R*Math.sqrt(value/Math.max(1,max)));return <li key={i} className="flex items-end gap-1.5">
   <svg width={r*2+2} height={r*2+2} aria-hidden="true"><circle cx={r+1} cy={r+1} r={r} fill="var(--muted)" stroke="var(--muted-foreground)" strokeWidth={1} strokeDasharray="3 2"/></svg>
   <span className="tabular-nums text-foreground">{format(value)}</span></li>;})}
 </ul>;
}
