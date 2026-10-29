const DAY=86400000;
const iso=(stamp:number)=>new Date(stamp).toISOString().slice(0,10);
/** Snapshots after certification are the point of this page: run from a week before certified data to real UTC tomorrow. */
export function liveWindow(dataThrough:string|null|undefined,now:number):{from_date:string;to_date:string}{
 const today=Date.UTC(new Date(now).getUTCFullYear(),new Date(now).getUTCMonth(),new Date(now).getUTCDate());
 const end=today+DAY,through=dataThrough?Date.parse(dataThrough.slice(0,10)+'T00:00:00Z'):NaN;
 const start=Number.isFinite(through)?Math.min(through-7*DAY,end-DAY):end-7*DAY;
 return {from_date:iso(Math.max(start,end-731*DAY)),to_date:iso(end)};
}
