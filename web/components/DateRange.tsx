import {dayAfter,lastDay} from '../lib/format';

/** Certified data is partitioned by UTC day; this is stated once next to every date filter. */
export const DAY_RULE='Days are UTC calendar days (07:00–07:00 Vietnam time)';

/** From/To inputs that both include their day; the URL and API keep an exclusive end bound. */
export function DateRangeFields({from,toExclusive,onFrom,onTo}:{from:string;toExclusive:string;onFrom:(day:string)=>void;onTo:(exclusive:string)=>void}){
 return <>
  <label>From<input type="date" value={from} onChange={e=>onFrom(e.target.value)}/></label>
  <label>To<input type="date" value={lastDay(toExclusive)} min={from||undefined} onChange={e=>onTo(dayAfter(e.target.value))}/></label>
 </>;
}
