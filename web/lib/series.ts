import type {DailyMetric} from '../app/types';

export type Granularity='daily'|'weekly'|'monthly';
export type Bucket={start:string;label:string;days:number;expected:number;spend_minor:string;net_revenue_minor:string;orders:string|null};
const DAY=86400000;
const stamp=(value:string)=>Date.parse(value+'T00:00:00Z');
const iso=(value:number)=>new Date(value).toISOString().slice(0,10);

/** Bucket start in UTC: the day itself, its Monday, or the first of its month. */
export function bucketStart(day:string,granularity:Granularity):string{
 if(granularity==='daily')return day;
 if(granularity==='monthly')return day.slice(0,8)+'01';
 const value=stamp(day);return iso(value-((new Date(value).getUTCDay()+6)%7)*DAY);
}
export function nextBucket(start:string,granularity:Granularity):string{
 if(granularity==='daily')return iso(stamp(start)+DAY);
 if(granularity==='weekly')return iso(stamp(start)+7*DAY);
 const value=new Date(stamp(start));return iso(Date.UTC(value.getUTCFullYear(),value.getUTCMonth()+1,1));
}
/** Long selections read better monthly; short ones keep daily detail. */
export const defaultGranularity=(from:string,to:string):Granularity=>{const days=(stamp(to)-stamp(from))/DAY;return days>92?'monthly':days>31?'weekly':'daily';};

/** Sums only observed certified rows; an unobserved bucket is absent, never zero. */
export function aggregate(rows:DailyMetric[],granularity:Granularity):Bucket[]{
 const buckets=new Map<string,{days:number;spend:bigint;net:bigint;orders:bigint|null}>();
 for(const row of rows){
  const key=bucketStart(row.event_date,granularity),bucket=buckets.get(key)||{days:0,spend:0n,net:0n,orders:0n};
  bucket.days++;bucket.spend+=BigInt(row.spend_minor);bucket.net+=BigInt(row.net_revenue_minor);bucket.orders=row.orders==null||bucket.orders==null?null:bucket.orders+BigInt(row.orders);buckets.set(key,bucket);
 }
 return [...buckets.entries()].sort(([a],[b])=>a.localeCompare(b)).map(([start,b])=>({start,days:b.days,spend_minor:b.spend.toString(),net_revenue_minor:b.net.toString(),orders:b.orders==null?null:b.orders.toString(),expected:(Date.parse(nextBucket(start,granularity)+'T00:00:00Z')-Date.parse(start+'T00:00:00Z'))/DAY,
  label:granularity==='monthly'?start.slice(0,7):granularity==='weekly'?'Week of '+start:start}));
}
