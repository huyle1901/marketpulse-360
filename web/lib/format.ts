/** Exact formatting: monetary integers never pass through Number. */
export const exact=(value:string|null|undefined)=>value==null?'Unavailable':BigInt(value).toLocaleString('en-US');
export function formatMoney(value:string|null|undefined,currency:string):string {
 if(value==null)return 'Unavailable';
 const amount=BigInt(value),negative=amount<0n,absolute=negative?-amount:amount;
 const formatter=new Intl.NumberFormat('en-US',{style:'currency',currency});
 const digits=formatter.resolvedOptions().maximumFractionDigits??2;
 const scale=10n**BigInt(digits),whole=absolute/scale;
 const fraction=(absolute%scale).toString().padStart(digits,'0');
 return formatter.formatToParts(negative?(whole===0n?-0:-whole):whole).map(p=>p.type==='fraction'?fraction:p.value).join('');
}
export function roas(value:string|null):string {
 if(value==null)return 'Unavailable';
 const match=/^(-?)(\d+)(?:\.(\d+))?$/.exec(value);if(!match)return 'Unavailable';
 const fraction=(match[3]||'').padEnd(3,'0');
 const scaled=BigInt(match[2])*100n+BigInt(fraction.slice(0,2))+(fraction[2]>='5'?1n:0n);
 return (match[1]&&scaled!==0n?'-':'')+(scaled/100n).toLocaleString('en-US')+'.'+(scaled%100n).toString().padStart(2,'0')+'×';
}
export function changePercent(current:string,previous:string):string|null {
 const baseline=BigInt(previous);if(baseline<=0n)return null;
 const delta=(BigInt(current)-baseline)*10000n/baseline;
 return (delta<0n?'-':'+')+(delta<0n?-delta:delta)/100n+'.'+((delta<0n?-delta:delta)%100n).toString().padStart(2,'0')+'%';
}
/** Compact money for headline cards (truncated to two decimals in major units); the exact value belongs in a title. */
export function compactMoney(value:string|null|undefined,currency:string):string {
 if(value==null)return 'Unavailable';
 const digits=BigInt(new Intl.NumberFormat('en-US',{style:'currency',currency}).resolvedOptions().maximumFractionDigits??2);
 const amount=BigInt(value),negative=amount<0n,major=(negative?-amount:amount)/10n**digits;
 const units=[[10n**12n,'T'],[10n**9n,'B'],[10n**6n,'M']] as const;
 const index=units.findIndex(([size])=>major>=size);
 if(index<0)return formatMoney(value,currency);
 // Round half up to the hundredth of the unit (₫3,457,576,000 → ₫3.46B); 999.995B carries into 1.00T.
 const round=(size:bigint)=>((negative?-amount:amount)*100n+size*10n**digits/2n)/(size*10n**digits);
 let unit=units[index],scaled=round(unit[0]);
 if(scaled>=100000n&&index>0){unit=units[index-1];scaled=round(unit[0]);}
 const prefix=formatMoney('0',currency).replace(/0([.,]0+)?$/,'');
 return (negative?'-':'')+prefix+(scaled/100n).toLocaleString('en-US')+'.'+(scaled%100n).toString().padStart(2,'0')+unit[1];
}

const pad=(n:number)=>String(n).padStart(2,'0');
const MONTHS=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
/** A 0–1 fixed-point ratio ("0.848271") as a one-decimal percentage, rounded half up. */
export function share(value:string|null|undefined):string {
 const match=value==null?null:/^(\d+)\.(\d{1,6})$/.exec(value);if(!match)return 'Unavailable';
 const millionths=BigInt(match[1]+match[2].padEnd(6,'0')),tenths=(millionths+500n)/1000n;
 return (tenths/10n).toString()+'.'+(tenths%10n).toString()+'%';
}
/** Fixed-point counts without trailing zeros: "1.000000" → "1", "0.250000" → "0.25". */
export function count(value:string|null|undefined):string {
 if(value==null)return 'Unavailable';const [whole,fraction='']=value.split('.');const trimmed=fraction.replace(/0+$/,'');
 return BigInt(whole).toLocaleString('en-US')+(trimmed?'.'+trimmed:'');
}
// Vietnam has no daylight saving, so GMT+7 is a fixed shift; certified days stay UTC calendar days.
const VIETNAM_MS=7*3600*1000;
const parse=(iso:string|null|undefined)=>{const time=iso?new Date(iso):null;return time&&!Number.isNaN(time.getTime())?time:null;};
const ymd=(day:string)=>/^(\d{4})-(\d{2})-(\d{2})/.exec(day);
/** A calendar day, "31 Aug 2026". */
export function dayLabel(day:string|null|undefined):string {
 const m=day?ymd(day):null;return m?`${Number(m[3])} ${MONTHS[Number(m[2])-1]} ${m[1]}`:'Unavailable';
}
/** A month, "Jan 2023" (any day of the month may be given). */
export function monthName(day:string|null|undefined):string {
 const m=day?ymd(day):null;return m?`${MONTHS[Number(m[2])-1]} ${m[1]}`:'Unavailable';
}
const shiftDay=(day:string,delta:number)=>new Date(Date.parse(day.slice(0,10)+'T00:00:00Z')+delta*86400000).toISOString().slice(0,10);
/** The last day included by an exclusive end bound ("2026-09-01" → "2026-08-31"). */
export const lastDay=(exclusive:string)=>exclusive?shiftDay(exclusive,-1):'';
/** The exclusive end bound for an inclusive last day ("2026-08-31" → "2026-09-01"). */
export const dayAfter=(inclusive:string)=>inclusive?shiftDay(inclusive,1):'';
/** An inclusive day range from an exclusive end: "3 Jun – 31 Aug 2026"; the year shows once when shared. */
export function dayRange(from:string,toExclusive:string):string {
 const last=lastDay(toExclusive),a=ymd(from),b=ymd(last);if(!a||!b)return 'Unavailable';
 if(from===last)return dayLabel(from);
 return a[1]===b[1]?`${Number(a[3])} ${MONTHS[Number(a[2])-1]} – ${dayLabel(last)}`:`${dayLabel(from)} – ${dayLabel(last)}`;
}
/** An instant in Vietnam time, "1 Nov 2026, 09:57 (GMT+7)"; seconds on request. */
export function localTime(iso:string|null|undefined,{seconds=false}:{seconds?:boolean}={}):string {
 const time=parse(iso);if(!time)return 'Unavailable';const t=new Date(time.getTime()+VIETNAM_MS);
 return `${t.getUTCDate()} ${MONTHS[t.getUTCMonth()]} ${t.getUTCFullYear()}, ${pad(t.getUTCHours())}:${pad(t.getUTCMinutes())}${seconds?':'+pad(t.getUTCSeconds()):''} (GMT+7)`;
}
/** Time of day in Vietnam time, "09:57:05 (GMT+7)". */
export function localClock(iso:string|null|undefined):string {
 const time=parse(iso);if(!time)return 'Unavailable';const t=new Date(time.getTime()+VIETNAM_MS);
 return `${pad(t.getUTCHours())}:${pad(t.getUTCMinutes())}:${pad(t.getUTCSeconds())} (GMT+7)`;
}
/** Whole seconds as the two largest units: "12 s", "4 min", "9 h 56 min", "2 d 3 h". */
export function elapsed(seconds:string|number|null|undefined):string {
 const s=Number(seconds);if(seconds==null||!Number.isFinite(s)||s<0)return 'Unavailable';
 const d=Math.floor(s/86400),h=Math.floor(s%86400/3600),m=Math.floor(s%3600/60);
 return d?`${d} d${h?` ${h} h`:''}`:h?`${h} h${m?` ${m} min`:''}`:m?`${m} min`:`${Math.floor(s)} s`;
}
const CHANNELS:Record<string,string>={google:'Google',meta:'Meta',tiktok:'TikTok',unattributed:'Unattributed'};
export const channelName=(key:string|null|undefined)=>key?CHANNELS[key]??key:'Unattributed';
/** A "data through" bound: a midnight bound means through the previous day ("31 Aug 2026"); otherwise the instant. */
export function throughLabel(iso:string|null|undefined):string {
 if(!iso)return 'no data';
 return /T00:00:00(\.0+)?(Z|\+00:00)$/.test(iso)||/^\d{4}-\d{2}-\d{2}$/.test(iso)?dayLabel(lastDay(iso.slice(0,10))):localTime(iso);
}
