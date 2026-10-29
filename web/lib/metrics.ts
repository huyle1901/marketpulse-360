/** Exact marketing KPI arithmetic on integer strings; display only, never Number for money. */
export type Totals = {currency:string;spend_minor:string;net_revenue_minor:string;orders:string;gross_revenue_minor?:string;clicks?:string;impressions?:string};

const big=(value:string|null|undefined)=>value==null?null:BigInt(value);

/** Signed percent change with one decimal, truncated toward zero; null without a positive baseline. */
export function percentChange(current:string|null|undefined,previous:string|null|undefined):string|null{
 const now=big(current),base=big(previous);if(now==null||base==null||base<=0n)return null;
 const scaled=(now-base)*1000n/base,absolute=scaled<0n?-scaled:scaled;
 return (scaled<0n?'-':scaled>0n?'+':'')+(absolute/10n).toString()+'.'+(absolute%10n).toString()+'%';
}

/** Integer quotient of two integer strings (e.g. AOV, cost per order) or null for a non-positive denominator. */
export function perUnit(numerator:string|null|undefined,denominator:string|null|undefined):string|null{
 const top=big(numerator),bottom=big(denominator);if(top==null||bottom==null||bottom<=0n)return null;
 return (top/bottom).toString();
}

/** A ratio such as net ROAS to two decimals with a trailing ×. */
export function multiple(numerator:string|null|undefined,denominator:string|null|undefined):string|null{
 const top=big(numerator),bottom=big(denominator);if(top==null||bottom==null||bottom<=0n)return null;
 const scaled=top*100n/bottom,absolute=scaled<0n?-scaled:scaled;
 return (scaled<0n?'-':'')+(absolute/100n).toLocaleString('en-US')+'.'+(absolute%100n).toString().padStart(2,'0')+'×';
}

/** Percent with two decimals for rates such as CTR. */
export function percent(numerator:string|null|undefined,denominator:string|null|undefined):string|null{
 const top=big(numerator),bottom=big(denominator);if(top==null||bottom==null||bottom<=0n)return null;
 const scaled=top*10000n/bottom;return (scaled/100n).toString()+'.'+(scaled%100n).toString().padStart(2,'0')+'%';
}
