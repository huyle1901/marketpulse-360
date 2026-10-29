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
 const unit=([[10n**12n,'T'],[10n**9n,'B'],[10n**6n,'M']] as const).find(([size])=>major>=size);
 if(!unit)return formatMoney(value,currency);
 const scaled=major*100n/unit[0],prefix=formatMoney('0',currency).replace(/0([.,]0+)?$/,'');
 return (negative?'-':'')+prefix+(scaled/100n).toLocaleString('en-US')+'.'+(scaled%100n).toString().padStart(2,'0')+unit[1];
}
