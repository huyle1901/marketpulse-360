import type {ReactNode} from 'react';

export type Stat={name:string;label?:string;value:string;title?:string;hint?:ReactNode;extra?:ReactNode};

/** A ledger of figures separated by hairlines instead of a row of identical cards. */
export function StatStrip({label,items,className=''}:{label:string;items:Stat[];className?:string}){
 return <section aria-label={label} className={'grid grid-cols-2 gap-px overflow-hidden rounded-xl border bg-border md:grid-cols-3 '+className}>
  {items.map(item=><article key={item.name} role="article" aria-label={item.name} title={item.title} className="min-w-0 bg-card px-4 py-3.5">
   <h3 className="truncate text-[13px] font-normal text-muted-foreground">{item.label??item.name}</h3>
   <p className="figure mt-1 truncate text-[1.6rem] leading-tight text-foreground" title={item.title??item.value}>{item.value}</p>
   {item.extra}
   {item.hint!=null&&<p className="mt-1 truncate text-xs text-muted-foreground">{item.hint}</p>}
  </article>)}
 </section>;
}
