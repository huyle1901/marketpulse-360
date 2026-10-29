import type {Metric} from '../app/types';
import {formatMoney} from '../lib/format';

const labels:Record<string,string>={google:'Google',meta:'Meta',tiktok:'TikTok'};

export default function ChannelBreakdown({rows}:{rows:Metric[]}){
  const currencies=[...new Set(rows.map(row=>row.currency))];
  return <section className="panel channel-panel" aria-label="Channel comparison">
    <div className="section-heading"><div><h2>Where revenue comes from</h2><p>Certified net revenue by source campaign channel</p></div></div>
    {!rows.length&&<p className="empty">No published campaigns for this selection.</p>}
    {currencies.map(currency=>{
      const byChannel=new Map<string,bigint>();
      for(const row of rows.filter(item=>item.currency===currency)) byChannel.set(row.platform,(byChannel.get(row.platform)||0n)+BigInt(row.net_revenue_minor));
      const values=[...byChannel.values()];
      const max=values.reduce((current,value)=>value<0n?(-value>current?-value:current):(value>current?value:current),1n);
      return <div key={currency} className="channel-currency"><h3>{currency}</h3>{[...byChannel.entries()].sort((a,b)=>a[0].localeCompare(b[0])).map(([platform,value])=>
        <div className="channel-row" key={platform}>
          <span className={'channel-platform '+platform}>{labels[platform]||platform}</span>
          <div className="channel-track" aria-hidden="true"><span className={value<0n?'negative':''} style={{width:Math.max(value===0n?0:3,Number((value<0n?-value:value)*100n/max))+'%'}}/></div>
          <strong>{formatMoney(value.toString(),currency)}</strong>
        </div>)}<p className="source">Source campaign finance · attribution credit is shown in Analysis.</p></div>;
    })}
  </section>;
}
