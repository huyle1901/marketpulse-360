import type {Metric} from '../app/types';
import {compactMoney,formatMoney} from '../lib/format';
import {Donut} from './charts';

const labels:Record<string,string>={google:'Google',meta:'Meta',tiktok:'TikTok',unattributed:'Unattributed'};
const colors:Record<string,string>={google:'var(--chart-1)',meta:'var(--chart-2)',tiktok:'var(--chart-3)',unattributed:'var(--chart-4)'};

export default function ChannelBreakdown({rows}:{rows:Metric[]}){
  const currencies=[...new Set(rows.map(row=>row.currency))];
  return <section className="panel channel-panel" aria-label="Channel comparison">
    <div className="section-heading"><div><h2>Where revenue comes from</h2><p>Certified net revenue by source campaign channel</p></div></div>
    {!rows.length&&<p className="empty">No published campaigns for this selection.</p>}
    {currencies.map(currency=>{
      const byChannel=new Map<string,bigint>();
      for(const row of rows.filter(item=>item.currency===currency)) {const channel=row.platform||'unattributed';byChannel.set(channel,(byChannel.get(channel)||0n)+BigInt(row.net_revenue_minor));}
      const entries=[...byChannel.entries()].sort((a,b)=>a[0].localeCompare(b[0]));
      const positive=entries.reduce((sum,[,value])=>sum+(value>0n?value:0n),0n);
      const negative=entries.filter(([,value])=>value<0n);
      // A donut cannot draw a negative part; such channels are named below it with their exact amount.
      return <div key={currency} className="channel-currency">
        <Donut name={'Net revenue by channel · '+currency} total={compactMoney(positive.toString(),currency)} caption={'net revenue · '+currency} size={132}
          slices={entries.map(([platform,value])=>({key:platform,label:labels[platform]||platform,value:value>0n?Number(value):0,color:colors[platform]||'var(--chart-5)',display:formatMoney(value.toString(),currency)}))}/>
        {negative.length>0&&<p className="source">Negative net revenue, not drawn: {negative.map(([platform,value])=>`${labels[platform]||platform} ${formatMoney(value.toString(),currency)}`).join(', ')}.</p>}
        <p className="source">Source campaign finance · attribution credit is shown in Analysis.</p></div>;
    })}
  </section>;
}
