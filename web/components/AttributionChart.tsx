import {formatMoney} from '../lib/format';
import type {Credit,Model} from '../app/analysis/types';

export const modelNames:Record<Model,string>={first_touch:'First touch',last_touch:'Last touch',linear:'Linear'};
const models=Object.keys(modelNames) as Model[];
const paid=['google','meta','tiktok'];
const channelName=(channel?:string)=>channel==='unattributed'?'Unattributed':channel||'Unknown';
const abs=(value:bigint)=>value<0n?-value:value;

/** Signed horizontal bar on a shared zero baseline; negative credit extends left of zero. */
function Bar({value,negMax,posMax,className}:{value:bigint;negMax:bigint;posMax:bigint;className:string}){
 const span=negMax+posMax||1n,zero=Number(negMax*10000n/span)/100,width=Number(abs(value)*10000n/span)/100;
 return <div className="credit-track"><span className="credit-zero" style={{left:zero+'%'}}/><span className={'credit-bar '+className+(value<0n?' negative':'')} style={{left:(value<0n?zero-width:zero)+'%',width:(value===0n?0:Math.max(width,0.6))+'%'}}/></div>;
}
const bounds=(values:bigint[])=>({negMax:values.reduce((m,v)=>v<0n&&-v>m?-v:m,0n),posMax:values.reduce((m,v)=>v>m?v:m,0n)});

export default function AttributionChart({channels,campaigns,currency,model,onModel,onChannel,onCampaign}:{channels:Credit[];campaigns:Credit[];currency:string;model:Model;onModel:(m:Model)=>void;onChannel:(c:string)=>void;onCampaign:(id:string)=>void}){
 const money=(value:string)=>formatMoney(value,currency);
 const keys=[...new Set(channels.map(row=>row.channel||''))].sort((a,b)=>(paid.indexOf(a)<0?9:paid.indexOf(a))-(paid.indexOf(b)<0?9:paid.indexOf(b))||a.localeCompare(b));
 const net=(rows:Credit[],m:Model,match:(r:Credit)=>boolean)=>rows.find(r=>r.model===m&&match(r))?.net_revenue_minor??'0';
 const channelScale=bounds(channels.map(r=>BigInt(r.net_revenue_minor)));
 const selected=campaigns.filter(r=>r.model===model).sort((a,b)=>{const x=BigInt(a.net_revenue_minor),y=BigInt(b.net_revenue_minor);return x===y?(a.campaign_name||'').localeCompare(b.campaign_name||''):x>y?-1:1;});
 const shown=selected.slice(0,12),campaignScale=bounds(shown.map(r=>BigInt(r.net_revenue_minor)));
 return <>
  <div className="metric-switch model-switch" role="group" aria-label="Attribution model">{models.map(m=><button key={m} aria-pressed={model===m} onClick={()=>onModel(m)}>{modelNames[m]}</button>)}</div>
  <div className="credit-charts">
  <section className="credit-chart" aria-label="Attribution credit by channel"><h3>Net credit by channel · {currency}</h3>
   <p className="source">Attribution credit, not source campaign finance. Bars share one zero baseline; refunds can make credit negative.</p>
   <ul className="credit-legend" aria-hidden="true">{models.map(m=><li key={m}><span className={'swatch '+m}/>{modelNames[m]}</li>)}</ul>
   {keys.map(key=>{const values=models.map(m=>net(channels,m,r=>r.channel===key));return <div className="credit-group" key={key}>
    {paid.includes(key)?<button className="link-button" aria-label={'Filter to channel '+key} onClick={()=>onChannel(key)}>{key}</button>:<strong>{channelName(key)}</strong>}
    <div role="img" aria-label={channelName(key)+': '+models.map((m,i)=>modelNames[m]+' '+money(values[i])).join(', ')}>{models.map((m,i)=><div className={'credit-line'+(m===model?' selected':'')} key={m}><Bar value={BigInt(values[i])} className={m} {...channelScale}/><small>{money(values[i])}</small></div>)}</div>
   </div>;})}
  </section>
  <section className="credit-chart" aria-label="Campaign credit"><h3>Campaign credit · {modelNames[model]}</h3>
   <p className="source">Top {shown.length} of {selected.length} credited campaigns by net credit.</p>
   {!shown.length?<p className="empty">No campaign credit for this model.</p>:shown.map(row=><div className="credit-group compact" key={row.campaign_id}>
    <button className="link-button" aria-label={'Explore campaign '+row.campaign_name} onClick={()=>onCampaign(row.campaign_id||'')}>{row.campaign_name}</button>
    <div className="credit-line selected"><Bar value={BigInt(row.net_revenue_minor)} className={model} {...campaignScale}/><small>{money(row.net_revenue_minor)}</small></div>
   </div>)}
  </section>
  </div>
 </>;
}
