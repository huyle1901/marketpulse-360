'use client';
import {useEffect,useRef,useState} from 'react';
import type {Condition,Rule,Segment,Preview} from './types';
import {api,digest,exact} from './helpers';
const fields=['recency_days','order_count','net_revenue_minor','session_count','event_count','add_to_cart_count','email_click_count'];
const fresh=():Rule=>({schema_version:'mp08-segment-v1',purpose:'activation',cohort_month:null,activity_month:null,profile_type:null,conditions:[]});
const order=(a:Condition,b:Condition)=>{const left=[a.field,a.op,a.value].join('|'),right=[b.field,b.op,b.value].join('|');return left<right?-1:left>right?1:0;};
export default function Segments({identity,onMismatch}:{identity:string;onMismatch:()=>void}){
 const [items,setItems]=useState<Segment[]>([]),[segmentId,setSegmentId]=useState(''),[headVersion,setHeadVersion]=useState('0'),[loadedVersion,setLoadedVersion]=useState('0');
 const [name,setName]=useState(''),[rule,setRule]=useState<Rule>(fresh),[inspectVersion,setInspectVersion]=useState('1');
 const [preview,setPreview]=useState<Preview|null>(null),[previewError,setPreviewError]=useState(''),[message,setMessage]=useState('');
 const [saving,setSaving]=useState(false),[loading,setLoading]=useState(false),[previewBusy,setPreviewBusy]=useState(false);
 const historical=segmentId!==''&&loadedVersion!==headVersion;
 const canonicalRule={...rule,conditions:[...rule.conditions].sort(order)};const ruleString=JSON.stringify(canonicalRule);
 const context=useRef('');context.current=JSON.stringify([identity,ruleString]);const request=useRef(0);const previewController=useRef<AbortController|null>(null);
 useEffect(()=>{void api<Segment[]>('segments').then(setItems).catch(e=>setMessage(e.message));return()=>previewController.current?.abort();},[]);
 useEffect(()=>{setPreview(null);setPreviewError('');setPreviewBusy(false);previewController.current?.abort();},[identity,ruleString]);
 const install=(item:Segment,head:string)=>{setSegmentId(item.segment_id);setHeadVersion(head);setLoadedVersion(item.version);setInspectVersion(item.version);setName(item.name);setRule(item.rule);setPreview(null);setMessage('');};
 const select=(value:string)=>{
  if(!value){setSegmentId('');setHeadVersion('0');setLoadedVersion('0');setName('');setRule(fresh());setMessage('');return;}
  const item=items.find(row=>row.segment_id===value);if(item)install(item,item.version);
 };
 const loadRevision=async()=>{
  setLoading(true);setMessage('');try{
   if(!/^[1-9][0-9]{0,18}$/.test(inspectVersion))throw new Error('Use a positive whole revision number');
   const result=await api<Segment>('segments/'+segmentId+'/versions/'+inspectVersion);install(result,headVersion);
  }catch(e){setMessage(e instanceof Error?e.message:'Revision unavailable');}finally{setLoading(false);}
 };
 const save=async()=>{
  setSaving(true);setMessage('');try{
   if(!name.trim())throw new Error('Enter a segment name');
   if(!/^(0|[1-9][0-9]{0,18})$/.test(headVersion))throw new Error('Reload the saved revision');
   // Keep the exact integer token even for a PostgreSQL version beyond JS safe integers.
   const body='{"name":'+JSON.stringify(name)+',"rule":'+ruleString+',"expected_version":'+headVersion+'}';
   const result=await api<Segment>('segments'+(segmentId?'/'+segmentId:''),undefined,body,segmentId?'PUT':'POST');
   install(result,result.version);setItems(current=>[...current.filter(row=>row.segment_id!==result.segment_id),result]);setMessage('Saved revision '+result.version);
  }catch(e){setMessage(e instanceof Error?e.message:'Save failed');}finally{setSaving(false);}
 };
 const runPreview=async()=>{
  previewController.current?.abort();const controller=new AbortController();previewController.current=controller;
  const current=context.current,serial=++request.current;setPreviewBusy(true);setPreview(null);setPreviewError('');
  try{
   const result=await api<Preview>('segments/preview',controller.signal,JSON.stringify({rule:canonicalRule}),'POST');
   const expected=await digest(canonicalRule);
   if(current!==context.current||serial!==request.current||controller.signal.aborted)return;
   if(result.provenance.identity!==identity){onMismatch();throw new Error('Dataset changed; refresh before previewing again.');}
   if(result.rule_digest!==expected)throw new Error('Preview rule differs from the current draft');
   setPreview(result);
  }catch(e){if(current===context.current&&!controller.signal.aborted)setPreviewError(e instanceof Error?e.message:'Preview unavailable');}
  finally{if(current===context.current&&serial===request.current)setPreviewBusy(false);}
 };
 const condition=(index:number,change:Partial<Condition>)=>setRule(current=>({...current,conditions:current.conditions.map((c,i)=>i===index?{...c,...change}:c)}));
 return <section className="panel"><div className="section-heading"><div><h2>Saved lifecycle segments</h2><p>Reproducible conjunctive rules · Definitions and immutable revisions in PostgreSQL</p></div><span className="badge neutral">{historical?'Historical revision '+loadedVersion+' · Read-only':'Operator-authored rules'}</span></div>
 <section className="rule-summary" aria-label="Segment rule summary" aria-live="polite"><h3>Rule at a glance</h3><p><strong>{rule.purpose==='activation'?'Activation preview':'Historical analytics'}</strong> · {rule.profile_type||'All identity types'}{rule.cohort_month?' · Cohort '+rule.cohort_month:''}{rule.activity_month?' · Active '+rule.activity_month:''}</p><p>{rule.conditions.length?rule.conditions.map(c=>c.field.replaceAll('_',' ')+' '+({gte:'at least',lte:'at most',eq:'equal to'}[c.op]||c.op)+' '+(c.value||'…')).join(' AND '):'All profiles in the selected scope'}</p>{rule.purpose==='activation'&&<p>Both certified consent flags required · Identified CRM match required</p>}<small>Membership is evaluated in preview. Saved segments contain versioned rules; previews show bounded samples.</small></section>
 <fieldset disabled={saving||loading} className="plain-fieldset">
 <div className="form-grid"><label>Saved segment<select value={segmentId} onChange={e=>select(e.target.value)}><option value="">New segment</option>{items.map(item=><option key={item.segment_id} value={item.segment_id}>{item.name} · v{item.version}</option>)}</select></label>{segmentId&&<label>Revision to inspect<input inputMode="numeric" value={inspectVersion} onChange={e=>setInspectVersion(e.target.value)}/></label>}</div>
 {segmentId&&<div className="actions"><button className="secondary" onClick={()=>void loadRevision()}>Load revision</button>{historical&&<button className="secondary" onClick={()=>select(segmentId)}>Edit latest revision</button>}</div>}
 <fieldset disabled={historical} className="plain-fieldset"><div className="form-grid">
 <label>Segment name<input maxLength={80} value={name} onChange={e=>setName(e.target.value)}/></label><label>Segment purpose<select value={rule.purpose} onChange={e=>setRule({...rule,purpose:e.target.value})}><option value="activation">Activation preview (consent gated)</option><option value="analytics">Historical analytics</option></select></label>
 <label>Rule cohort month<input type="month" value={rule.cohort_month?.slice(0,7)||''} onChange={e=>setRule({...rule,cohort_month:e.target.value?e.target.value+'-01':null})}/></label><label>Rule activity month<input type="month" value={rule.activity_month?.slice(0,7)||''} onChange={e=>setRule({...rule,activity_month:e.target.value?e.target.value+'-01':null})}/></label>
 <label>Rule profile type<select value={rule.profile_type||''} onChange={e=>setRule({...rule,profile_type:e.target.value||null})}><option value="">All profiles</option>{['customer','anonymous','session'].map(v=><option key={v}>{v}</option>)}</select></label></div>
 <p>All conditions must match. Empty activation rules still require an identified CRM match and both certified consent flags. Money thresholds are declared currency minor units.</p>
 {rule.conditions.map((item,index)=><div className="condition-row" key={index}><label>Condition {index+1} field<select value={item.field} onChange={e=>condition(index,{field:e.target.value})}>{fields.map(v=><option key={v}>{v}</option>)}</select></label><label>Condition {index+1} operator<select value={item.op} onChange={e=>condition(index,{op:e.target.value})}><option value="gte">At least</option><option value="lte">At most</option><option value="eq">Equals</option></select></label><label>Condition {index+1} whole value<input inputMode="numeric" value={item.value} onChange={e=>condition(index,{value:e.target.value})}/></label><button className="secondary" aria-label={'Remove condition '+(index+1)} onClick={()=>setRule({...rule,conditions:rule.conditions.filter((_,i)=>i!==index)})}>Remove</button></div>)}
 <button className="secondary" disabled={rule.conditions.length>=8} onClick={()=>setRule({...rule,conditions:[...rule.conditions,{field:fields.find(f=>!rule.conditions.some(c=>c.field===f&&c.op==='gte'))||'order_count',op:'gte',value:'1'}]})}>Add condition</button>
 </fieldset>
 <div className="actions"><button disabled={!identity||previewBusy} onClick={()=>void runPreview()}>Preview segment</button><button disabled={historical} onClick={()=>void save()}>Save segment</button></div>
 </fieldset>
 {message&&<p className="notice" role="status">{message}</p>}{previewError&&<p className="error" role="alert">{previewError}</p>}
 {previewBusy&&<p role="status">Evaluating current rule…</p>}
 {preview&&<div className="result"><h3>{exact(preview.matched_total)} matched · {exact(preview.excluded_from_activation_total)} excluded globally from activation</h3><p>Purpose: {preview.purpose} · {preview.rows.length} shown{preview.sample_truncated?' (sample limited)':''}. Preview performs no delivery or member persistence.</p><p>{preview.consent_policy}</p><ul>{preview.rows.map(row=><li key={row.profile_ref}>Profile {row.profile_ref.slice(0,12)} · {row.profile_type} · Net {exact(row.net_revenue_minor)} · {row.activation_eligible?'Activation eligible':'Analytics only'}</li>)}</ul><details className="provenance"><summary>Rule and evaluation provenance</summary><p>Rule {preview.rule_digest}</p><p>Evaluation {preview.evaluation_key}</p><p>Release {preview.provenance.identity} · As of {preview.provenance.as_of}</p></details></div>}
 </section>;
}
