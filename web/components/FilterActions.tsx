'use client';
import {useState} from 'react';
export default function FilterActions({through,onDates,onReset}:{through?:string|null;onDates?:(from:string,to:string)=>void;onReset:()=>void}){
 const [message,setMessage]=useState('');
 const preset=(days:number)=>{if(!through||!onDates)return;const end=through.slice(0,10),stamp=Date.parse(end+'T00:00:00Z');if(!Number.isFinite(stamp))return;onDates(new Date(Math.max(Date.UTC(1900,0,1),stamp-days*86400000)).toISOString().slice(0,10),end);};
 const copy=async()=>{try{await navigator.clipboard.writeText(window.location.href);setMessage('View link copied');}catch{setMessage('Could not copy; copy the URL from your address bar.');}};
 return <div className="filter-actions"><div className="actions">{onDates&&<><button className="secondary" disabled={!through} title="Ends at the certified data-through day; missing history stays unavailable" onClick={()=>preset(1)}>Last published day</button><button className="secondary" disabled={!through} title="Ends at the certified data-through day; missing history stays unavailable" onClick={()=>preset(7)}>Last 7 published days</button></>}<button className="secondary" onClick={onReset}>Reset filters</button><button className="secondary" onClick={()=>void copy()}>Copy view link</button></div>{message&&<p role="status" className="source">{message}</p>}</div>;
}
