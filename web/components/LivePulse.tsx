'use client';

import Link from 'next/link';
import {useEffect,useState} from 'react';
import {exact} from '../lib/format';

/** A small doorway to Live View; provisional numbers stay outside certified surfaces. */
export function LivePulse(){
 const [visitors,setVisitors]=useState<string|null>(null);
 useEffect(()=>{
  let active=true;const controller=new AbortController();
  const load=()=>fetch('/api/v1/live/view',{cache:'no-store',signal:controller.signal}).then(r=>r.ok?r.json():null)
   .then(body=>{if(active)setVisitors(body?.status==='available'&&typeof body.active?.visitors==='string'?body.active.visitors:null);}).catch(()=>{});
  void load();const timer=setInterval(load,10000);
  return()=>{active=false;controller.abort();clearInterval(timer);};
 },[]);
 if(visitors==null)return null;
 return <Link href="/live" className="inline-flex items-center gap-2 rounded-full border border-emerald-200 bg-emerald-50 px-3 py-1 text-xs font-medium text-emerald-800 no-underline hover:bg-emerald-100">
  <span className="size-2 animate-pulse rounded-full bg-emerald-500" aria-hidden/>{exact(visitors)} {visitors==='1'?'shopper':'shoppers'} on the store now · Live View</Link>;
}
