'use client';
import {useCallback,useEffect,useRef,useState,type Dispatch,type SetStateAction} from 'react';

/** Allowlisted, non-sensitive view state. Consumers select API request keys separately. */
export function useUrlState<T extends Record<string,string>>(defaults:T,normalize?:(value:T)=>T):[T,Dispatch<SetStateAction<T>>,Dispatch<SetStateAction<T>>]{
 const config=useRef({defaults,normalize});config.current={defaults,normalize};
 const read=()=>{
  const values={...config.current.defaults};
  if(typeof window!=='undefined'){
   const params=new URLSearchParams(window.location.search);
   for(const key of Object.keys(values) as (keyof T)[])values[key]=(params.get(String(key))??values[key]) as T[keyof T];
  }
  return config.current.normalize?.(values)??values;
 };
 const [value,setValue]=useState<T>(read);
 const current=useRef(value);
 const write=(next:T,replace:boolean)=>{
  const params=new URLSearchParams();
  for(const key of Object.keys(config.current.defaults)){
   if(next[key]&&next[key]!==config.current.defaults[key])params.set(key,next[key]);
  }
  const query=params.toString();
  const url=window.location.pathname+(query?'?'+query:'')+window.location.hash;
  if(url===window.location.pathname+window.location.search+window.location.hash)return;
  if(replace)window.history.replaceState(null,'',url);else window.history.pushState(null,'',url);
 };
 // Write at the state transition, never from a delayed render effect that can race Back.
 const transition=useCallback((action:SetStateAction<T>,replace:boolean)=>{
  const draft=typeof action==='function'?action(current.current):action;
  const next=config.current.normalize?.(draft)??draft;
  current.current=next;write(next,replace);setValue(next);
 },[]);
 useEffect(()=>{
  const restore=()=>{const next=read();current.current=next;write(next,true);setValue(next);};
  window.addEventListener('popstate',restore);
  restore();
  return()=>window.removeEventListener('popstate',restore);
 },[]);
 const update:Dispatch<SetStateAction<T>>=useCallback(action=>transition(action,false),[transition]);
 const replace:Dispatch<SetStateAction<T>>=useCallback(action=>transition(action,true),[transition]);
 return [value,update,replace];
}
export const choice=(value:string,allowed:readonly string[],fallback='')=>allowed.includes(value)?value:fallback;
