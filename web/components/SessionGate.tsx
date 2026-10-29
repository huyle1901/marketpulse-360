'use client';
import {createContext,useContext,useEffect,useRef,useState} from 'react';
import {usePathname,useRouter} from 'next/navigation';
import Link from 'next/link';
import {Activity,LayoutDashboard,Mail,Megaphone,LogOut,Radio,Route,ShieldCheck,Users} from 'lucide-react';
import {Button} from '@/components/ui/button';
import CohortHeatmap from './CohortHeatmap';
import type {Cohort} from '../app/customers/types';
type Principal={user_id:string;username:string;role:'viewer'|'operator'|'admin'};
const Context=createContext<{principal:Principal|null;canOperate:boolean;canAdmin:boolean}>({principal:null,canOperate:false,canAdmin:false});
export const useSession=()=>useContext(Context);
export default function SessionGate({children}:{children:React.ReactNode}){
 const [principal,setPrincipal]=useState<Principal|null>(null);const [loading,setLoading]=useState(true);const [error,setError]=useState('');const [retry,setRetry]=useState(0);
 const path=usePathname();const router=useRouter();
 const authEpoch=useRef(0);const checkSequence=useRef(0);
 useEffect(()=>{
  let active=true;const controller=new AbortController();
  const check=async()=>{
   const epoch=authEpoch.current;const sequence=++checkSequence.current;
   const current=()=>active&&epoch===authEpoch.current&&sequence===checkSequence.current;
   try{
    const r=await fetch('/api/v1/auth/session',{cache:'no-store',signal:controller.signal});
    if(!current())return;
    if(r.status===401){setPrincipal(null);setError('');}
    else if(!r.ok){setPrincipal(null);setError('Authentication service unavailable');}
    else{
     const value=await r.json();if(!current())return;
     if(!['viewer','operator','admin'].includes(value.role))throw new Error();
     setPrincipal(value);setError('');
    }
   }catch{if(current()&&!controller.signal.aborted){setPrincipal(null);setError('Authentication service unavailable');}}
   finally{if(current())setLoading(false);}
  };
  void check();const timer=setInterval(()=>void check(),15000);
  const expired=()=>{authEpoch.current++;setPrincipal(null);setLoading(false);setError('Session expired; sign in again');};
  window.addEventListener('mp-session-expired',expired);
  const original=window.fetch;
  window.fetch=async(...args)=>{
   const epoch=authEpoch.current;const r=await original(...args);const url=String(args[0]);
   if(epoch===authEpoch.current&&r.status===401&&url.startsWith('/api/v1/')&&!url.includes('/auth/'))window.dispatchEvent(new Event('mp-session-expired'));
   return r;
  };
  return()=>{active=false;authEpoch.current++;controller.abort();clearInterval(timer);window.removeEventListener('mp-session-expired',expired);window.fetch=original;};
 },[path,retry]);
 const canOperate=principal?.role==='operator'||principal?.role==='admin';const canAdmin=principal?.role==='admin';
 if(loading)return <main><p role="status">Checking session…</p></main>;
 if(!principal)return <Login error={error} onLogin={()=>{authEpoch.current++;setLoading(true);setRetry(v=>v+1);}}/>;
 if(path==='/login')return <main><h1>Signed in</h1><Link href="/">Open Command Center</Link></main>;
 const links:[string,string,typeof LayoutDashboard][]=[['/','Overview',LayoutDashboard],['/ads','Ad performance',Megaphone],['/live','Live activity',Radio],['/analysis','Journey analysis',Route],['/customers','Customers',Users],['/email','Email',Mail],...(canAdmin?[['/operations','Data health',ShieldCheck] as [string,string,typeof LayoutDashboard]]:[])];
 return <Context.Provider value={{principal,canOperate,canAdmin}}>
 <a className="skip-link" href="#main-content">Skip to content</a>
 <div className="min-h-screen md:grid md:grid-cols-[232px_minmax(0,1fr)]">
 <header className="flex min-w-0 max-w-full flex-col gap-3 overflow-hidden border-b bg-sidebar px-3 py-3 md:sticky md:top-0 md:h-screen md:gap-1 md:border-r md:border-b-0 md:py-5">
  <Link href="/" className="flex items-center gap-2 px-2 text-[15px] font-semibold tracking-tight" aria-label="MarketPulse 360 home"><span className="grid size-7 place-items-center rounded-md bg-primary text-primary-foreground" aria-hidden="true"><Activity className="size-4"/></span>MarketPulse <span className="font-normal text-muted-foreground">360</span></Link>
  <p className="mt-5 mb-1 hidden px-2 text-xs font-medium text-muted-foreground md:block">Workspaces</p>
  <nav aria-label="Workspaces" className="flex min-w-0 max-w-full gap-1 overflow-x-auto md:flex-col">{links.map(([href,label,Icon])=><Link key={href} href={href} aria-current={path===href?'page':undefined} className="flex shrink-0 items-center gap-2.5 rounded-md px-2.5 py-2 text-sm text-muted-foreground transition-colors hover:bg-sidebar-accent hover:text-foreground aria-[current=page]:bg-sidebar-accent aria-[current=page]:font-medium aria-[current=page]:text-foreground"><Icon className="size-4" aria-hidden="true"/>{label}</Link>)}</nav>
  <div className="flex items-center gap-2 md:mt-auto md:border-t md:px-1 md:pt-3"><span className="grid size-8 shrink-0 place-items-center rounded-full bg-muted text-xs font-semibold uppercase" aria-hidden="true">{principal.username.slice(0,2)}</span><span className="min-w-0 flex-1 truncate text-sm">{principal.username} · {principal.role}</span><Button variant="ghost" size="sm" onClick={async()=>{authEpoch.current++;try{const r=await fetch('/api/v1/auth/logout',{method:'POST'});if(!r.ok&&r.status!==401)throw new Error();authEpoch.current++;setPrincipal(null);router.replace('/login');}catch{setError('Sign out failed; retry');}}}><LogOut className="size-4" aria-hidden="true"/>Sign out</Button></div>{error&&<p role="alert" className="text-sm text-destructive">{error}</p>}</header>
 <div id="main-content" tabIndex={-1} className="min-w-0 outline-none">{path==='/operations'&&!canAdmin?<main><h1>Administrator access required</h1></main>:path==='/customers'&&!canOperate?<ViewerCohorts/>:children}</div></div></Context.Provider>;

}
function Login({error,onLogin}:{error:string;onLogin:()=>void}){
 const [name,setName]=useState('');const [password,setPassword]=useState('');const [message,setMessage]=useState('');const [busy,setBusy]=useState(false);
 return <main className="login"><div className="brand">MARKETPULSE <span>360</span></div><h1>Sign in to MarketPulse 360</h1><p>Use an operator-created application account.</p><form className="panel" onSubmit={async e=>{e.preventDefault();setBusy(true);setMessage('');try{const r=await fetch('/api/v1/auth/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({username:name,password})});const body=await r.json();if(!r.ok)throw new Error(body.detail||body.code||'Sign in failed');setPassword('');onLogin();}catch(e){setMessage(e instanceof Error?e.message:'Sign in failed');}finally{setBusy(false);}}}><label>Username<input autoComplete="username" maxLength={64} required value={name} onChange={e=>setName(e.target.value)}/></label><label>Password<input type="password" autoComplete="current-password" maxLength={128} required value={password} onChange={e=>setPassword(e.target.value)}/></label><button disabled={busy}>{busy?'Signing in…':'Sign in'}</button>{(message||error)&&<p role="alert" className="error">{message||error}</p>}</form></main>;
}
function ViewerCohorts(){
 const [data,setData]=useState<{rows:Cohort[];provenance:{identity:string}}|null>(null);const [error,setError]=useState('');
 useEffect(()=>{const c=new AbortController();void fetch('/api/v1/lifecycle/cohorts',{signal:c.signal,cache:'no-store'}).then(async r=>{if(!r.ok)throw new Error('Cohorts unavailable');setData(await r.json());}).catch(e=>{if(!c.signal.aborted)setError(e.message);});return()=>c.abort();},[]);
 return <main><h1>Cohort retention</h1><p>Profile access requires operator role</p>{error&&<p role="alert">{error}</p>}{data&&<section className="panel"><CohortHeatmap rows={data.rows}/><p className="source">Release {data.provenance.identity}</p></section>}</main>;
}
