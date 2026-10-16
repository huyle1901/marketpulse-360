'use client';
import {createContext,useContext,useEffect,useState} from 'react';
import {usePathname,useRouter} from 'next/navigation';
import Link from 'next/link';
type Principal={user_id:string;username:string;role:'viewer'|'operator'|'admin'};
const Context=createContext<{principal:Principal|null;canOperate:boolean;canAdmin:boolean}>({principal:null,canOperate:false,canAdmin:false});
export const useSession=()=>useContext(Context);
export default function SessionGate({children}:{children:React.ReactNode}){
 const [principal,setPrincipal]=useState<Principal|null>(null);const [loading,setLoading]=useState(true);const [error,setError]=useState('');const [retry,setRetry]=useState(0);
 const path=usePathname();const router=useRouter();
 useEffect(()=>{
  let active=true;const controller=new AbortController();
  const check=async()=>{try{const r=await fetch('/api/v1/auth/session',{cache:'no-store',signal:controller.signal});if(!active)return;if(r.status===401){setPrincipal(null);setError('');}else if(!r.ok){setPrincipal(null);setError('Authentication service unavailable');}else{const value=await r.json();if(!active)return;if(!['viewer','operator','admin'].includes(value.role))throw new Error();setPrincipal(value);setError('');}}catch{if(active&&!controller.signal.aborted){setPrincipal(null);setError('Authentication service unavailable');}}finally{if(active)setLoading(false);}};
  void check();const timer=setInterval(()=>void check(),15000);const expired=()=>{setPrincipal(null);setError('Session expired; sign in again');};window.addEventListener('mp-session-expired',expired);
  const original=window.fetch;window.fetch=async(...args)=>{const r=await original(...args);const url=String(args[0]);if(r.status===401&&url.startsWith('/api/v1/')&&!url.includes('/auth/'))window.dispatchEvent(new Event('mp-session-expired'));return r;};
  return()=>{active=false;controller.abort();clearInterval(timer);window.removeEventListener('mp-session-expired',expired);window.fetch=original;};
 },[path,retry]);
 const canOperate=principal?.role==='operator'||principal?.role==='admin';const canAdmin=principal?.role==='admin';
 if(loading)return <main><p role="status">Checking session…</p></main>;
 if(!principal)return <Login error={error} onLogin={()=>setRetry(v=>v+1)}/>;
 if(path==='/login')return <main><h1>Signed in</h1><Link href="/">Open Command Center</Link></main>;
 return <Context.Provider value={{principal,canOperate,canAdmin}}><div className="session-bar"><span>{principal.username} · {principal.role}</span><nav aria-label="Session navigation"><Link href="/">Campaigns</Link><Link href="/analysis">Analysis</Link><Link href="/customers">Customers</Link>{canAdmin&&<Link href="/operations">Operations</Link>}<button className="secondary" onClick={async()=>{try{const r=await fetch('/api/v1/auth/logout',{method:'POST'});if(!r.ok&&r.status!==401)throw new Error();setPrincipal(null);router.replace('/login');}catch{setError('Sign out failed; retry');}}}>Sign out</button></nav>{error&&<p role="alert">{error}</p>}</div>{path==='/operations'&&!canAdmin?<main><h1>Administrator access required</h1></main>:path==='/customers'&&!canOperate?<ViewerCohorts/>:children}</Context.Provider>;
}
function Login({error,onLogin}:{error:string;onLogin:()=>void}){
 const [name,setName]=useState('');const [password,setPassword]=useState('');const [message,setMessage]=useState('');const [busy,setBusy]=useState(false);
 return <main className="login"><div className="brand">MARKETPULSE <span>360</span></div><h1>Sign in to MarketPulse 360</h1><p>Use an operator-created application account.</p><form className="panel" onSubmit={async e=>{e.preventDefault();setBusy(true);setMessage('');try{const r=await fetch('/api/v1/auth/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({username:name,password})});const body=await r.json();if(!r.ok)throw new Error(body.detail||body.code||'Sign in failed');setPassword('');onLogin();}catch(e){setMessage(e instanceof Error?e.message:'Sign in failed');}finally{setBusy(false);}}}><label>Username<input autoComplete="username" maxLength={64} required value={name} onChange={e=>setName(e.target.value)}/></label><label>Password<input type="password" autoComplete="current-password" maxLength={128} required value={password} onChange={e=>setPassword(e.target.value)}/></label><button disabled={busy}>{busy?'Signing in…':'Sign in'}</button>{(message||error)&&<p role="alert" className="error">{message||error}</p>}</form></main>;
}
function ViewerCohorts(){
 const [data,setData]=useState<{rows:Record<string,string>[];provenance:{identity:string}}|null>(null);const [error,setError]=useState('');
 useEffect(()=>{const c=new AbortController();void fetch('/api/v1/lifecycle/cohorts',{signal:c.signal,cache:'no-store'}).then(async r=>{if(!r.ok)throw new Error('Cohorts unavailable');setData(await r.json());}).catch(e=>{if(!c.signal.aborted)setError(e.message);});return()=>c.abort();},[]);
 return <main><h1>Cohort retention</h1><p>Profile access requires operator role</p>{error&&<p role="alert">{error}</p>}{data&&<section className="panel"><div className="table-scroll"><table><thead><tr>{['Cohort','Activity','Identity','Cohort size','Active','Retention'].map(v=><th key={v}>{v}</th>)}</tr></thead><tbody>{data.rows.map((r,i)=><tr key={i}>{['cohort_month','activity_month','profile_type','cohort_size','active_profiles','retained_rate'].map(k=><td key={k}>{r[k]}</td>)}</tr>)}</tbody></table></div><p className="source">Release {data.provenance.identity}</p></section>}</main>;
}
