import { NextRequest } from 'next/server';

function allowed(path:string,method:string):boolean {
  if(method==='GET'&&/^(auth\/session|ops\/(status|quality|lineage|audit|users|retention))$/.test(path))return true;
  if(method==='POST'&&/^(auth\/(login|logout)|ops\/(quality\/check|quality\/probe|quality\/probe\/clear|retention\/sessions))$/.test(path))return true;
  if(method==='PUT'&&/^ops\/users\/[a-f0-9-]{36}$/.test(path))return true;
  if(method==='GET' && /^lifecycle\/(cohorts|email-engagement|customer-mix|profiles|profiles\/[a-f0-9]{64}\/timeline|segments|segments\/[a-f0-9-]{36}\/versions\/[0-9]+)$/.test(path)) return true;
  if(method==='GET' && /^insights\/(activity|order-values|loyalty|channels)$/.test(path)) return true;
  if(method==='POST' && /^lifecycle\/segments(\/preview)?$/.test(path)) return true;
  if(method==='PUT' && /^lifecycle\/segments\/[a-f0-9-]{36}$/.test(path)) return true;
  if(method==='GET') return /^(dashboard|campaign-settings|alert-rules|alerts|health\/(live|ready)|budget-pacing\/[^/]+|analysis\/(funnel|attribution|journeys))$/.test(path);
  if(method==='POST') return /^(alert-rules|alerts\/evaluate|budget-simulation)$/.test(path);
  return method==='PUT' && /^(campaign-settings|alert-rules)\/[^/]+$/.test(path);
}
async function proxy(request:NextRequest,{params}:{params:Promise<{path:string[]}>}) {
  const {path}=await params;
  const route=path.map(encodeURIComponent).join('/');
  if(!allowed(route,request.method)) return Response.json({code:'unknown_route'},{status:404});
  const base=process.env.API_INTERNAL_URL || 'http://api:8000';
  try {
    const upstream=await fetch(`${base}/api/v1/${route}${request.nextUrl.search}`,{
      method:request.method,headers:{'Content-Type':'application/json',...(request.headers.get('cookie')?{'Cookie':request.headers.get('cookie')!}:{}),...(request.headers.get('origin')?{'Origin':request.headers.get('origin')!}:{})},
      body:['POST','PUT'].includes(request.method)?await request.text():undefined,
      cache:'no-store',signal:AbortSignal.timeout(15000),redirect:'error'});
    const headers=new Headers({'Content-Type':'application/json','Cache-Control':'no-store'});for(const cookie of upstream.headers.getSetCookie())headers.append('Set-Cookie',cookie);
    return new Response(await upstream.arrayBuffer(),{status:upstream.status,headers});
  } catch {return Response.json({code:'dependency_unavailable',message:'API unavailable'},{status:503,headers:{'Cache-Control':'no-store'}});}
}
export const GET=proxy;
export const POST=proxy;
export const PUT=proxy;
