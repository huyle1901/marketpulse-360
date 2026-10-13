import { NextRequest } from 'next/server';

function allowed(path:string,method:string):boolean {
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
      method:request.method,headers:{'Content-Type':'application/json'},
      body:['POST','PUT'].includes(request.method)?await request.text():undefined,
      cache:'no-store',signal:AbortSignal.timeout(15000),redirect:'error'});
    return new Response(await upstream.arrayBuffer(),{status:upstream.status,headers:{'Content-Type':'application/json','Cache-Control':'no-store'}});
  } catch {return Response.json({code:'dependency_unavailable',message:'API unavailable'},{status:503,headers:{'Cache-Control':'no-store'}});}
}
export const GET=proxy;
export const POST=proxy;
export const PUT=proxy;
