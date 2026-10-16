export const exact=(value:string|null|undefined)=>value==null?'Unavailable':BigInt(value).toLocaleString('en-US');
export const consent=(value:boolean|null)=>value===true?'Granted':value===false?'Denied':'Unknown';
export async function api<T>(path:string,signal?:AbortSignal,body?:string,method='GET'):Promise<T>{
 const response=await fetch('/api/v1/lifecycle/'+path,{cache:'no-store',signal,method,body,headers:{'Content-Type':'application/json'}});
 const data=await response.json();if(!response.ok)throw new Error(data.message||data.detail||data.code||'Request failed');return data;
}
export function canonical(value:unknown):string{
 if(Array.isArray(value))return '['+value.map(canonical).join(',')+']';
 if(value&&typeof value==='object')return '{'+Object.keys(value).sort().map(k=>JSON.stringify(k)+':'+canonical((value as Record<string,unknown>)[k])).join(',')+'}';
 return JSON.stringify(value);
}
export async function digest(value:unknown):Promise<string>{
 const result=await crypto.subtle.digest('SHA-256',new TextEncoder().encode(canonical(value)));return [...new Uint8Array(result)].map(b=>b.toString(16).padStart(2,'0')).join('');
}
