'use client';
import {useEffect,useId,useRef,type ReactNode} from 'react';
/** Modeless so another profile or filter remains reachable while detail is open. */
export default function ProfileDetail({profileRef,onClose,children}:{profileRef:string;onClose:()=>void;children:ReactNode}){
 const panel=useRef<HTMLElement>(null),heading=useId();
 useEffect(()=>{panel.current?.focus();},[profileRef]);
 return <section ref={panel} role="region" tabIndex={-1} aria-labelledby={heading} className="profile-detail" onKeyDown={e=>{if(e.key==='Escape'){e.stopPropagation();onClose();}}}><div className="section-heading"><h2 id={heading}>Governed profile timeline</h2><button className="secondary" onClick={onClose}>Close timeline</button></div>{children}</section>;
}
