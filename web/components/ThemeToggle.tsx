'use client';

import {useEffect,useState} from 'react';
import {Moon,Sun} from 'lucide-react';

/** Light/dark switch; the choice is a per-browser convenience, the system preference is the default. */
export function ThemeToggle(){
 const [dark,setDark]=useState<boolean|null>(null);
 useEffect(()=>{setDark(document.documentElement.classList.contains('dark'));},[]);
 const toggle=()=>{const next=!document.documentElement.classList.contains('dark');document.documentElement.classList.toggle('dark',next);
  try{localStorage.setItem('mp-theme',next?'dark':'light');}catch{}setDark(next);};
 const Icon=dark?Sun:Moon;
 return <button type="button" onClick={toggle} aria-label={dark?'Switch to light mode':'Switch to dark mode'} title={dark?'Switch to light mode':'Switch to dark mode'}
  className="inline-flex size-8 shrink-0 items-center justify-center rounded-md border border-sidebar-border bg-transparent p-0 text-sidebar-foreground hover:bg-sidebar-accent hover:text-foreground">
  <Icon className="size-4" aria-hidden="true"/></button>;
}
