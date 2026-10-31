import type { Metadata } from 'next';
import { Geist, Geist_Mono } from 'next/font/google';
import './globals.css';
import SessionGate from '../components/SessionGate';
const sans=Geist({subsets:['latin','latin-ext','vietnamese'],variable:'--font-geist'});
const mono=Geist_Mono({subsets:['latin','latin-ext'],variable:'--font-geist-mono'});
// Applies the saved or system theme before first paint so there is no light/dark flash.
const THEME="try{var t=localStorage.getItem('mp-theme');if(t==='dark'||(!t&&matchMedia('(prefers-color-scheme: dark)').matches))document.documentElement.classList.add('dark')}catch(e){}";
export const metadata: Metadata = {title:'MarketPulse 360 | Command Center',description:'Traceable campaign operations, funnel reach and attribution analysis'};
export default function Layout({children}:{children:React.ReactNode}) {
  return <html lang="en" className={sans.variable+' '+mono.variable} suppressHydrationWarning><head><script dangerouslySetInnerHTML={{__html:THEME}}/></head><body><SessionGate>{children}</SessionGate></body></html>;
}
