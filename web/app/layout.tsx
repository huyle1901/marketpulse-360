import type { Metadata } from 'next';
import './globals.css';
import SessionGate from '../components/SessionGate';
export const metadata: Metadata = {title:'MarketPulse 360 | Command Center',description:'Traceable campaign operations, funnel reach and attribution analysis'};
export default function Layout({children}:{children:React.ReactNode}) {
  return <html lang="en"><body><SessionGate>{children}</SessionGate></body></html>;
}
