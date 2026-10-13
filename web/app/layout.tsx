import type { Metadata } from 'next';
import './styles.css';
export const metadata: Metadata = {title:'MarketPulse 360 | Campaign Operations',description:'Traceable campaign performance, alerts and budget scenarios'};
export default function Layout({children}:{children:React.ReactNode}) {
  return <html lang="en"><body>{children}</body></html>;
}
