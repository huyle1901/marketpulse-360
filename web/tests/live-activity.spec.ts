import {test,expect} from '@playwright/test';

const provenance={generation:'1',identity:'a'.repeat(64),model:'gold.campaign_performance_daily',data_through:'2026-01-03T00:00:00Z'};
const live=(sessions:string,offset:string)=>({metric_key:'a|2026-01-04',campaign_id:'a',sessions,order_count:'2',net_revenue_minor:'-5',currency:'VND',max_event_time:'2026-01-04T10:00:00Z',processed_at:'2026-10-29T10:00:0'+offset+'Z',received_at:'2026-10-29T10:00:0'+offset+'Z',stale:true,mapped:true,offset});
const dashboard=(rows:object[])=>({filters:{from_date:'2026-01-01',to_date:'2026-01-03'},campaigns:[{campaign_id:'a',name:'Campaign A',currency:'VND',platform:'google',objective:'traffic'}],
 certified:{rows:[],totals:[],provenance,freshness:{historical:true,stale:true}},
 streaming:{status:rows.length?'available':'empty',source:'marketing.gold.campaign-metrics.v1',source_identity:'cluster/topic',offset_bounds:[{partition:'0',from_offset:'1',to_offset:'3'}],rows}});

test.beforeEach(async({page})=>{await page.route('**/api/v1/**',r=>{const p=new URL(r.request().url()).pathname;return r.fulfill({json:p.endsWith('auth/session')?{user_id:'a',username:'admin',role:'admin'}:p.endsWith('dashboard')?dashboard([live('7','1')]):[]});});});

test('workspace navigation uses task names and marks the current workspace',async({page})=>{
 await page.goto('/');const nav=page.getByRole('navigation',{name:'Workspaces',exact:true});
 for(const name of ['Overview','Live activity','Journey analysis','Customers','Data health'])await expect(nav.getByRole('link',{name,exact:true})).toHaveCount(1);
 await expect(nav.getByRole('link',{name:'Overview',exact:true})).toHaveAttribute('aria-current','page');
 await nav.getByRole('link',{name:'Live activity',exact:true}).click();
 await expect(nav.getByRole('link',{name:'Live activity',exact:true})).toHaveAttribute('aria-current','page');
});

test('overview no longer embeds the streaming table',async({page})=>{
 await page.goto('/');await expect(page.getByRole('heading',{name:'Daily certified trends'})).toBeVisible();
 await expect(page.getByRole('heading',{name:'Streaming activity'})).toHaveCount(0);
});

test('live activity separates provisional snapshots from certified finance and keeps provenance',async({page})=>{
 await page.goto('/live');
 await expect(page.getByRole('heading',{name:'Live activity',level:1})).toBeVisible();
 const summary=page.getByRole('region',{name:'Provisional live summary'});
 await expect(summary).toContainText('7');await expect(summary).toContainText('-₫5');
 await expect(summary).toContainText('campaign-day session incidences');
 await expect(page.getByText('Not certified finance',{exact:false}).first()).toBeVisible();
 await expect(page.getByText(/events\/second/i)).toHaveCount(0);
 const table=page.getByRole('table',{name:'Latest campaign-day snapshots'});
 await expect(table).toContainText('Campaign A');await expect(table).toContainText('2026-01-04');
 await page.getByText('Streaming source provenance',{exact:true}).click();
 await expect(page.getByText('cluster/topic',{exact:false})).toBeVisible();
 await expect(page.getByText('Replay controller status unavailable',{exact:false})).toBeVisible();
});

test('live window starts a week before certified data and runs through the present',async({page})=>{
 const requests:URL[]=[];page.on('request',r=>{if(r.url().includes('/api/v1/dashboard'))requests.push(new URL(r.url()));});
 await page.goto('/live');await expect(page).toHaveURL(/from_date=2025-12-27/);
 const today=new Date();const tomorrow=new Date(Date.UTC(today.getUTCFullYear(),today.getUTCMonth(),today.getUTCDate()+1)).toISOString().slice(0,10);
 await expect(page).toHaveURL(new RegExp('to_date='+tomorrow));
 await expect.poll(()=>requests.some(u=>u.searchParams.get('from_date')==='2025-12-27')).toBeTruthy();
});

test('a replaced live snapshot is marked as changed',async({page})=>{
 let calls=0;
 await page.route('**/api/v1/dashboard*',r=>{if(new URL(r.request().url()).searchParams.has('from_date'))calls++;return r.fulfill({json:dashboard([calls<2?live('7','1'):live('9','2')])});});
 await page.goto('/live');const table=page.getByRole('table',{name:'Latest campaign-day snapshots'});
 await expect(table).toContainText('Updated',{timeout:15000});await expect(table).toContainText('9');
 await page.waitForTimeout(5500);await expect(table).toContainText('Updated');
});

test('an empty live projection is explicit and draws no fabricated rows',async({page})=>{
 await page.route('**/api/v1/dashboard*',r=>r.fulfill({json:dashboard([])}));
 await page.goto('/live');await expect(page.getByText('No committed streaming snapshot',{exact:false})).toBeVisible();
 await expect(page.getByRole('table',{name:'Latest campaign-day snapshots'})).toHaveCount(0);
});

test('live activity fits a phone viewport',async({page})=>{
 await page.setViewportSize({width:390,height:844});await page.goto('/live');
 await expect(page.getByRole('table',{name:'Latest campaign-day snapshots'})).toBeVisible();
 expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBeTruthy();
});
