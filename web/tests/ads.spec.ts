import {test,expect} from '@playwright/test';

const provenance={generation:'1',identity:'a'.repeat(64),model:'gold.campaign_performance_daily',data_through:'2026-01-03T00:00:00Z'};
const row=(id:string,name:string,platform:string|null,spend:string,net:string,orders:string,clicks:string,impressions:string,gross:string)=>({campaign_id:id,campaign_name:name,currency:'VND',platform,objective:platform?'traffic':null,spend_minor:spend,net_revenue_minor:net,orders,clicks,impressions,gross_revenue_minor:gross,source_roas:null});
const rows=[row('g','Google Traffic 1','google','200','500','4','15','60','600'),row('m','Meta Traffic 2','meta','100','200','1','5','40','300'),row('__unattributed__','Unattributed',null,'0','0','0','0','0','0')];
const dashboard={filters:{from_date:'2026-01-01',to_date:'2026-01-03'},campaigns:[],certified:{rows,totals:[{currency:'VND',spend_minor:'300',net_revenue_minor:'700',gross_revenue_minor:'900',orders:'5',clicks:'20',impressions:'100'}],daily_series:[],provenance,freshness:{historical:true,stale:true}},streaming:{status:'unavailable',source:'x',rows:[]}};

test.beforeEach(async({page})=>{await page.route('**/api/v1/**',r=>{const p=new URL(r.request().url()).pathname;return r.fulfill({json:p.endsWith('auth/session')?{user_id:'a',username:'admin',role:'admin'}:p.endsWith('dashboard')?dashboard:[]});});});

test('ad performance is reachable and summarizes efficiency exactly',async({page})=>{
 await page.goto('/');await page.getByRole('navigation',{name:'Workspaces'}).getByRole('link',{name:'Ad performance'}).click();
 await expect(page.getByRole('heading',{name:'Ad performance',level:1})).toBeVisible();
 const kpis=page.getByRole('region',{name:'Ad efficiency · VND'});const card=(name:string)=>kpis.getByRole('article',{name});
 await expect(card('Impressions')).toContainText('100');await expect(card('Clicks')).toContainText('20');
 await expect(card('Click-through rate')).toContainText('20.00%');await expect(card('Cost per click')).toContainText('₫15');
 await expect(card('Cost per order')).toContainText('₫60');await expect(card('Net ROAS')).toContainText('2.33×');
});

test('channels and campaigns compare efficiency, excluding rows with no ad delivery',async({page})=>{
 await page.goto('/ads');const channels=page.getByRole('table',{name:'Efficiency by channel · VND'});
 await expect(channels.locator('tbody tr')).toHaveCount(2);
 await expect(channels.locator('tbody tr').first()).toContainText('Google');await expect(channels.locator('tbody tr').first()).toContainText('25.00%');
 await expect(page.getByRole('img',{name:'Ad spend and net revenue by channel · VND'})).toBeVisible();
 const campaigns=page.getByRole('table',{name:'Efficiency by campaign · VND'});
 await expect(campaigns.locator('tbody tr').first()).toContainText('Google Traffic 1');
 await expect(campaigns).not.toContainText('Unattributed');
 await expect(page.getByText('Unattributed revenue has no ad delivery',{exact:false})).toBeVisible();
});

test('ad performance fits a phone',async({page})=>{
 await page.setViewportSize({width:390,height:844});await page.goto('/ads');
 await expect(page.getByRole('region',{name:'Ad efficiency · VND'})).toBeVisible();
 expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBeTruthy();
});
