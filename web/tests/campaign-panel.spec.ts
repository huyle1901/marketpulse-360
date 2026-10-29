import {test,expect} from '@playwright/test';

const provenance={generation:'1',identity:'a'.repeat(64),model:'gold.campaign_performance_daily',data_through:'2026-01-03T00:00:00Z'};
const row=(i:number)=>({campaign_id:'c'+i,campaign_name:'Campaign '+i,currency:'VND',platform:['google','meta','tiktok'][i%3],objective:'traffic',spend_minor:String(100+i),net_revenue_minor:String(300+i),orders:'2',source_roas:'3'});
const rows=Array.from({length:12},(_,i)=>row(i+1));
const dashboard={filters:{from_date:'2026-01-01',to_date:'2026-01-03'},campaigns:rows.map(r=>({campaign_id:r.campaign_id,name:r.campaign_name,currency:'VND',platform:r.platform,objective:'traffic'})),
 certified:{rows,totals:[{currency:'VND',spend_minor:'1278',net_revenue_minor:'3678',orders:'24',clicks:'1',impressions:'1'}],daily_series:[{event_date:'2026-01-01',currency:'VND',spend_minor:'600',net_revenue_minor:'1800',orders:'12'},{event_date:'2026-01-02',currency:'VND',spend_minor:'678',net_revenue_minor:'1878',orders:'12'}],provenance,freshness:{historical:true,stale:true}},
 streaming:{status:'unavailable',source:'marketing.gold.campaign-metrics.v1',rows:[]}};
const single={...dashboard,certified:{...dashboard.certified,rows:[rows[0]],daily_series:[{event_date:'2026-01-01',currency:'VND',spend_minor:'51',net_revenue_minor:'9007199254740993',orders:'1'}]}};
const alert=(i:number)=>({evaluation_key:'k'+i,evaluated_at:'2026-01-03T00:00:0'+(i%10)+'Z',evidence:{campaign_id:'c1',explanation:'Refund share triggered '+i,currency:'VND',outcome:'triggered',from_date:'2026-01-01',to_date:'2026-01-03',rule_id:'r',rule_version:'1',setting_version:'1',operands:{gross_revenue_minor:'10',refund_amount_minor:'1'},provenance}});

test.beforeEach(async({page})=>{
 await page.route('**/api/v1/**',r=>{const url=new URL(r.request().url()),p=url.pathname;
  return r.fulfill({json:p.endsWith('auth/session')?{user_id:'a',username:'admin',role:'admin'}:p.endsWith('dashboard')?(url.searchParams.get('campaign_id')==='c1'?single:dashboard):
   p.includes('budget-pacing')?{status:'insufficient_data',spend_minor:'0',remaining_minor:'100',forecast_minor:null}:p.endsWith('alerts')?Array.from({length:30},(_,i)=>alert(i)):[]});});
});

test('campaign details open in a labelled dialog with daily results and close back to the opener',async({page})=>{
 await page.goto('/');const opener=page.getByRole('button',{name:'Open details for Campaign 1',exact:true});await opener.click();
 const dialog=page.getByRole('dialog',{name:'Campaign 1'});await expect(dialog).toBeVisible();
 await expect(dialog.getByRole('table',{name:'Daily results · Campaign 1'})).toContainText('9007199254740993');
 await expect(dialog.getByLabel('Period budget (minor units)')).toBeVisible();
 await expect(dialog.getByRole('button',{name:'View pacing'})).toBeVisible();
 await expect(dialog).toContainText('Refund share triggered');
 await page.keyboard.press('Escape');await expect(dialog).toHaveCount(0);await expect(opener).toBeFocused();
});

test('keyboard focus stays inside the open campaign dialog',async({page})=>{
 await page.goto('/');await page.getByRole('button',{name:'Open details for Campaign 1',exact:true}).click();
 const dialog=page.getByRole('dialog',{name:'Campaign 1'});await expect(dialog.getByRole('button',{name:'Close campaign details'})).toBeFocused();
 await page.keyboard.press('Shift+Tab');expect(await dialog.evaluate(d=>d.contains(document.activeElement))).toBeTruthy();
 for(let i=0;i<40;i++)await page.keyboard.press('Tab');
 expect(await dialog.evaluate(d=>d.contains(document.activeElement))).toBeTruthy();
});

test('investigating an alert opens that campaign and the simulator entry leaves the dialog',async({page})=>{
 await page.goto('/');await page.getByRole('region',{name:'Decision overview'}).getByRole('button',{name:'Investigate Campaign 1'}).first().click();
 const dialog=page.getByRole('dialog',{name:'Campaign 1'});await expect(dialog).toBeVisible();
 await dialog.getByRole('button',{name:'Simulate a budget allocation'}).click();
 await expect(dialog).toHaveCount(0);await expect(page.getByLabel('Scenario total (minor units)')).toBeVisible();
});

test('retained rows of a failed new selection cannot open campaign actions',async({page})=>{
 await page.goto('/');await expect(page.getByRole('button',{name:'Open details for Campaign 1',exact:true})).toBeEnabled();
 await page.route('**/api/v1/dashboard*platform=meta*',r=>r.fulfill({status:503,json:{code:'dependency_unavailable'}}));
 await page.getByRole('combobox',{name:'Platform',exact:true}).selectOption('meta');await expect(page.getByText('Refresh failed; showing last successful data')).toBeVisible();
 await expect(page.getByRole('button',{name:'Open details for Campaign 1',exact:true})).toBeDisabled();
});

test('viewers can read campaign details but not save configuration',async({page})=>{
 await page.route('**/api/v1/auth/session',r=>r.fulfill({json:{user_id:'v',username:'viewer',role:'viewer'}}));
 await page.goto('/');await page.getByRole('button',{name:'Open details for Campaign 1',exact:true}).click();
 const dialog=page.getByRole('dialog',{name:'Campaign 1'});await expect(dialog.getByRole('button',{name:'Save campaign settings'})).toBeDisabled();
 await expect(dialog.getByRole('button',{name:'Save alert rule'})).toBeDisabled();
});

test('12-campaign overview fits the desktop height budget with alert evidence collapsed',async({page})=>{
 await page.setViewportSize({width:1440,height:1000});await page.goto('/');
 await expect(page.getByRole('table',{name:'Campaign performance'}).locator('tbody tr')).toHaveCount(12);
 for(const name of ['Net revenue · VND','Ad spend · VND','Orders','Net ROAS · VND'])await expect(page.getByText(name,{exact:false}).first()).toBeInViewport();
 await expect(page.getByRole('img',{name:'Daily net revenue in VND'})).toBeInViewport();
 expect(await page.evaluate(()=>document.documentElement.scrollHeight)).toBeLessThanOrEqual(2000);
 await page.getByText('All alert evidence (30)',{exact:true}).click();await expect(page.getByText('Refund share triggered 29',{exact:true})).toBeVisible();
});

test('campaign dialog fits a phone without page overflow',async({page})=>{
 await page.setViewportSize({width:390,height:844});await page.goto('/');await page.getByRole('button',{name:'Open details for Campaign 1',exact:true}).click();
 await expect(page.getByRole('dialog',{name:'Campaign 1'})).toBeVisible();
 expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBeTruthy();
});

