import {test,expect} from '@playwright/test';

const provenance={generation:'1',identity:'a'.repeat(64),model:'gold.campaign_performance_daily',data_through:'2026-01-03T00:00:00Z'};
const row={campaign_id:'g',campaign_name:'Google Traffic 1',currency:'VND',platform:'google',objective:'traffic',spend_minor:'300',net_revenue_minor:'700',orders:'5',source_roas:'2.333333'};
const totals={currency:'VND',spend_minor:'300',net_revenue_minor:'700',gross_revenue_minor:'900',refund_amount_minor:'200',orders:'5',clicks:'20',impressions:'100'};
const prior={currency:'VND',spend_minor:'300',net_revenue_minor:'560',gross_revenue_minor:'700',refund_amount_minor:'140',orders:'4',clicks:'10',impressions:'100'};
const day=(d:string,net:string)=>({event_date:d,currency:'VND',spend_minor:'150',net_revenue_minor:net,gross_revenue_minor:'450',orders:'2',clicks:'10',impressions:'50'});
const dashboard={filters:{from_date:'2026-01-01',to_date:'2026-01-03'},campaigns:[{campaign_id:'g',name:'Google Traffic 1',currency:'VND',platform:'google',objective:'traffic'}],
 certified:{rows:[row],totals:[totals],daily_series:[day('2026-01-01','300'),day('2026-01-02','400')],
  comparison:{status:'complete',previous_status:'complete',current_status:'complete',from_date:'2025-12-30',to_date:'2026-01-01',observed_days:'2',expected_days:'2',current_observed_days:'2',current_expected_days:'2',totals:[prior]},provenance,freshness:{historical:true,stale:true}},
 streaming:{status:'unavailable',source:'marketing.gold.campaign-metrics.v1',rows:[]}};

test.beforeEach(async({page})=>{await page.route('**/api/v1/**',r=>{const p=new URL(r.request().url()).pathname;return r.fulfill({json:p.endsWith('auth/session')?{user_id:'a',username:'admin',role:'admin'}:p.endsWith('dashboard')?dashboard:[]});});});

test('KPI cards speak business language with prior-period change and a sparkline',async({page})=>{
 await page.goto('/');const kpis=page.getByRole('region',{name:'Key metrics · VND'});
 const card=(name:string)=>kpis.getByRole('article',{name});
 await expect(card('Net revenue')).toContainText('₫700');await expect(card('Net revenue')).toContainText('+25.0%');
 await expect(card('Ad spend')).toContainText('₫300');await expect(card('Ad spend')).toContainText('0.0%');
 await expect(card('Orders')).toContainText('5');await expect(card('Orders')).toContainText('+25.0%');
 await expect(card('Net ROAS')).toContainText('2.33×');
 await expect(card('Avg. order value')).toContainText('₫180');
 await expect(card('Cost per order')).toContainText('₫60');
 await expect(card('Net revenue').getByRole('img',{name:'Net revenue by day'})).toBeVisible();
 await expect(kpis).not.toContainText('minor units');
});

test('a missing prior period says so instead of showing a change',async({page})=>{
 await page.route('**/api/v1/dashboard*',r=>r.fulfill({json:{...dashboard,certified:{...dashboard.certified,comparison:{...dashboard.certified.comparison,status:'unavailable',previous_status:'unavailable',totals:[]}}}}));
 await page.goto('/');const card=page.getByRole('region',{name:'Key metrics · VND'}).getByRole('article',{name:'Net revenue'});
 await expect(card).toContainText('No comparable prior period');await expect(card).not.toContainText('%');
});

test('range tabs pick published windows ending at the latest certified day',async({page})=>{
 await page.goto('/');const ranges=page.getByRole('group',{name:'Date range'});
 await ranges.getByRole('button',{name:'30D'}).click();
 await expect(page.getByLabel('From (UTC)',{exact:true})).toHaveValue('2025-12-04');
 await expect(page.getByLabel('To (exclusive UTC)',{exact:true})).toHaveValue('2026-01-03');
 await expect(ranges.getByRole('button',{name:'30D'})).toHaveAttribute('aria-pressed','true');
});

test('the area chart offers exact values and keeps engineering detail out of the headline',async({page})=>{
 await page.goto('/');const overview=page.getByRole('region',{name:'Decision overview'});
 await expect(overview.getByRole('img',{name:'Daily net revenue in VND'})).toBeVisible();
 await expect(overview.locator('.recharts-area')).toHaveCount(1);
 await expect(overview.locator('h2,figcaption')).not.toContainText(['exclusive UTC']);
});
