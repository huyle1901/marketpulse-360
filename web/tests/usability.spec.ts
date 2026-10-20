import {test,expect} from '@playwright/test';
const provenance={generation:'1',identity:'a'.repeat(64),model:'gold.campaign_performance_daily',data_through:'2026-01-03T00:00:00Z'};
const metric={campaign_id:'a',campaign_name:'Campaign A',currency:'VND',platform:'google',objective:'traffic',spend_minor:'300',net_revenue_minor:'9007199254740993',orders:'1',source_roas:'2.123456'};
const dashboard={filters:{from_date:'2026-01-01',to_date:'2026-01-03'},campaigns:[{campaign_id:'a',name:'Campaign A',currency:'VND',platform:'google',objective:'traffic'}],certified:{rows:[metric],totals:[{...metric,clicks:'2',impressions:'10'}],provenance,freshness:{historical:true,stale:true}},streaming:{status:'unavailable',source:'marketing.gold.campaign-metrics.v1',rows:[]}};
test.beforeEach(async({page})=>{
 await page.route('**/api/v1/**',r=>{const p=new URL(r.request().url()).pathname;return r.fulfill({json:p.endsWith('auth/session')?{user_id:'a',username:'admin',role:'admin'}:p.endsWith('dashboard')?dashboard:[]});});
});
test('one workspace navigation exposes current page and a keyboard skip link',async({page})=>{
 await page.goto('/');const nav=page.getByRole('navigation',{name:'Workspaces',exact:true});
 await expect(nav).toHaveCount(1);await expect(nav.getByRole('link',{name:'Campaign Operations',exact:true})).toHaveAttribute('aria-current','page');
 await expect(page.getByRole('link',{name:'Customer 360',exact:true})).toHaveCount(1);
 await page.getByRole('link',{name:'Skip to content'}).focus();await page.keyboard.press('Enter');await expect(page.locator('#main-content')).toBeFocused();
});
test('configuration is discoverable without dominating the campaign overview',async({page})=>{
 await page.goto('/');await expect(page.getByRole('heading',{name:'Marketing Command Center'})).toBeVisible();await expect(page.getByLabel('Period budget (minor units)')).not.toBeVisible();
 await page.getByText('Budgets & alert rules',{exact:true}).click();await expect(page.getByLabel('Period budget (minor units)')).toBeVisible();
 await page.getByText('Budget allocation simulator',{exact:true}).click();await expect(page.getByLabel('Scenario total (minor units)')).toBeVisible();
 await page.setViewportSize({width:390,height:844});expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBeTruthy();
});

test('daily trends expose exact values and missing prior history stays unavailable',async({page})=>{
 const daily_series=[{event_date:'2026-01-01',currency:'VND',spend_minor:'100',net_revenue_minor:'9007199254740993'}, {event_date:'2026-01-02',currency:'VND',spend_minor:'200',net_revenue_minor:'-3'}];
 await page.route('**/api/v1/dashboard*',r=>r.fulfill({json:{...dashboard,certified:{...dashboard.certified,daily_series,comparison:{status:'unavailable',from_date:'2025-12-30',to_date:'2026-01-01',observed_days:'0',expected_days:'2',totals:[]}}}}));
 await page.goto('/');await expect(page.getByRole('heading',{name:'Daily certified trends'})).toBeVisible();
 await expect(page.getByText('Previous period unavailable',{exact:true})).toBeVisible();
 await page.getByText('Exact daily values · VND',{exact:true}).click();const table=page.getByRole('table',{name:'Daily certified values · VND'});
 await expect(table).toContainText('9007199254740993');await expect(table).toContainText('-3');
 await expect(page.getByRole('img',{name:'Daily net revenue and ad spend in VND'})).toBeVisible();
});
test('readable currency, qualified changes and alert shortcuts retain selection context',async({page})=>{
 const usd={...metric,currency:'USD',spend_minor:'125',net_revenue_minor:'250'};
 await page.route('**/api/v1/dashboard*',r=>r.fulfill({json:{...dashboard,certified:{...dashboard.certified,rows:[usd],totals:[{...usd,clicks:'2',impressions:'10'}],daily_series:[{...usd,event_date:'2026-01-02'}],comparison:{status:'complete',from_date:'2025-12-30',to_date:'2026-01-01',observed_days:'2',expected_days:'2',totals:[{...usd,spend_minor:'100',net_revenue_minor:'200'}]}}}}));
 const evidence={campaign_id:'a',explanation:'Spend rose above configured threshold',currency:'USD',outcome:'triggered',from_date:'2026-01-01',to_date:'2026-01-03',rule_id:'r',rule_version:'1',setting_version:'1',operands:{},provenance};
 await page.route('**/api/v1/alerts',r=>r.fulfill({json:[{evaluation_key:'valid',evidence},{evaluation_key:'old',evidence:{...evidence,explanation:'Old release only',provenance:{...provenance,identity:'b'.repeat(64)}}},{evaluation_key:'outside',evidence:{...evidence,explanation:'Outside selected period',from_date:'2026-02-01',to_date:'2026-02-03'}}]}));
 await page.goto('/');const overview=page.getByRole('region',{name:'Decision overview'});
 await expect(page.getByText('$1.25',{exact:true}).first()).toBeVisible();await expect(overview).toContainText('+25.00%');
 await expect(overview).toContainText(evidence.explanation);await expect(overview).not.toContainText('Old release only');await expect(overview).not.toContainText('Outside selected period');
 await overview.getByRole('button',{name:'Investigate Campaign A'}).click();await expect(page.getByRole('combobox',{name:'Campaign',exact:true})).toHaveValue('a');
});
