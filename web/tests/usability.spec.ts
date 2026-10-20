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
 await page.route('**/api/v1/dashboard*',r=>r.fulfill({json:{...dashboard,certified:{...dashboard.certified,rows:[usd],totals:[{...usd,clicks:'2',impressions:'10'}],daily_series:[{...usd,event_date:'2026-01-02'}],comparison:{status:'complete',previous_status:'complete',current_status:'complete',current_observed_days:'2',current_expected_days:'2',from_date:'2025-12-30',to_date:'2026-01-01',observed_days:'2',expected_days:'2',totals:[{...usd,spend_minor:'100',net_revenue_minor:'200'}]}}}}));
 const evidence={campaign_id:'a',explanation:'Spend rose above configured threshold',currency:'USD',outcome:'triggered',from_date:'2026-01-01',to_date:'2026-01-03',rule_id:'r',rule_version:'1',setting_version:'1',operands:{},provenance};
 await page.route('**/api/v1/alerts',r=>r.fulfill({json:[{evaluation_key:'valid',evidence},{evaluation_key:'old',evidence:{...evidence,explanation:'Old release only',provenance:{...provenance,identity:'b'.repeat(64)}}},{evaluation_key:'outside',evidence:{...evidence,explanation:'Outside selected period',from_date:'2026-02-01',to_date:'2026-02-03'}}]}));
 await page.goto('/');const overview=page.getByRole('region',{name:'Decision overview'});
 await expect(page.getByText('$1.25',{exact:true}).first()).toBeVisible();await expect(overview).toContainText('+25.00%');
 await expect(overview).toContainText(evidence.explanation);await expect(overview).not.toContainText('Old release only');await expect(overview).not.toContainText('Outside selected period');
 await overview.getByRole('button',{name:'Investigate Campaign A'}).click();await expect(page.getByRole('combobox',{name:'Campaign',exact:true})).toHaveValue('a');
});

test('campaign deep links, reload and back restore filters without sending local table preferences',async({page})=>{
 const requests:URL[]=[];page.on('request',r=>{if(r.url().includes('/api/v1/dashboard'))requests.push(new URL(r.url()));});
 await page.goto('/?from_date=2026-01-01&to_date=2026-01-03&platform=google&q=Campaign&sort=net_desc&unknown=ignored');
 await expect(page.getByRole('combobox',{name:'Platform',exact:true})).toHaveValue('google');
 await expect(page.getByLabel('Search campaigns')).toHaveValue('Campaign');
 await page.getByRole('combobox',{name:'Platform',exact:true}).selectOption('meta');await expect(page).toHaveURL(/platform=meta/);
 await page.reload();await expect(page.getByRole('combobox',{name:'Platform',exact:true})).toHaveValue('meta');
 await page.goBack();await expect(page.getByRole('combobox',{name:'Platform',exact:true})).toHaveValue('google');
 expect(requests.length).toBeGreaterThan(0);for(const url of requests){expect(url.searchParams.has('q')).toBe(false);expect(url.searchParams.has('sort')).toBe(false);expect(url.searchParams.has('unknown')).toBe(false);}
});
test('campaign search, exact numeric sorting, published presets and analysis links work',async({page})=>{
 const rows=[{...metric,campaign_id:'negative',campaign_name:'Refund campaign',net_revenue_minor:'-9007199254740993'}, {...metric,campaign_id:'large',campaign_name:'Large campaign',net_revenue_minor:'9007199254740993'}, {...metric,campaign_id:'larger',campaign_name:'Larger campaign',net_revenue_minor:'9007199254740994'}];
 await page.route('**/api/v1/dashboard*',r=>r.fulfill({json:{...dashboard,certified:{...dashboard.certified,rows}}}));
 await page.goto('/');await page.getByLabel('Campaign table order').selectOption('net_desc');
 const table=page.getByRole('table',{name:'Campaign performance'});await expect(table.locator('tbody tr').first()).toContainText('Larger campaign');
 await page.getByLabel('Campaign table order').selectOption('net_asc');await expect(table.locator('tbody tr').first()).toContainText('Refund campaign');
 await page.getByLabel('Search campaigns').fill('Larger');await expect(table.locator('tbody tr')).toHaveCount(1);
 const link=table.getByRole('link',{name:'Analyze Larger campaign'});await expect(link).toHaveAttribute('href',/campaign_id=larger/);await expect(link).toHaveAttribute('href',/from_date=2026-01-01/);
 await page.getByRole('button',{name:'Last published day',exact:true}).click();await expect(page.getByLabel('From (UTC)',{exact:true})).toHaveValue('2026-01-02');
 await page.getByRole('button',{name:'Last 7 published days',exact:true}).click();await expect(page.getByLabel('From (UTC)',{exact:true})).toHaveValue('2025-12-27');
 await page.getByRole('button',{name:'Reset filters',exact:true}).click();await expect(page.getByLabel('Search campaigns')).toHaveValue('');await expect(page.getByLabel('From (UTC)',{exact:true})).toHaveValue('2026-01-01');
});
test('copy view link reports clipboard failure without claiming success',async({page})=>{
 await page.addInitScript(()=>{Object.defineProperty(navigator,'clipboard',{value:{writeText:async()=>{throw new Error('denied');}},configurable:true});});
 await page.goto('/');await page.getByRole('button',{name:'Copy view link'}).click();await expect(page.getByText('Could not copy; copy the URL from your address bar.',{exact:true})).toBeVisible();
});

test('automatic campaign dates replace history so Back leaves the workspace',async({page})=>{
 await page.goto('/login');await page.getByRole('link',{name:'Open Command Center'}).click();await expect(page).toHaveURL(/from_date=/);
 await page.goBack();await expect(page).toHaveURL(url=>url.pathname==='/login');
});
test('missing currency or filtered campaign baselines remain unavailable instead of zero',async({page})=>{
 for(const totals of [[],[{currency:'USD',spend_minor:'100',net_revenue_minor:'200'}]]){
  await page.route('**/api/v1/dashboard*',r=>r.fulfill({json:{...dashboard,certified:{...dashboard.certified,comparison:{status:'complete',previous_status:'complete',current_status:'complete',from_date:'2025-12-30',to_date:'2026-01-01',observed_days:'2',expected_days:'2',current_observed_days:'2',current_expected_days:'2',totals}}}}));
  await page.goto('/?campaign_id=a');const comparison=page.getByRole('region',{name:'Decision overview'}).locator('.period-comparison');await expect(comparison).toContainText('Baseline unavailable');await expect(comparison).not.toContainText('vs ₫0');
 }
});
test('sparse current data cannot appear as a percentage decline',async({page})=>{
 await page.route('**/api/v1/dashboard*',r=>r.fulfill({json:{...dashboard,certified:{...dashboard.certified,totals:[{...metric,net_revenue_minor:'100',spend_minor:'100',clicks:'2',impressions:'10'}],comparison:{status:'complete',previous_status:'complete',current_status:'partial',from_date:'2025-12-30',to_date:'2026-01-01',observed_days:'2',expected_days:'2',current_observed_days:'1',current_expected_days:'2',totals:[{currency:'VND',net_revenue_minor:'200',spend_minor:'200'}]}}}}));
 await page.goto('/');const comparison=page.getByRole('region',{name:'Decision overview'}).locator('.period-comparison');await expect(comparison).toContainText('Current period partially covered');await expect(comparison).not.toContainText('-50.00%');
});
test('retained campaign rows do not offer drill links for a failed new selection',async({page})=>{
 await page.goto('/');await expect(page.getByRole('link',{name:'Analyze Campaign A'})).toBeVisible();
 await page.route('**/api/v1/dashboard*platform=meta*',r=>r.fulfill({status:503,json:{code:'dependency_unavailable'}}));
 await page.getByRole('combobox',{name:'Platform',exact:true}).selectOption('meta');await expect(page.getByText('Refresh failed; showing last successful data')).toBeVisible();
 await expect(page.getByRole('table',{name:'Campaign performance'})).toContainText('Campaign A');await expect(page.getByRole('link',{name:'Analyze Campaign A'})).toHaveCount(0);
});

test('reset hydration replaces its default dates so Back restores the prior selection',async({page})=>{
 await page.goto('/');await expect(page).toHaveURL(/from_date=/);await page.getByRole('combobox',{name:'Platform',exact:true}).selectOption('google');await expect(page).toHaveURL(/platform=google/);
 await page.getByRole('button',{name:'Reset filters',exact:true}).click();await expect(page.getByLabel('From (UTC)',{exact:true})).toHaveValue('2026-01-01');await expect(page.getByRole('combobox',{name:'Platform',exact:true})).toHaveValue('');
 await page.goBack();await expect(page.getByRole('combobox',{name:'Platform',exact:true})).toHaveValue('google');
});
