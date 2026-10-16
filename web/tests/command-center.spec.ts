import { test, expect } from '@playwright/test';

const data = {
  filters:{from_date:'2026-01-01',to_date:'2026-01-03'},
  certified:{rows:[{campaign_id:'a',campaign_name:'Campaign A',currency:'VND',platform:'google',objective:'traffic',spend_minor:'3',net_revenue_minor:'9007199254740993',orders:'1',source_roas:'2'}],
    totals:[{currency:'VND',spend_minor:'3',net_revenue_minor:'9007199254740993',orders:'1',clicks:'2',impressions:'10'}],
    provenance:{generation:'1',identity:'a'.repeat(64),model:'gold.campaign_performance_daily',data_through:'2026-01-03T00:00:00Z'},freshness:{historical:true,stale:true}},
  streaming:{status:'unavailable',rows:[],source:'marketing.gold.campaign-metrics.v1',source_identity:'cluster/topic',offset_bounds:[{partition:'0',from_offset:'1',to_offset:'3'}]},
  campaigns:[{campaign_id:'a',name:'Campaign A',currency:'VND',platform:'google',objective:'traffic'}]
};

test.beforeEach(async ({page})=>{
  await page.route('**/api/v1/**',async route=>{
    const path=new URL(route.request().url()).pathname;
    const body=path.endsWith('dashboard')?data:path.includes('budget-pacing')?{status:'insufficient_data',spend_minor:'0',remaining_minor:'100',forecast_minor:null}:
      path.endsWith('budget-simulation')?{projected_net_minor:'200',rows:[],provenance:data.certified.provenance,assumption:'Linear scenario using historical net ROAS; not a prediction'}:
      path.endsWith('alerts')?[{evaluation_key:'x',evidence:{explanation:'refund_share: triggered',campaign_id:'a',outcome:'triggered',currency:'VND',rule_id:'r',rule_version:'2',setting_version:'3',provenance:{identity:'a'.repeat(64)},operands:{gross_revenue_minor:'10',refund_amount_minor:'1'}}}]:[];
    await route.fulfill({json:body});
  });
});

test('sources, historical status and exact integers stay visible',async({page})=>{
  await page.goto('/');
  await expect(page.getByRole('heading',{name:'Marketing Command Center'})).toBeVisible();
  await expect(page.getByText('Historical data',{exact:true})).toBeVisible();
  await expect(page.getByText('9,007,199,254,740,993').first()).toBeVisible();
  await expect(page.getByRole('heading',{name:'Streaming activity'})).toBeVisible();
  await expect(page.getByText('refund_share: triggered')).toBeVisible();
});

test('configuration, pacing and simulation are distinct user actions',async({page})=>{
  await page.goto('/');
  await page.getByLabel('Period budget (minor units)').fill('100');
  await page.getByRole('button',{name:'Save campaign settings'}).click();
  await expect(page.getByText('Campaign settings saved')).toBeVisible();
  await page.getByRole('button',{name:'View pacing'}).click();
  await expect(page.getByText('insufficient_data',{exact:true})).toBeVisible();
  await page.getByLabel('Scenario total (minor units)').fill('100');
  await page.getByLabel('Allocation for Campaign A').fill('99');
  await page.getByRole('button',{name:'Simulate allocation'}).click();
  await expect(page.getByText('Allocations must equal the scenario total')).toBeVisible();
  await page.getByLabel('Allocation for Campaign A').fill('100');
  await page.getByRole('button',{name:'Simulate allocation'}).click();
  await expect(page.getByText('Linear scenario using historical net ROAS; not a prediction')).toBeVisible();
});

test('mobile layout stays within viewport and forms have labels',async({page})=>{
  await page.setViewportSize({width:390,height:844});
  await page.goto('/');
  await expect(page.getByLabel('Configured market')).toBeVisible();
  expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBeTruthy();
});

test('failed polling retains last successful results with error state',async({page})=>{
  await page.goto('/');
  await expect(page.getByText('9,007,199,254,740,993').first()).toBeVisible();
  await page.route('**/api/v1/dashboard*',route=>route.fulfill({status:503,json:{code:'dependency_unavailable'}}));
  await expect(page.getByText('Refresh failed; showing last successful data')).toBeVisible({timeout:10000});
  await expect(page.getByText('9,007,199,254,740,993').first()).toBeVisible();
});

test('a changed interval discards a pending scenario from the old interval',async({page})=>{
  let release!:()=>void;
  const pending=new Promise<void>(resolve=>{release=resolve;});
  await page.route('**/api/v1/budget-simulation',async route=>{
    await pending;
    await route.fulfill({json:{projected_net_minor:'777',rows:[],provenance:data.certified.provenance,assumption:'Old scenario must disappear'}});
  });
  await page.goto('/');
  await page.getByLabel('Scenario total (minor units)').fill('100');
  await page.getByLabel('Allocation for Campaign A').fill('100');
  const request=page.waitForRequest('**/api/v1/budget-simulation');
  await page.getByRole('button',{name:'Simulate allocation'}).click();await request;
  await page.getByLabel('From (UTC)',{exact:true}).fill('2026-01-02');
  release();
  await page.waitForResponse('**/api/v1/budget-simulation');
  await page.waitForTimeout(250); // Allow the response continuation to render before a negative assertion.
  await expect(page.getByText('Old scenario must disappear')).toHaveCount(0);
});

test('a pending settings save disables its editable draft',async({page})=>{
  let release!:()=>void;const pending=new Promise<void>(resolve=>{release=resolve;});
  await page.route('**/api/v1/campaign-settings/a',async route=>{
    await pending;await route.fulfill({json:{campaign_id:'a',market:null,currency:'VND',period_start:'2026-01-01',period_end:'2026-01-03',budget_minor:'100',version:'1'}});
  });
  await page.goto('/');await page.getByLabel('Period budget (minor units)').fill('100');
  const request=page.waitForRequest('**/api/v1/campaign-settings/a');
  await page.getByRole('button',{name:'Save campaign settings'}).click();await request;
  await expect(page.getByLabel('Period budget (minor units)')).toBeDisabled();release();
});

test('changed filter labels retained results while the next selection loads',async({page})=>{
  await page.goto('/');await expect(page.getByText('9,007,199,254,740,993').first()).toBeVisible();
  let release!:()=>void;const pending=new Promise<void>(resolve=>{release=resolve;});
  await page.route('**/api/v1/dashboard*platform=google*',async route=>{await pending;await route.fulfill({json:data});});
  await page.getByRole('combobox',{name:/^Platform/}).selectOption('google');
  await expect(page.getByText('Loading selection; showing previous filter results')).toBeVisible();release();
});

test('release change invalidates a pending scenario',async({page})=>{
  let release!:()=>void;const pending=new Promise<void>(resolve=>{release=resolve;});
  await page.route('**/api/v1/budget-simulation',async route=>{await pending;await route.fulfill({json:{projected_net_minor:'777',rows:[],provenance:data.certified.provenance,assumption:'Scenario from old release'}});});
  await page.goto('/');await page.getByLabel('Scenario total (minor units)').fill('100');await page.getByLabel('Allocation for Campaign A').fill('100');
  const request=page.waitForRequest('**/api/v1/budget-simulation');await page.getByRole('button',{name:'Simulate allocation'}).click();await request;
  await page.route('**/api/v1/dashboard*',route=>route.fulfill({json:{...data,certified:{...data.certified,provenance:{...data.certified.provenance,generation:'2',identity:'b'.repeat(64)}}}}));
  await expect(page.locator('details').filter({has:page.locator('summary',{hasText:'Metric source & freshness'})})).toContainText('Generation 2',{timeout:10000});
  release();await page.waitForResponse('**/api/v1/budget-simulation');await page.waitForTimeout(250);
  await expect(page.getByText('Scenario from old release')).toHaveCount(0);
});

test('source identity and versioned alert evidence are inspectable',async({page})=>{
  await page.goto('/');
  await page.getByText('Streaming source provenance',{exact:true}).click();
  await expect(page.getByText('cluster/topic',{exact:false})).toBeVisible();
  await page.getByText('Full alert provenance',{exact:true}).click();
  await expect(page.getByText('Rule version 2 · Setting version 3',{exact:false})).toBeVisible();
});

test.beforeEach(async({page})=>{await page.route('**/api/v1/auth/session',r=>r.fulfill({json:{user_id:'fixture-admin',username:'fixture-admin',role:'admin'}}));});
