import {test,expect} from '@playwright/test';
const models=['first_touch','last_touch','linear'];
const provenance={identity:'a'.repeat(64),generation:'1',proof_digest:'b'.repeat(64),historical:true,data_through:'2026-01-03T00:00:00Z',model:'gold.funnel_daily',timestamp_resolution:'day'};
const counts={sessions:'2',page_view_sessions:'2',view_item_sessions:'1',add_to_cart_sessions:'1',checkout_sessions:'1',purchase_sessions:'1'};
const ratios={page_view_sessions:'1.000000',view_item_sessions:'0.500000',add_to_cart_sessions:'0.500000',checkout_sessions:'0.500000',purchase_sessions:'0.500000'};
const finance={gross_revenue_minor:'9007199254740993',refund_amount_minor:'3',net_revenue_minor:'9007199254740990',allocated_order_count:'1.000000'};
const defs={first_touch:'Earliest unique eligible paid click in the inclusive 30-day window ending at original purchase time.',last_touch:'Latest unique eligible paid click in the inclusive 30-day window ending at original purchase time.',linear:'Equal credit across unique eligible clicks; integral money/count micros remainders follow event-time/campaign/click/event order.'};
const funnel={totals:counts,reach_ratios:ratios,unit:'campaign-day session incidences',definition:'Stage reach, not a strict sequential funnel; sessions may span multiple campaign/day buckets.',filters:{from_date:'2026-01-01',to_date:'2026-01-03'},provenance,
 campaign_options:[{campaign_id:'a',name:'Campaign A',currency:'VND',platform:'google',objective:'traffic'}],channels:[{channel:'google',...counts}],campaigns:[{campaign_id:'a',campaign_name:'Campaign A',channel:'google',...counts}]};
const attribution={provenance,currency:'VND',definitions:defs,models:models.map(model=>({model,definition:defs[model as keyof typeof defs],...finance})),channels:models.map(model=>({model,channel:'google',...finance})),campaigns:models.map(model=>({model,channel:'google',campaign_id:'a',campaign_name:'Campaign A',...finance})),global_reconciliation:{balanced:true,scope:'All campaigns before dimension filters',source_totals:finance,model_totals:models.map(model=>({model,...finance}))}};
const journeys={provenance:{...provenance,silver_snapshot_id:'42',projection_checksum:'c'.repeat(64),projection_row_count:'10'},status:'available',model:'last_touch',currency:'VND',representative:true,candidates_scanned:'1',context_unavailable:'0',candidate_scan_capped:false,sample_limit:'5',rows:[{journey_id:'d'.repeat(64),purchase_time:'2026-01-02T00:00:00Z',context_from:'2025-12-03T00:00:00Z',context_to:'2026-01-02T00:00:00Z',touches:[{touch_ref:'t',event_time:'2025-12-31T00:00:00Z',campaign_name:'Campaign A',campaign_id:'a',channel:'google',outside_report_interval:true}],financial_events:[{event_ref:'e',event_name:'order_refunded',event_time:'2026-01-02T01:00:00Z',gross_revenue_minor:'0',refund_amount_minor:'3'}],allocations:models.map(model=>({model,event_date:'2026-01-02',campaign_id:'a',campaign_name:'Campaign A',channel:'google',allocated_gross_minor:'0',allocated_refund_minor:'3',allocated_net_minor:'-3',allocated_order_count:'0.000000'}))}]};
test.beforeEach(async({page})=>{await page.route('**/api/v1/analysis/**',route=>{const name=new URL(route.request().url()).pathname.split('/').pop();return route.fulfill({json:name==='funnel'?funnel:name==='attribution'?attribution:{...journeys,model:new URL(route.request().url()).searchParams.get('model')||'last_touch'}});});});
test('navigation, stage unit, exact comparisons and refund context',async({page})=>{
 await page.goto('/analysis');await expect(page.getByRole('heading',{name:'Journey analysis',exact:true})).toBeVisible();
 await expect(page.getByText('campaign-day session incidences',{exact:true})).toBeVisible();
 await expect(page.getByText('9,007,199,254,740,993').first()).toBeVisible();
 await expect(page.getByText(defs.linear)).toBeVisible();
 await expect(page.getByText('All campaigns before dimension filters',{exact:true})).toBeVisible();
 await expect(page.getByText('Outside report interval',{exact:true})).toBeVisible();
 await expect(page.getByText('-3',{exact:true}).first()).toBeVisible();
 await page.getByRole('link',{name:'Overview',exact:true}).click();await expect(page).toHaveURL(url=>url.pathname==='/');
 await page.getByRole('link',{name:'Journey analysis',exact:true}).click();await expect(page).toHaveURL(url=>url.pathname==='/analysis');
});
test('channel and campaign drill apply credited filters, model is selectable',async({page})=>{
 await page.goto('/analysis');await page.getByRole('button',{name:'Explore channel google',exact:true}).click();
 await expect(page.getByLabel('Paid channel')).toHaveValue('google');
 await page.getByRole('button',{name:'Explore campaign Campaign A',exact:true}).click();await expect(page.getByLabel('Campaign',{exact:true})).toHaveValue('a');
 await page.getByLabel('Journey attribution model').selectOption('linear');
 await expect(page.getByText('Sampling model: Linear',{exact:true})).toBeVisible();
 await page.getByRole('button',{name:'All channels',exact:true}).click();await expect(page.getByLabel('Paid channel')).toHaveValue('');
});
test('mobile, unavailable samples, keyboard and overflow',async({page})=>{
 await page.setViewportSize({width:390,height:844});await page.route('**/api/v1/analysis/journeys*',route=>route.fulfill({json:{...journeys,status:'unavailable',rows:[],message:'Publish pinned journey detail for this certified release first.'}}));
 await page.goto('/analysis');await expect(page.getByText('Publish pinned journey detail for this certified release first.')).toBeVisible();
 await page.getByLabel('Journey attribution model').focus();await page.keyboard.press('ArrowUp');
 expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBeTruthy();
});
test('mixed release refresh retains marked previous results',async({page})=>{
 await page.goto('/analysis');await expect(page.getByText('9,007,199,254,740,993').first()).toBeVisible();
 await page.route('**/api/v1/analysis/attribution*',route=>route.fulfill({json:{...attribution,provenance:{...provenance,identity:'f'.repeat(64)}}}));
 await page.getByRole('button',{name:'Refresh analysis',exact:true}).click();
 await expect(page.getByText('Release changed between reads; refresh analysis again.')).toBeVisible();await expect(page.getByText('Refresh failed; showing last successful analysis')).toBeVisible();
});
test('slow previous selection never replaces current results',async({page})=>{
 await page.goto('/analysis');await expect(page.getByText('9,007,199,254,740,993').first()).toBeVisible();
 let release!:()=>void;const pending=new Promise<void>(resolve=>{release=resolve;});
 await page.route('**/api/v1/analysis/funnel*platform=google*',async route=>{await pending;await route.fulfill({json:{...funnel,totals:{...counts,sessions:'777'}}}).catch(()=>{});});
 const requested=page.waitForRequest('**/api/v1/analysis/funnel*platform=google*');await page.getByLabel('Paid channel').selectOption('google');await requested;
 await expect(page.getByText('Loading selection; showing previous analysis')).toBeVisible();
 await page.getByLabel('Paid channel').selectOption('meta');await expect(page.getByText('Loading selection; showing previous analysis')).toHaveCount(0);release();
 await page.waitForTimeout(250);await expect(page.getByText('777',{exact:true})).toHaveCount(0);
});

test.beforeEach(async({page})=>{await page.route('**/api/v1/auth/session',r=>r.fulfill({json:{user_id:'fixture-admin',username:'fixture-admin',role:'admin'}}));});

test('analysis URL restores model and filters across reload and back',async({page})=>{
 await page.goto('/analysis?from_date=2026-01-01&to_date=2026-01-03&platform=google&campaign_id=a&model=linear');
 await expect(page.getByLabel('Journey attribution model')).toHaveValue('linear');await expect(page.getByLabel('Paid channel')).toHaveValue('google');await expect(page.getByLabel('Campaign',{exact:true})).toHaveValue('a');
 await page.getByLabel('Journey attribution model').selectOption('first_touch');await expect(page).toHaveURL(/model=first_touch/);await page.reload();await expect(page.getByLabel('Journey attribution model')).toHaveValue('first_touch');
 await page.goBack();await expect(page.getByLabel('Journey attribution model')).toHaveValue('linear');await page.goForward();await expect(page.getByLabel('Journey attribution model')).toHaveValue('first_touch');
});

test('automatic analysis dates do not trap Back after an undated entry',async({page})=>{
 await page.goto('/login');await page.goto('/analysis');await expect(page).toHaveURL(/from_date=/);await page.goBack();await expect(page).toHaveURL(url=>url.pathname==='/login');
});
