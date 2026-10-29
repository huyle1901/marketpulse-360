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

const credit=(model:string,channel:string,net:string,extra={})=>({model,channel,gross_revenue_minor:net.startsWith('-')?'0':net,refund_amount_minor:net.startsWith('-')?net.slice(1):'0',net_revenue_minor:net,allocated_order_count:'1.000000',...extra});
const visual={...attribution,
 channels:[credit('first_touch','google','600'),credit('last_touch','google','400'),credit('linear','google','500'),credit('first_touch','unattributed','-50'),credit('last_touch','unattributed','0'),credit('linear','unattributed','-25')],
 campaigns:[credit('first_touch','google','600',{campaign_id:'a',campaign_name:'Campaign A'}),credit('last_touch','google','400',{campaign_id:'a',campaign_name:'Campaign A'}),credit('linear','google','500',{campaign_id:'a',campaign_name:'Campaign A'})]};
test.beforeEach(async({page})=>{
 await page.route('**/api/v1/auth/session',r=>r.fulfill({json:{user_id:'a',username:'admin',role:'admin'}}));
 await page.route('**/api/v1/analysis/**',route=>{const url=new URL(route.request().url()),name=url.pathname.split('/').pop();return route.fulfill({json:name==='funnel'?funnel:name==='attribution'?visual:{...journeys,model:url.searchParams.get('model')||'last_touch'}});});
});
test('journey analysis leads with grouped attribution bars for all three models',async({page})=>{
 await page.goto('/analysis');await expect(page.getByRole('heading',{name:'Journey analysis',level:1})).toBeVisible();
 const chart=page.getByRole('region',{name:'Attribution credit by channel'});
 await expect(chart.getByRole('img')).toHaveCount(2);
 await expect(chart.getByRole('img',{name:/google: First touch ₫600, Last touch ₫400, Linear ₫500/})).toBeVisible();
 await expect(chart.getByRole('img',{name:/Unattributed: First touch -₫50, Last touch ₫0, Linear -₫25/})).toBeVisible();
 await expect(chart.locator('.credit-bar.negative')).toHaveCount(2);
 await expect(chart).toContainText('Attribution credit, not source campaign finance');
});
test('an explicit model selector drives campaign credit and journey sampling',async({page})=>{
 await page.goto('/analysis');const group=page.getByRole('group',{name:'Attribution model'});
 await expect(group.getByRole('button',{name:'Last touch'})).toHaveAttribute('aria-pressed','true');
 await group.getByRole('button',{name:'Linear'}).click();await expect(page).toHaveURL(/model=linear/);
 const campaigns=page.getByRole('region',{name:'Campaign credit'});await expect(campaigns).toContainText('Linear');await expect(campaigns).toContainText('₫500');
 await expect(page.getByText('Sampling model: Linear',{exact:true})).toBeVisible();
});
test('exact credit tables are one step away and reconciliation stays visible',async({page})=>{
 await page.goto('/analysis');await expect(page.getByText('All three models reconcile',{exact:true})).toBeVisible();
 await expect(page.getByRole('table',{name:/Channel credited net comparison/})).not.toBeVisible();
 await page.getByText('Exact credit tables',{exact:true}).click();await expect(page.getByRole('table',{name:/Channel credited net comparison/})).toBeVisible();
});
test('journey analysis fits a phone viewport',async({page})=>{
 await page.setViewportSize({width:390,height:844});await page.goto('/analysis');
 await expect(page.getByRole('region',{name:'Attribution credit by channel'})).toBeVisible();
 expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBeTruthy();
});
