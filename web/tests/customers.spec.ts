import {test,expect} from '@playwright/test';
import {createHash} from 'node:crypto';
const id='d'.repeat(64),segmentId='11111111-1111-4111-8111-111111111111';
const prov={identity:'a'.repeat(64),generation:'1',proof_digest:'b'.repeat(64),as_of:'2026-01-03T00:00:00Z',historical:true,model:'gold.customer_360',data_through:'2026-01-03T00:00:00Z',projection_checksum:'c'.repeat(64),silver_snapshot_id:'123'};
const profile={profile_ref:id,profile_type:'customer',first_activity_at:'2026-01-01T00:00:00Z',last_activity_at:'2026-01-02T00:00:00Z',cohort_month:'2025-12-01',event_count:'3',session_count:'1',order_count:'1',refund_count:'1',gross_revenue_minor:'9007199254740993',refund_amount_minor:'3',net_revenue_minor:'9007199254740990',recency_days:'2',last_purchase_at:'2026-01-01T00:00:00Z',add_to_cart_count:'1',email_click_count:'0',consent_analytics:true,crm_matched:true,crm_consent_analytics:true,crm_consent_marketing:true,activation_eligible:true,activation_exclusion_reasons:[],confidence:'deterministic_customer_reference',resolution_basis:'Explicit ID or existing deterministic session stitch.',detail_available:true,opportunities:{high_value:true,churn_risk:false,cart_opportunity:false}};
const cohorts={provenance:prov,population:'Historical governed analytics, not an activation population',consent_policy:'Both certified analytics and marketing consent required.',totals:{profiles_total:'2',activation_eligible_total:'1'},rows:[{cohort_month:'2025-12-01',activity_month:'2026-01-01',profile_type:'customer',month_number:'1',cohort_size:'2',active_profiles:'1',retained_rate:'0.500000'}]};
const profiles={provenance:prov,purpose:'analytics',status:'available',rows:[profile],total:'1',limit:'20',offset:'0',has_more:false};
const timeline={provenance:prov,status:'available',profile,events:[{event_ref:'e',event_name:'order_refunded',event_time:'2026-01-02T00:00:00Z',source:'commerce',campaign_name:'Campaign A',gross_revenue_minor:'0',refund_amount_minor:'3'}],total:'1',limit:'50',offset:'0',has_more:false};
const emptyRule={schema_version:'mp08-segment-v1',purpose:'activation',cohort_month:null,activity_month:null,profile_type:null,conditions:[]};
const canonical=(value:unknown):string=>JSON.stringify(value&&typeof value==='object'&&!Array.isArray(value)?Object.fromEntries(Object.entries(value as object).sort(([a],[b])=>a.localeCompare(b)).map(([k,v])=>[k,JSON.parse(canonical(v))])):Array.isArray(value)?value.map(v=>JSON.parse(canonical(v))):value);
const digest=(rule:unknown)=>createHash('sha256').update(canonical(rule)).digest('hex');
let saved:{segment_id:string;version:string;name:string;rule:typeof emptyRule;rule_digest:string}[]=[];
test.beforeEach(async({page})=>{saved=[];await page.route('**/api/v1/lifecycle/**',route=>{
 const url=new URL(route.request().url()),path=url.pathname,method=route.request().method();
 if(path.endsWith('/cohorts'))return route.fulfill({json:cohorts});
 if(path.includes('/timeline'))return route.fulfill({json:timeline});
 if(path.endsWith('/profiles'))return route.fulfill({json:{...profiles,purpose:url.searchParams.get('purpose')||'analytics'}});
 if(path.endsWith('/preview')){const rule=route.request().postDataJSON().rule;return route.fulfill({json:{rows:[profile],matched_total:'1',excluded_from_activation_total:'1',purpose:rule.purpose,rule_digest:digest(rule),evaluation_key:'e'.repeat(64),provenance:prov,sample_truncated:false,sample_limit:'20'}});}
 if(path.endsWith('/customer-mix')||path.endsWith('/email-engagement'))return route.fulfill({json:{status:'unavailable',months:[],message:'Projection unavailable in this fixture',provenance:prov}});
 if(path.endsWith('/segments')&&method==='GET')return route.fulfill({json:saved.slice(-1)});
 if(path.includes('/versions/'))return route.fulfill({json:saved[Number(path.split('/').pop())-1]});
 const body=route.request().postDataJSON();const result={segment_id:segmentId,version:String(saved.length+1),name:body.name,rule:body.rule,rule_digest:digest(body.rule)};saved.push(result);return route.fulfill({json:result});
});});

test('cohort drill, exact RFM, consent and timeline are inspectable',async({page})=>{
 await page.goto('/customers');await expect(page.getByRole('heading',{name:'Customers',level:1})).toBeVisible();
 await expect(page.getByText(cohorts.population,{exact:true})).toBeVisible();
 await expect(page.getByText('9,007,199,254,740,990',{exact:true}).first()).toBeVisible();
 await page.getByRole('button',{name:'Explore cohort 2025-12-01 customer active 2026-01-01'}).click();
 await expect(page.getByLabel('Activity month',{exact:true})).toHaveValue('2026-01');
 await page.getByRole('button',{name:'Inspect profile '+id.slice(0,12)}).click();
 await expect(page.getByText('deterministic_customer_reference',{exact:true})).toBeVisible();await expect(page.getByText('order_refunded',{exact:true})).toBeVisible();
});
test('segment preview, revision save and immutable historical reload',async({page})=>{
 await page.goto('/customers');await page.getByLabel('Segment name').fill('Consented audience');
 await page.getByRole('button',{name:'Preview segment',exact:true}).click();await expect(page.getByText('1 matched · 1 excluded globally from activation',{exact:true})).toBeVisible();
 await page.getByRole('button',{name:'Save segment',exact:true}).click();await expect(page.getByText('Saved revision 1',{exact:true})).toBeVisible();
 await page.getByLabel('Segment name').fill('Updated audience');await page.getByRole('button',{name:'Save segment',exact:true}).click();await expect(page.getByText('Saved revision 2',{exact:true})).toBeVisible();
 await page.getByLabel('Revision to inspect').fill('1');await page.getByRole('button',{name:'Load revision',exact:true}).click();await expect(page.getByLabel('Segment name')).toHaveValue('Consented audience');
 await expect(page.getByRole('button',{name:'Save segment',exact:true})).toBeDisabled();await expect(page.getByText('Historical revision 1 · Read-only',{exact:true})).toBeVisible();
});
test('mobile and denied timeline/unavailable detail states stay honest',async({page})=>{
 await page.setViewportSize({width:390,height:844});await page.route('**/api/v1/lifecycle/profiles/*/timeline*',route=>route.fulfill({json:{...timeline,status:'consent_restricted',events:[],message:'Latest certified CRM analytics consent is denied; timeline detail is suppressed.'}}));
 await page.goto('/customers');await page.getByRole('button',{name:'Inspect profile '+id.slice(0,12)}).click();await expect(page.getByText('Latest certified CRM analytics consent is denied; timeline detail is suppressed.')).toBeVisible();
 await page.getByLabel('Segment purpose').focus();await page.keyboard.press('ArrowUp');expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBeTruthy();
});
test('old rule preview is discarded and save draft is locked',async({page})=>{
 await page.goto('/customers');let release!:()=>void;const pending=new Promise<void>(resolve=>{release=resolve;});
 let completed!:()=>void;const done=new Promise<void>(resolve=>{completed=resolve;});
 await page.route('**/api/v1/lifecycle/segments/preview',async route=>{const body=route.request().postDataJSON();await pending;try{await route.fulfill({json:{provenance:prov,rule_digest:digest(body.rule),rows:[],matched_total:'777',excluded_from_activation_total:'0',purpose:'activation'}});}catch{}finally{completed();}});
 const requested=page.waitForRequest('**/api/v1/lifecycle/segments/preview');await page.getByRole('button',{name:'Preview segment',exact:true}).click();await requested;
 await page.getByRole('button',{name:'Add condition',exact:true}).click();release();await done;await page.waitForTimeout(200);await expect(page.getByText(/777 matched/)).toHaveCount(0);
 let finish!:()=>void;const saving=new Promise<void>(resolve=>{finish=resolve;});await page.route('**/api/v1/lifecycle/segments',async route=>{if(route.request().method()==='POST'){await saving;await route.fulfill({json:{segment_id:segmentId,version:'1',name:'A',rule:emptyRule,rule_digest:digest(emptyRule)}});}else await route.fulfill({json:[]});});
 await page.getByLabel('Segment name').fill('A');const write=page.waitForRequest(r=>r.method()==='POST'&&r.url().endsWith('/segments'));await page.getByRole('button',{name:'Save segment',exact:true}).click();await write;await expect(page.getByLabel('Segment name')).toBeDisabled();finish();
});
test('mixed release cohort/profile batch is rejected',async({page})=>{
 await page.goto('/customers');await expect(page.getByText('9,007,199,254,740,990').first()).toBeVisible();
 await page.route('**/api/v1/lifecycle/profiles*',route=>route.fulfill({json:{...profiles,provenance:{...prov,identity:'f'.repeat(64)}}}));
 await page.getByRole('button',{name:'Refresh customers',exact:true}).click();await expect(page.getByText('Release changed between reads; refresh customers again.')).toBeVisible();await expect(page.getByText('Refresh failed; showing last successful customers')).toBeVisible();
});

test('a delayed old profile cannot replace the selected timeline',async({page})=>{
 const otherId='f'.repeat(64);await page.route('**/api/v1/lifecycle/profiles?*',route=>route.fulfill({json:{...profiles,rows:[profile,{...profile,profile_ref:otherId}]}}));
 let release!:()=>void,completed!:()=>void;const held=new Promise<void>(resolve=>{release=resolve;}),done=new Promise<void>(resolve=>{completed=resolve;});
 await page.route('**/profiles/'+id+'/timeline*',async route=>{await held;try{await route.fulfill({json:timeline});}catch{}finally{completed();}});
 await page.route('**/profiles/'+otherId+'/timeline*',route=>route.fulfill({json:{...timeline,profile:{...profile,profile_ref:otherId}}}));
 await page.goto('/customers');const requested=page.waitForRequest('**/profiles/'+id+'/timeline*');await page.getByRole('button',{name:'Inspect profile '+id.slice(0,12)}).click();await requested;
 await page.getByRole('button',{name:'Inspect profile '+otherId.slice(0,12)}).click();await expect(page.getByRole('heading',{name:'Profile '+otherId.slice(0,12),exact:true})).toBeVisible();
 release();await done;await page.waitForTimeout(200);await expect(page.getByRole('heading',{name:'Profile '+id.slice(0,12),exact:true})).toHaveCount(0);
});
test('release replacement invalidates an already displayed preview',async({page})=>{
 await page.goto('/customers');await page.getByRole('button',{name:'Preview segment',exact:true}).click();await expect(page.getByText('1 matched · 1 excluded globally from activation',{exact:true})).toBeVisible();
 const changed={...prov,identity:'b'.repeat(64)};
 await page.route('**/api/v1/lifecycle/cohorts',route=>route.fulfill({json:{...cohorts,provenance:changed}}));await page.route('**/api/v1/lifecycle/profiles?*',route=>route.fulfill({json:{...profiles,provenance:changed}}));
 await page.getByRole('button',{name:'Refresh customers',exact:true}).click();await expect(page.locator('details').filter({has:page.locator('summary',{hasText:'Cohort source & freshness'})})).toContainText('b'.repeat(64));await expect(page.getByText('1 matched · 1 excluded globally from activation',{exact:true})).toHaveCount(0);
});
test('missing detail leaves certified cohorts available and negative net stays exact',async({page})=>{
 await page.route('**/api/v1/lifecycle/profiles?*',route=>route.fulfill({json:{...profiles,status:'unavailable',rows:[],total:null,message:'Publish pinned detail for this release.'}}));
 await page.goto('/customers');await expect(page.getByRole('heading',{name:'Cohort retention',exact:true})).toBeVisible();await expect(page.getByText('Publish pinned detail for this release.')).toBeVisible();
 await page.route('**/api/v1/lifecycle/profiles?*',route=>route.fulfill({json:{...profiles,rows:[{...profile,net_revenue_minor:'-9007199254740993'}]}}));await page.getByRole('button',{name:'Refresh customers',exact:true}).click();await expect(page.getByText('-9,007,199,254,740,993',{exact:true})).toBeVisible();
});

test.beforeEach(async({page})=>{await page.route('**/api/v1/auth/session',r=>r.fulfill({json:{user_id:'fixture-admin',username:'fixture-admin',role:'admin'}}));});

test('customer URL restores purpose, paging and local search while invalid offsets normalize',async({page})=>{
 await page.goto('/customers?purpose=activation&profile_type=customer&offset=20&q=dddd&sort=net_desc');
 await expect(page.getByLabel('Discovery purpose')).toHaveValue('activation');await expect(page.getByLabel('Profile type',{exact:true})).toHaveValue('customer');await expect(page.getByLabel('Search loaded profiles')).toHaveValue('dddd');
 await page.reload();await expect(page.getByLabel('Discovery purpose')).toHaveValue('activation');await expect(page.getByText('Search and sort apply to this loaded page only.',{exact:true})).toBeVisible();
 await page.goto('/customers?purpose=invalid&profile_type=invalid&offset=9007199254740993');await expect(page.getByLabel('Discovery purpose')).toHaveValue('analytics');await expect(page.getByLabel('Profile type',{exact:true})).toHaveValue('');await expect(page).not.toHaveURL(/9007199254740993/);
});

test('retention heatmap distinguishes observed zero from missing cells and keeps denominators',async({page})=>{
 const rows=[...cohorts.rows,{cohort_month:'2026-01-01',activity_month:'2026-01-01',profile_type:'customer',month_number:'0',cohort_size:'3',active_profiles:'0',retained_rate:'0.000000'}];
 await page.route('**/api/v1/lifecycle/cohorts',r=>r.fulfill({json:{...cohorts,rows}}));
 await page.goto('/customers');const table=page.getByRole('table',{name:'Retention heatmap'});
 await expect(table).toContainText('50%');await expect(table).toContainText('1 / 2 active');await expect(table).toContainText('0%');await expect(table).toContainText('0 / 3 active');await expect(table.getByText('Unavailable',{exact:true})).toHaveCount(2);
 await table.getByRole('button',{name:'Explore cohort 2025-12-01 customer active 2026-01-01',exact:true}).click();await expect(page.getByLabel('Activity month',{exact:true})).toHaveValue('2026-01');
});
test('profile panel receives focus, closes with Escape and returns to its trigger',async({page})=>{
 await page.goto('/customers');const trigger=page.getByRole('button',{name:'Inspect profile '+id.slice(0,12)});await trigger.click();
 const panel=page.getByRole('region',{name:'Governed profile timeline',exact:true});await expect(panel).toBeFocused();await expect(panel.getByText('order_refunded',{exact:true})).toBeVisible();
 await page.keyboard.press('Escape');await expect(panel).toHaveCount(0);await expect(trigger).toBeFocused();
});
test('segment summary explains the current draft and consent boundary',async({page})=>{
 await page.goto('/customers');const summary=page.getByRole('region',{name:'Segment rule summary'});await expect(summary).toContainText('Activation preview');await expect(summary).toContainText('Both certified consent flags required');
 await page.getByRole('button',{name:'Add condition',exact:true}).click();await page.getByLabel('Condition 1 whole value').fill('30');await expect(summary).toContainText('recency days at least 30');
 await page.getByLabel('Segment purpose').selectOption('analytics');await expect(summary).toContainText('Historical analytics');await expect(summary).toContainText('Membership is evaluated in preview');
});
