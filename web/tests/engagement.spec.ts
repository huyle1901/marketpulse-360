import {test,expect} from '@playwright/test';

const provenance={identity:'a'.repeat(64),generation:'1',proof_digest:'b'.repeat(64),as_of:'2026-02-03T00:00:00Z',model:'silver.customer_events projection',serving_relation:'journey events',data_through:'2026-02-03T00:00:00Z',observed_at:'2026-02-03T00:00:00Z',historical:true,currency:'VND'};
const email={status:'available',provenance,definition:'Rates use delivered emails as the denominator.',
 months:[{month:'2026-01-01',sent:'10',delivered:'8',opened:'4',clicked:'1',unsubscribed:'0',recipients:'7',open_rate:'0.500000',click_rate:'0.125000',unsubscribe_rate:'0.000000'},
  {month:'2026-02-01',sent:'5',delivered:'0',opened:'0',clicked:'0',unsubscribed:'0',recipients:'5',open_rate:null,click_rate:null,unsubscribe_rate:null}],
 totals:{sent:'15',delivered:'8',opened:'4',clicked:'1',unsubscribed:'0',open_rate:'0.500000',click_rate:'0.125000',unsubscribe_rate:'0.000000'}};
const mix={status:'available',currency:'VND',provenance,definition:'A profile is new in the month of its first original order.',
 months:[{month:'2026-01-01',new_customers:'3',returning_customers:'0',new_orders:'3',returning_orders:'0',new_gross_minor:'9007199254740993',returning_gross_minor:'0',returning_share:'0.000000'},
  {month:'2026-02-01',new_customers:'1',returning_customers:'2',new_orders:'1',returning_orders:'4',new_gross_minor:'5',returning_gross_minor:'7',returning_share:'0.666666'}]};
const cohorts={provenance,population:'Historical governed analytics',consent_policy:'Both consent flags required.',totals:{profiles_total:'4',activation_eligible_total:'2'},rows:[]};

test.beforeEach(async({page})=>{
 await page.route('**/api/v1/**',r=>{const p=new URL(r.request().url()).pathname;
  return r.fulfill({json:p.endsWith('auth/session')?{user_id:'a',username:'admin',role:'admin'}:p.endsWith('email-engagement')?email:p.endsWith('customer-mix')?mix:p.endsWith('cohorts')?cohorts:p.endsWith('/profiles')?{provenance,purpose:'analytics',status:'available',rows:[],total:'0',limit:'20',offset:'0',has_more:false}:[]});});
});

test('email workspace summarizes the delivery funnel with honest rates',async({page})=>{
 await page.goto('/email');await expect(page.getByRole('navigation',{name:'Workspaces'}).getByRole('link',{name:'Email'})).toHaveAttribute('aria-current','page');
 await expect(page.getByRole('heading',{name:'Email',level:1})).toBeVisible();
 const kpis=page.getByRole('region',{name:'Email totals'});const card=(name:string)=>kpis.getByRole('article',{name});
 await expect(card('Emails sent')).toContainText('15');await expect(card('Open rate')).toContainText('50.0%');
 await expect(card('Click rate')).toContainText('12.5%');await expect(card('Unsubscribe rate')).toContainText('0.0%');
 await expect(page.getByRole('img',{name:'Monthly emails sent, opened and clicked'})).toBeVisible();
 const table=page.getByRole('table',{name:'Email engagement by month'});
 await expect(table.locator('tbody tr').nth(0)).toContainText('Unavailable');
 await expect(page.getByText('not credited with revenue',{exact:false})).toBeVisible();
});

test('an unpublished projection leaves email unavailable rather than zero',async({page})=>{
 await page.route('**/api/v1/lifecycle/email-engagement',r=>r.fulfill({json:{status:'unavailable',months:[],message:'Publish the pinned journey detail for this certified release first.',provenance}}));
 await page.goto('/email');await expect(page.getByText('Publish the pinned journey detail',{exact:false})).toBeVisible();
 await expect(page.getByRole('region',{name:'Email totals'})).toHaveCount(0);
});

test('customers shows new versus returning buyers per month with exact counts',async({page})=>{
 await page.goto('/customers');const region=page.getByRole('region',{name:'New vs returning customers'});
 await expect(region.getByRole('img',{name:'Monthly new and returning ordering customers'})).toBeVisible();
 await region.getByText('Exact monthly values',{exact:true}).click();const table=region.getByRole('table',{name:'New and returning customers by month'});
 await expect(table.locator('tbody tr').nth(1)).toContainText('66.6%');await expect(table.locator('tbody tr').first()).toContainText('₫9,007,199,254,740,993');
});

test('email fits a phone',async({page})=>{
 await page.setViewportSize({width:390,height:844});await page.goto('/email');
 await expect(page.getByRole('region',{name:'Email totals'})).toBeVisible();
 expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBeTruthy();
});
