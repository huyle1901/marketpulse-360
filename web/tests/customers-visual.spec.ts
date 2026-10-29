import {test,expect} from '@playwright/test';
const prov={identity:'a'.repeat(64),generation:'1',proof_digest:'b'.repeat(64),as_of:'2026-01-03T00:00:00Z',historical:true,model:'gold.customer_360',serving_relation:'customer_360',data_through:'2026-01-03T00:00:00Z',observed_at:'2026-01-03T00:00:00Z'};
const cohorts={provenance:prov,population:'Historical governed analytics, not an activation population',consent_policy:'Both consent flags required.',totals:{profiles_total:'60',activation_eligible_total:'37'},
 rows:[{cohort_month:'2025-12-01',activity_month:'2025-12-01',profile_type:'customer',month_number:'0',cohort_size:'40',active_profiles:'40',retained_rate:'1.000000'},{cohort_month:'2025-12-01',activity_month:'2026-02-01',profile_type:'customer',month_number:'2',cohort_size:'40',active_profiles:'0',retained_rate:'0.000000'}]};
const totals:Record<string,string|null>={customer:'40',anonymous:'15',session:'5',high_value:'9',churn_risk:'12',cart_opportunity:null};
let identity='a'.repeat(64);
test.beforeEach(async({page})=>{identity='a'.repeat(64);
 await page.route('**/api/v1/auth/session',r=>r.fulfill({json:{user_id:'a',username:'admin',role:'admin'}}));
 await page.route('**/api/v1/lifecycle/**',route=>{const url=new URL(route.request().url()),p=url.pathname;
  if(p.endsWith('/cohorts'))return route.fulfill({json:cohorts});
  if(p.endsWith('/profiles')){const key=url.searchParams.get('profile_type')||url.searchParams.get('opportunity');const total=key?totals[key]:'60';
   return route.fulfill({json:{provenance:{...prov,identity:key?identity:prov.identity},purpose:'analytics',status:total===null?'unavailable':'available',message:total===null?'Event detail unavailable':undefined,rows:[],total,limit:url.searchParams.get('limit')||'20',offset:'0',has_more:false}});}
  return route.fulfill({json:[]});});
});
test('customers workspace leads with exact profile mix and opportunity counts',async({page})=>{
 await page.goto('/customers');await expect(page.getByRole('heading',{name:'Customers',level:1})).toBeVisible();
 const mix=page.getByRole('region',{name:'Profile mix'});
 await expect(mix.getByRole('img',{name:'Customer profiles: 40 of 60'})).toBeVisible();
 await expect(mix.getByRole('img',{name:'Anonymous device profiles: 15 of 60'})).toBeVisible();
 await expect(mix.getByRole('img',{name:'Session-only profiles: 5 of 60'})).toBeVisible();
 const opportunity=page.getByRole('region',{name:'Opportunity heuristics'});
 await expect(opportunity).toContainText('High value');await expect(opportunity).toContainText('9');
 await expect(opportunity).toContainText('Churn risk');await expect(opportunity).toContainText('12');
 await expect(opportunity).toContainText('Cart opportunity');await expect(opportunity).toContainText('Unavailable');
 await expect(opportunity).toContainText('rule-based, not predictions');
});
test('summary counts from another release are withheld',async({page})=>{
 identity='f'.repeat(64);await page.goto('/customers');
 await expect(page.getByRole('region',{name:'Profile mix'})).toContainText('different certified release');
 await expect(page.getByRole('region',{name:'Profile mix'}).getByRole('img')).toHaveCount(0);
});
test('observed zero retention keeps its denominator while unobserved months stay unavailable',async({page})=>{
 await page.goto('/customers');const heatmap=page.getByRole('table',{name:'Retention heatmap'});
 await expect(heatmap).toContainText('0 / 40 active');await expect(heatmap).toContainText('Unavailable');
});
test('opportunity counts drill into the profile list',async({page})=>{
 await page.goto('/customers');await page.getByRole('region',{name:'Opportunity heuristics'}).getByRole('button',{name:'Show churn risk profiles'}).click();
 await expect(page).toHaveURL(/opportunity=churn_risk/);
});
test('an observed zero count stays zero rather than unavailable',async({page})=>{
 await page.route('**/api/v1/lifecycle/profiles?*high_value*',r=>r.fulfill({json:{provenance:prov,purpose:'analytics',status:'empty',rows:[],total:'0',limit:'1',offset:'0',has_more:false}}));
 await page.goto('/customers');const opportunity=page.getByRole('region',{name:'Opportunity heuristics'});
 await expect(opportunity.getByRole('button',{name:'Show high value profiles'})).toHaveText('0');
});
