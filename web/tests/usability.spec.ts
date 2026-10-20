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
