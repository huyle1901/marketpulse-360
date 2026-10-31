import {test,expect} from '@playwright/test';

const provenance={identity:'a'.repeat(64),generation:'1',proof_digest:'b'.repeat(64),model:'gold.customer_360 + customers',serving_relation:'customer_360',historical:true,currency:'VND'};
const city=(name:string,net:string,customers='10',orders='12')=>({country:'VN',city:name,customers,orders,net_revenue_minor:net,net_per_customer_minor:String(BigInt(net)/BigInt(customers))});
const geography={status:'available',currency:'VND',provenance,total_net_revenue_minor:'9000',definition:'Identified customers by their master-record city.',
 cities:[city('Ho Chi Minh City','4000'),city('Ha Noi','3000'),city('Hue','1500'),city('Can Tho','-500')],unknown:{profiles:'40',orders:'7',net_revenue_minor:'1000'}};

test.beforeEach(async({page})=>{await page.route('**/api/v1/**',r=>{const p=new URL(r.request().url()).pathname;
 return r.fulfill({json:p.endsWith('auth/session')?{user_id:'a',username:'admin',role:'admin'}:p.endsWith('/insights/geography')?geography:[]});});});

test('geography plots positive known cities at their coordinates and ranks every city',async({page})=>{
 await page.goto('/geography');await expect(page.getByRole('navigation',{name:'Workspaces'}).getByRole('link',{name:'Geography'})).toHaveAttribute('aria-current','page');
 await expect(page.getByRole('heading',{name:'Geography',level:1})).toBeVisible();
 const map=page.getByRole('img',{name:'Customer net revenue by city'});
 await expect(map).toContainText('Ho Chi Minh City');await expect(map).toContainText('Ha Noi');
 await expect(map.locator('circle[data-city]')).toHaveCount(2);
 await expect(page.getByRole('list',{name:'Cities ranked by net revenue'}).getByRole('listitem').first()).toContainText('Ho Chi Minh City');
 await page.getByText('Exact city values').click();const table=page.getByRole('table',{name:'Net revenue by city'});
 await expect(table.getByRole('row',{name:/Ho Chi Minh City/})).toContainText('₫4,000');
 await expect(table.getByRole('row',{name:/Can Tho/})).toContainText('-₫500');
 await expect(table.getByRole('row',{name:/Hue/})).toBeVisible();
 await expect(page.getByText('Not on the map: Hue (no bundled coordinates), Can Tho (net revenue not positive)',{exact:false})).toBeVisible();
});

test('anonymous and unmatched activity stays location unknown',async({page})=>{
 await page.goto('/geography');const card=page.getByRole('article',{name:'Location unknown'});
 await expect(card).toContainText('40 profiles');await expect(card).toContainText('₫1,000');
});

test('a release without customer location says so',async({page})=>{
 await page.route('**/api/v1/insights/geography',r=>r.fulfill({json:{status:'unavailable',message:'This certified release predates customer location; publish a newer release.',provenance}}));
 await page.goto('/geography');await expect(page.getByText('predates customer location',{exact:false})).toBeVisible();
});

test('geography fits a phone',async({page})=>{
 await page.setViewportSize({width:390,height:844});await page.goto('/geography');
 await expect(page.getByRole('img',{name:'Customer net revenue by city'})).toBeVisible();
 expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBeTruthy();
});
