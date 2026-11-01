import {test,expect} from '@playwright/test';
import {expectReadableLegend} from './legend-marks';

const provenance={identity:'a'.repeat(64),generation:'1',proof_digest:'b'.repeat(64),model:'gold.customer_360 + customers',serving_relation:'customer_360',historical:true,currency:'VND'};
const city=(name:string,net:string,customers='10',orders='12')=>({country:'VN',city:name,customers,orders,net_revenue_minor:net,net_per_customer_minor:String(BigInt(net)/BigInt(customers))});
const geography={status:'available',currency:'VND',provenance,total_net_revenue_minor:'9000',definition:'Identified customers by their master-record city.',
 cities:[city('Ho Chi Minh City','4000','10','12'),city('Ha Noi','3000','30','40'),city('Hue','1500'),city('Can Tho','-500')],unknown:{profiles:'40',orders:'7',net_revenue_minor:'1000'}};

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

test('the map draws Vietnam with Hoang Sa and Truong Sa, sizes bubbles by area and explains the size',async({page})=>{
 await page.goto('/geography');const map=page.getByRole('img',{name:'Customer net revenue by city'});
 await expect(map.locator('path[data-country="VN"]')).toHaveCount(1);
 await expect(map).toContainText('Hoàng Sa');await expect(map).toContainText('Trường Sa');
 const r=async(name:string)=>Number(await map.locator(`circle[data-city="${name}"]`).getAttribute('r'));
 expect((await r('Ho Chi Minh City'))/(await r('Ha Noi'))).toBeCloseTo(Math.sqrt(4000/3000),2);
 await expect(page.getByRole('list',{name:'Bubble size legend'}).getByRole('listitem')).toHaveCount(3);
});

test('switching the measure re-ranks cities and renames the map',async({page})=>{
 await page.goto('/geography');await page.getByRole('group',{name:'Map measure'}).getByRole('button',{name:'Customers'}).click();
 await expect(page.getByRole('img',{name:'Customer customers by city'})).toBeVisible();
 await expect(page.getByRole('list',{name:'Cities ranked by customers'}).getByRole('listitem').first()).toContainText('Ha Noi');
});

test('hovering a ranked city highlights its bubble',async({page})=>{
 await page.goto('/geography');
 await page.getByRole('list',{name:'Cities ranked by net revenue'}).getByRole('listitem').filter({hasText:'Ha Noi'}).hover();
 await expect(page.locator('circle[data-city="Ha Noi"]')).toHaveAttribute('data-active','true');
 await expect(page.locator('circle[data-city="Ho Chi Minh City"]')).toHaveAttribute('data-active','false');
});

for(const theme of ['light','dark'] as const)test(`regions split located revenue with a marked legend in ${theme} mode`,async({page})=>{
 await page.addInitScript(t=>localStorage.setItem('mp-theme',t),theme);await page.goto('/geography');
 const regions=page.getByRole('region',{name:'Net revenue by region'});
 await expect(regions).toContainText('North');await expect(regions).toContainText('South');
 await expectReadableLegend(regions.getByRole('list',{name:'Region legend'}));
 await expect(page.getByRole('region',{name:'Location coverage'})).toContainText('Known city');
});
