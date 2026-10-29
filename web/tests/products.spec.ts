import {test,expect} from '@playwright/test';

const provenance={identity:'a'.repeat(64),generation:'1',proof_digest:'b'.repeat(64),model:'gold.commerce_daily',serving_relation:'commerce_daily',historical:true,currency:'VND'};
const products={status:'available',currency:'VND',provenance,definition:'Certified order lines by product and purchase device.',
 categories:[{category:'electronics',units:'6',gross_revenue_minor:'800',net_revenue_minor:'700',brands:[{brand:'Nova',units:'5',gross_revenue_minor:'700',net_revenue_minor:'600'},{brand:'Volt',units:'1',gross_revenue_minor:'100',net_revenue_minor:'100'}]},
  {category:'home',units:'2',gross_revenue_minor:'100',net_revenue_minor:'-20',brands:[{brand:'Lumen',units:'2',gross_revenue_minor:'100',net_revenue_minor:'-20'}]}],
 top_products:[{product_id:'p-1',product_name:'Nova Phone',category:'electronics',brand:'Nova',units:'5',orders:'3',net_revenue_minor:'600'}],
 devices:[{device_type:'mobile',units:'4',gross_revenue_minor:'600',net_revenue_minor:'500',gross_share:'0.666666',avg_item_value_minor:'150'},{device_type:'unknown',units:'4',gross_revenue_minor:'300',net_revenue_minor:'180',gross_share:'0.333333',avg_item_value_minor:'75'}]};

test.beforeEach(async({page})=>{await page.route('**/api/v1/**',r=>{const p=new URL(r.request().url()).pathname;
 return r.fulfill({json:p.endsWith('auth/session')?{user_id:'a',username:'admin',role:'admin'}:p.endsWith('/insights/products')?products:[]});});});

test('products leads with a category and brand treemap of positive net revenue',async({page})=>{
 await page.goto('/products');await expect(page.getByRole('navigation',{name:'Workspaces'}).getByRole('link',{name:'Products'})).toHaveAttribute('aria-current','page');
 await expect(page.getByRole('heading',{name:'Products',level:1})).toBeVisible();
 const map=page.getByRole('img',{name:'Net revenue by category and brand'});await expect(map).toBeVisible();
 await expect(map).toContainText('Electronics');
 await expect(page.getByText('Categories with negative net revenue are listed below the map',{exact:false})).toBeVisible();
 const table=page.getByRole('table',{name:'Net revenue by category'});
 await expect(table.getByRole('row',{name:/Home/})).toContainText('-₫20');
});

test('top products and devices show exact certified values without inventing order counts per device',async({page})=>{
 await page.goto('/products');
 await expect(page.getByRole('table',{name:'Top products by net revenue'}).getByRole('row',{name:/Nova Phone/})).toContainText('3');
 const devices=page.getByRole('region',{name:'Revenue by purchase device'});
 await expect(devices.getByRole('article',{name:'Mobile'})).toContainText('66.6%');
 await expect(devices.getByRole('article',{name:'Mobile'})).toContainText('₫150');
 await expect(devices.getByRole('article',{name:'Unknown device'})).toBeVisible();
 await expect(devices).not.toContainText('AOV');
});

test('a release without product commerce says so instead of crashing',async({page})=>{
 await page.route('**/api/v1/insights/products',r=>r.fulfill({json:{status:'unavailable',message:'This certified release predates product commerce; publish a newer release.',provenance}}));
 await page.goto('/products');await expect(page.getByText('predates product commerce',{exact:false})).toBeVisible();
});

test('mixed currency or reconciliation failures surface as unavailable',async({page})=>{
 await page.route('**/api/v1/insights/products',r=>r.fulfill({status:409,json:{code:'unsupported_currency',message:'unsupported currency'}}));
 await page.goto('/products');await expect(page.getByText('unsupported currency',{exact:false})).toBeVisible();
});

test('products fits a phone',async({page})=>{
 await page.setViewportSize({width:390,height:844});await page.goto('/products');
 await expect(page.getByRole('img',{name:'Net revenue by category and brand'})).toBeVisible();
 expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBeTruthy();
});
