import {test,expect} from '@playwright/test';

const provenance={identity:'a'.repeat(64),generation:'1',proof_digest:'b'.repeat(64),as_of:'2026-09-01T00:00:00Z',model:'m',serving_relation:'r',data_through:'2026-09-01T00:00:00Z',observed_at:'2026-09-01T00:00:00Z',historical:true,currency:'VND',timezone:'UTC'};
const cells=Array.from({length:168},(_,i)=>{const weekday=Math.floor(i/24)+1,hour=i%24;return {weekday:String(weekday),hour:String(hour),sessions:weekday===3&&hour===10?'12':String((i*7)%5),orders:weekday===3&&hour===10?'4':'0'};});
const activity={status:'available',timezone:'UTC',cells,busiest:{weekday:'3',hour:'10',sessions:'12'},provenance};
const orderValues={status:'available',currency:'VND',orders:'10',gross_minor:'9000000',median_minor:'1250000',p90_minor:'3400000',band_width_minor:'500000',
 bands:Array.from({length:8},(_,k)=>({from_minor:String(k*500000),to_minor:k===7?null:String((k+1)*500000),orders:k===0?'4':k===7?'6':'0',gross_minor:k===0?'1000000':k===7?'8000000':'0'})),provenance};
const loyalty={status:'available',currency:'VND',total_net_revenue_minor:'100',tiers:[{tier:'gold',customers:'2',orders:'3',net_revenue_minor:'60',net_per_customer_minor:'30'},{tier:'silver',customers:'0',orders:'0',net_revenue_minor:'0',net_per_customer_minor:null},{tier:'bronze',customers:'1',orders:'1',net_revenue_minor:'30',net_per_customer_minor:'30'},{tier:'none',customers:'4',orders:'2',net_revenue_minor:'10',net_per_customer_minor:'2'}],provenance};
const channels={status:'available',sources:[{source:'web',sessions:'100',orders:'5',conversion_rate:'0.050000'},{source:'mobile',sessions:'50',orders:'4',conversion_rate:'0.080000'},{source:'unknown',sessions:'0',orders:'1',conversion_rate:null}],provenance};
const bodies:Record<string,unknown>={activity,'order-values':orderValues,loyalty,channels};

test.beforeEach(async({page})=>{await page.route('**/api/v1/**',r=>{const p=new URL(r.request().url()).pathname;const name=p.split('/').pop()!;
 return r.fulfill({json:p.endsWith('auth/session')?{user_id:'a',username:'admin',role:'admin'}:p.includes('/insights/')?bodies[name]:[]});});});

test('insights leads with a weekday-hour heatmap and a busiest-slot callout',async({page})=>{
 await page.goto('/insights');await expect(page.getByRole('navigation',{name:'Workspaces'}).getByRole('link',{name:'Insights'})).toHaveAttribute('aria-current','page');
 const region=page.getByRole('region',{name:'When customers act'});
 const grid=region.getByRole('table',{name:'Sessions by weekday and hour (UTC)'});
 await expect(grid.getByRole('cell',{name:'Wednesday 10:00–11:00 UTC: 12 sessions'})).toBeVisible();
 await expect(region).toContainText('Busiest: Wednesday 10:00–11:00 UTC');
 await region.getByRole('button',{name:'Show orders'}).click();
 await expect(region.getByRole('table',{name:'Orders by weekday and hour (UTC)'}).getByRole('cell',{name:'Wednesday 10:00–11:00 UTC: 4 orders'})).toBeVisible();
});

test('order value distribution shows median, P90 and exact bands',async({page})=>{
 await page.goto('/insights');const region=page.getByRole('region',{name:'Order value distribution'});
 await expect(region.getByRole('article',{name:'Median order'})).toContainText('₫1,250,000');
 await expect(region.getByRole('article',{name:'90th percentile'})).toContainText('₫3,400,000');
 await expect(region.getByRole('img',{name:'Orders by order value'})).toBeVisible();
 await region.getByText('Exact bands',{exact:true}).click();
 await expect(region.getByRole('row',{name:/₫0 – ₫500,000/})).toContainText('4');
 await expect(region.getByRole('row',{name:/₫3,500,000 and above/})).toContainText('6');
});

test('loyalty tiers stay separate from customers without a tier',async({page})=>{
 await page.goto('/insights');const region=page.getByRole('region',{name:'Revenue by loyalty tier'});
 for(const name of ['Gold','Silver','Bronze','No CRM tier'])await expect(region.getByRole('article',{name})).toBeVisible();
 await expect(region.getByRole('article',{name:'Gold'})).toContainText('₫60');
 await expect(region.getByRole('article',{name:'Silver'})).toContainText('No customers');
});

test('web vs app compares sessions, orders and conversion',async({page})=>{
 await page.goto('/insights');const table=page.getByRole('table',{name:'Web vs app'});
 await expect(table.getByRole('row',{name:/Web/})).toContainText('5.0%');await expect(table.getByRole('row',{name:/App/})).toContainText('8.0%');
 await expect(table.getByRole('row',{name:/Unknown/})).toContainText('1');
});

test('unexpected insight bodies show unavailable, not a crash',async({page})=>{
 await page.route('**/api/v1/insights/**',r=>r.fulfill({json:[]}));
 await page.goto('/insights');await expect(page.getByText('unavailable',{exact:false}).first()).toBeVisible();
 await expect(page.getByRole('heading',{name:'Insights',level:1})).toBeVisible();
});

test('insights fits a phone',async({page})=>{
 await page.setViewportSize({width:390,height:844});await page.goto('/insights');
 await expect(page.getByRole('region',{name:'When customers act'})).toBeVisible();
 expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBeTruthy();
});
