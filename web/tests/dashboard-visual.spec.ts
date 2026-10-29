import {test,expect} from '@playwright/test';

const provenance={generation:'1',identity:'a'.repeat(64),model:'gold.campaign_performance_daily',data_through:'2026-01-03T00:00:00Z'};
const rows=[
 {campaign_id:'g',campaign_name:'Google Traffic 1',currency:'VND',platform:'google',objective:'traffic',spend_minor:'100',net_revenue_minor:'300',orders:'2',source_roas:'3'},
 {campaign_id:'m',campaign_name:'Meta Traffic 2',currency:'VND',platform:'meta',objective:'traffic',spend_minor:'200',net_revenue_minor:'400',orders:'3',source_roas:'2'},
];
const dashboard={filters:{from_date:'2026-01-01',to_date:'2026-01-03'},campaigns:rows.map(r=>({campaign_id:r.campaign_id,name:r.campaign_name,currency:r.currency,platform:r.platform,objective:r.objective})),certified:{rows,totals:[{currency:'VND',spend_minor:'300',net_revenue_minor:'700',orders:'5',clicks:'20',impressions:'100'}],daily_series:[{event_date:'2026-01-01',currency:'VND',spend_minor:'100',net_revenue_minor:'300'},{event_date:'2026-01-02',currency:'VND',spend_minor:'200',net_revenue_minor:'400'}],comparison:{status:'unavailable',current_status:'complete',previous_status:'unavailable',from_date:'2025-12-30',to_date:'2026-01-01',observed_days:'0',expected_days:'2',current_observed_days:'2',current_expected_days:'2',totals:[]},provenance,freshness:{historical:true,stale:true}},streaming:{status:'unavailable',source:'marketing.gold.campaign-metrics.v1',rows:[]}};

test.beforeEach(async({page})=>{await page.route('**/api/v1/**',r=>{const p=new URL(r.request().url()).pathname;return r.fulfill({json:p.endsWith('auth/session')?{user_id:'a',username:'admin',role:'admin'}:p.endsWith('dashboard')?dashboard:[]});});});

test('visual overview shows net ROAS and switches a one-unit trend',async({page})=>{
 await page.goto('/');
 await expect(page.getByText('Net ROAS · VND',{exact:true})).toBeVisible();
 await expect(page.getByText('2.33×',{exact:true})).toBeVisible();
 const chart=page.getByRole('img',{name:'Daily net revenue in VND'});
 await expect(chart).toBeVisible();
 await page.getByRole('button',{name:'Show spend trend'}).click();
 await expect(page.getByText('Ad spend trend · VND',{exact:true})).toBeVisible();
 await expect(page.getByRole('img',{name:'Daily ad spend in VND'})).toBeVisible();
 await page.getByText('Exact daily values · VND',{exact:true}).click();
 await expect(page.getByRole('table',{name:'Daily certified values · VND'})).toContainText('400');
});

test('channel comparison is backed by certified campaign rows',async({page})=>{
 await page.goto('/');const region=page.getByRole('region',{name:'Channel comparison'});
 await expect(region).toContainText('Google');await expect(region).toContainText('Meta');
 await expect(region).toContainText('VND');
 await expect(region).toContainText('300');await expect(region).toContainText('400');
});

const longRange=(series:object[])=>({...dashboard,filters:{from_date:'2025-01-01',to_date:'2026-09-01'},certified:{...dashboard.certified,daily_series:series}});
const point=(event_date:string,net:string,spend:string,orders:string)=>({event_date,currency:'VND',net_revenue_minor:net,spend_minor:spend,orders});

test('orders trend uses its own count unit',async({page})=>{
 await page.route('**/api/v1/dashboard*',r=>r.fulfill({json:{...dashboard,certified:{...dashboard.certified,daily_series:[point('2026-01-01','300','100','2'),point('2026-01-02','400','200','3')]}}}));
 await page.goto('/');await page.getByRole('button',{name:'Show orders trend'}).click();
 await expect(page.getByRole('img',{name:'Daily orders for VND campaigns'})).toBeVisible();
 await page.getByText('Exact daily values · VND',{exact:true}).click();
 await expect(page.getByRole('table',{name:'Daily certified values · VND'})).toContainText('Orders');
});

test('long ranges default to monthly buckets summed from observed days with gaps kept',async({page})=>{
 await page.route('**/api/v1/dashboard*',r=>r.fulfill({json:longRange([point('2025-01-02','9007199254740993','100','1'),point('2025-01-30','-3','200','2'),point('2025-03-05','10','5','1')])}));
 await page.goto('/?from_date=2025-01-01&to_date=2026-09-01');
 await expect(page.getByRole('button',{name:'Monthly',exact:true})).toHaveAttribute('aria-pressed','true');
 const chart=page.getByRole('img',{name:'Monthly net revenue in VND'});await expect(chart).toBeVisible();
 await expect(page.locator('figure.trend')).toHaveAttribute('data-segments','2');
 await page.getByText('Exact monthly values · VND',{exact:true}).click();
 const table=page.getByRole('table',{name:'Monthly certified values · VND'});
 await expect(table.locator('tbody tr')).toHaveCount(2);
 await expect(table.locator('tbody tr').first()).toContainText('2025-01');await expect(table.locator('tbody tr').first()).toContainText('9007199254740990 / 300');
 await expect(table.locator('tbody tr').first()).toContainText('2 observed days');
});

test('weekly buckets start on UTC Monday and connect only adjacent weeks',async({page})=>{
 await page.route('**/api/v1/dashboard*',r=>r.fulfill({json:{...dashboard,filters:{from_date:'2026-01-01',to_date:'2026-02-01'},certified:{...dashboard.certified,daily_series:[point('2026-01-01','1','1','1'),point('2026-01-05','2','1','1'),point('2026-01-11','3','1','1'),point('2026-01-26','4','1','1')]}}}));
 await page.goto('/');await page.getByRole('button',{name:'Weekly',exact:true}).click();
 const chart=page.getByRole('img',{name:'Weekly net revenue in VND'});
 await expect(page.locator('figure.trend')).toHaveAttribute('data-segments','2');
 await page.getByText('Exact weekly values · VND',{exact:true}).click();
 const table=page.getByRole('table',{name:'Weekly certified values · VND'});
 await expect(table.locator('tbody tr')).toHaveCount(3);
 await expect(table.locator('tbody tr').nth(1)).toContainText('Week of 2026-01-05');await expect(table.locator('tbody tr').nth(1)).toContainText('5 / 2');
});

const counts={sessions:'100',page_view_sessions:'90',view_item_sessions:'40',add_to_cart_sessions:'20',checkout_sessions:'12',purchase_sessions:'7'};
const funnel=(identity:string)=>({totals:counts,reach_ratios:{},unit:'campaign-day session incidences',definition:'Stage reach',channels:[],campaigns:[],campaign_options:[],filters:{from_date:'2026-01-01',to_date:'2026-01-03'},provenance:{...provenance,identity}});

test('stage reach summarizes certified sessions on one linear scale with honest units',async({page})=>{
 const requests:URL[]=[];page.on('request',r=>{if(r.url().includes('/analysis/funnel'))requests.push(new URL(r.url()));});
 await page.route('**/api/v1/analysis/funnel*',r=>r.fulfill({json:funnel('a'.repeat(64))}));
 await page.goto('/?platform=google');const region=page.getByRole('region',{name:'Stage reach'});
 await expect(region).toContainText('Sessions');await expect(region).toContainText('100');
 await expect(region).toContainText('Purchase');await expect(region).toContainText('7');
 await expect(region).toContainText('campaign-day session incidences');await expect(region).toContainText('not a sequential funnel');
 await expect(region.getByRole('link',{name:'Open journey analysis'})).toHaveAttribute('href',/platform=google/);
 expect(requests.some(u=>u.searchParams.get('platform')==='google'&&u.searchParams.get('from_date')==='2026-01-01')).toBeTruthy();
});

test('stage reach from another release or a failed read is not drawn',async({page})=>{
 await page.route('**/api/v1/analysis/funnel*',r=>r.fulfill({json:funnel('b'.repeat(64))}));
 await page.goto('/');const region=page.getByRole('region',{name:'Stage reach'});
 await expect(region).toContainText('different certified release');await expect(region).not.toContainText('100');
 await page.route('**/api/v1/analysis/funnel*',r=>r.fulfill({status:503,json:{code:'dependency_unavailable'}}));
 await page.reload();await expect(page.getByRole('region',{name:'Stage reach'})).toContainText('Stage reach unavailable');
 await expect(page.getByRole('region',{name:'Channel comparison'})).toContainText('Google');
});

test('an unattributed campaign row with no platform renders as its own channel',async({page})=>{
 const unattributed={campaign_id:'__unattributed__',campaign_name:'Unattributed',currency:'VND',platform:null,objective:null,spend_minor:'0',net_revenue_minor:'-7',orders:'1',source_roas:null};
 await page.route('**/api/v1/dashboard*',r=>r.fulfill({json:{...dashboard,certified:{...dashboard.certified,rows:[...rows,unattributed]}}}));
 await page.goto('/');const region=page.getByRole('region',{name:'Channel comparison'});
 await expect(region).toContainText('Unattributed');await expect(region).toContainText('-₫7');
 await expect(page.getByRole('table',{name:'Campaign performance'})).toContainText('Unattributed');
});
