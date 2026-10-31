import {test,expect} from '@playwright/test';

const anchor='2026-09-03T20:15:30+00:00';
const minute=(i:number)=>new Date(Date.parse('2026-09-03T19:46:00Z')+i*60000).toISOString().replace('.000Z','+00:00');
const view={status:'available',anchor,window_minutes:'30',
 active:{visitors:'3',by_source:[{source:'mobile',visitors:'1'},{source:'web',visitors:'2'}],by_device:[{device:'desktop',visitors:'1'},{device:'mobile',visitors:'2'},{device:'tablet',visitors:'0'},{device:'unknown',visitors:'0'}]},
 behaviour:[{stage:'browsing',sessions:'1'},{stage:'viewing_product',sessions:'0'},{stage:'cart',sessions:'2'},{stage:'checkout',sessions:'0'},{stage:'purchased',sessions:'1'}],
 today:{visitors:'20',sessions:'25',purchased_sessions:'3',conversion:'0.120000',money:[{currency:'VND',orders:'3',gross_minor:'900000',refunds_minor:'100000',net_minor:'800000'}]},
 series:Array.from({length:30},(_,i)=>({minute:minute(i),visitors:i===29?'2':'0',orders:i===29?'1':'0'})),
 top_products:[{label:'Linen jacket',category:'fashion',units:'4',carts:'2',views:'9'},{label:'Unlisted product',category:null,units:'0',carts:'1',views:'3'}],
 activity:[{event_time:anchor,kind:'order_placed',source:'commerce',device:'unknown',label:'Linen jacket',amount_minor:'300000',currency:'VND'},
  {event_time:'2026-09-03T20:14:30+00:00',kind:'added_to_cart',source:'mobile',device:'mobile',label:'Unlisted product',amount_minor:null,currency:null}],
 freshness:{anchor,last_received_at:'2026-10-31T09:00:00+00:00',seconds_since_receipt:'12',receiving:true},
 provenance:{source:'marketing.clean.customer-events.v1',source_identity:'cluster/topic',rows:'40',offset_bounds:[{partition:'0',from_offset:'0',to_offset:'40'}],note:'Provisional · not certified'}};
const dashboard={filters:{from_date:'2026-09-01',to_date:'2026-09-05'},campaigns:[],certified:{rows:[],totals:[],provenance:{data_through:'2026-09-01T00:00:00Z'},freshness:{historical:true,stale:true}},
 streaming:{status:'empty',source:'marketing.gold.campaign-metrics.v1',rows:[]}};

const serve=(body:object)=>async({page}:{page:import('@playwright/test').Page})=>{await page.route('**/api/v1/**',r=>{const p=new URL(r.request().url()).pathname;
 return r.fulfill({json:p.endsWith('auth/session')?{user_id:'a',username:'admin',role:'admin'}:p.endsWith('/live/view')?body:p.endsWith('/dashboard')?dashboard:[]});});};

test.describe('available',()=>{
 test.beforeEach(serve(view));
 test('live view shows who is on the store and what they are buying, all provisional',async({page})=>{
  await page.goto('/live');
  await expect(page.getByRole('heading',{name:'Live View',level:1})).toBeVisible();
  const now=page.getByRole('article',{name:'Visitors right now'});
  await expect(now).toContainText('3');await expect(now).toContainText('Web 2');await expect(now).toContainText('App 1');
  await expect(page.getByRole('article',{name:'Net sales today'})).toContainText('₫800,000');
  await expect(page.getByRole('article',{name:'Orders today'})).toContainText('3');
  await expect(page.getByRole('article',{name:'Conversion today'})).toContainText('12.0%');
  const behaviour=page.getByRole('list',{name:'Shopper behaviour'});
  await expect(behaviour.getByRole('listitem')).toHaveText([/Browsing\s*1/,/Viewing a product\s*0/,/Holding a cart\s*2/,/Checking out\s*0/,/Purchased\s*1/]);
  await expect(page.getByRole('img',{name:'Visitors and orders per minute, last 30 minutes'})).toBeVisible();
  const products=page.getByRole('list',{name:'Top products today'});
  await expect(products.getByRole('listitem').first()).toContainText('Linen jacket');await expect(products.getByRole('listitem').first()).toContainText('4 ordered');
  await expect(products).toContainText('Unlisted product');
  await expect(page.getByRole('region',{name:'Shop floor'}).locator('.live-dot')).toHaveCount(4);
  const feed=page.getByRole('list',{name:'Recent activity'});
  await expect(feed.getByRole('listitem').first()).toContainText('Order placed');await expect(feed.getByRole('listitem').first()).toContainText('₫300,000');
  await expect(feed.getByRole('listitem').nth(1)).toContainText('Added to cart');
  await expect(page.getByText('Provisional · not certified').first()).toBeVisible();
  await expect(page.getByText('Replay time',{exact:false})).toContainText('20:15');await expect(page.getByText('Replay time',{exact:false})).toContainText('Sep 3, 2026');
  await expect(page.getByRole('heading',{name:'By campaign (provisional)'})).toBeVisible();
 });
 test('live view fits a phone',async({page})=>{
  await page.setViewportSize({width:390,height:844});await page.goto('/live');
  await expect(page.getByRole('article',{name:'Visitors right now'})).toBeVisible();
  expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBeTruthy();
 });
});

test.describe('empty',()=>{
 test.beforeEach(serve({status:'empty',message:'No live event has been projected yet; start the live events worker and the replay.'}));
 test('an empty projection says so instead of showing zeros',async({page})=>{
  await page.goto('/live');
  await expect(page.getByText('No live event has been projected yet',{exact:false})).toBeVisible();
  await expect(page.getByRole('article',{name:'Visitors right now'})).toHaveCount(0);
 });
});
