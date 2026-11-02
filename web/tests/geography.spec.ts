import {test,expect} from '@playwright/test';
import {expectReadableLegend} from './legend-marks';
import {placeCallouts} from '../lib/callouts';

const provenance={identity:'a'.repeat(64),generation:'1',proof_digest:'b'.repeat(64),model:'gold.customer_360 + customers',serving_relation:'customer_360',historical:true,currency:'VND'};
const city=(name:string,net:string,customers='10',orders='12')=>({country:'VN',city:name,customers,orders,net_revenue_minor:net,net_per_customer_minor:String(BigInt(net)/BigInt(customers))});
const geography={status:'available',currency:'VND',provenance,total_net_revenue_minor:'9000',definition:'Identified customers by their master-record city.',
 cities:[city('Ho Chi Minh City','4000','10','12'),city('Ha Noi','3000','30','40'),city('Hue','1500'),city('Can Tho','-500'),city('Lang Son','200')],unknown:{profiles:'40',orders:'7',net_revenue_minor:'1000'}};

test.beforeEach(async({page})=>{await page.route('**/api/v1/**',r=>{const p=new URL(r.request().url()).pathname;
 return r.fulfill({json:p.endsWith('auth/session')?{user_id:'a',username:'admin',role:'admin'}:p.endsWith('/insights/geography')?geography:[]});});});

test('geography plots positive known cities at their coordinates and ranks every city',async({page})=>{
 await page.goto('/geography');await expect(page.getByRole('navigation',{name:'Workspaces'}).getByRole('link',{name:'Geography'})).toHaveAttribute('aria-current','page');
 await expect(page.getByRole('heading',{name:'Geography',level:1})).toBeVisible();
 const map=page.getByRole('img',{name:'Customer net revenue by city'});
 await expect(map).toContainText('Ho Chi Minh City');await expect(map).toContainText('Ha Noi');
 await expect(map.locator('circle[data-city]')).toHaveCount(3);
 await expect(page.getByRole('list',{name:'Cities ranked by net revenue'}).getByRole('listitem').first()).toContainText('Ho Chi Minh City');
 await page.getByText('Exact city values').click();const table=page.getByRole('table',{name:'Net revenue by city'});
 await expect(table.getByRole('row',{name:/Ho Chi Minh City/})).toContainText('₫4,000');
 await expect(table.getByRole('row',{name:/Can Tho/})).toContainText('-₫500');
 await expect(table.getByRole('row',{name:/Hue/})).toBeVisible();
 await expect(page.getByText('Not on the map: Can Tho (net revenue not positive), Lang Son (no bundled coordinates)',{exact:false})).toBeVisible();
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

test('bubbles wear their region colour, matching the region legend, with a callout per city',async({page})=>{
 await page.goto('/geography');const map=page.getByRole('img',{name:'Customer net revenue by city'});
 const bubble=map.locator('circle[data-city="Ha Noi"]');await expect(bubble).toHaveAttribute('data-region','North');
 const stroke=await bubble.evaluate(e=>getComputedStyle(e).stroke);
 const north=await page.getByRole('list',{name:'Region legend'}).getByRole('listitem').filter({hasText:'North'}).locator('[aria-hidden="true"]').evaluate(e=>getComputedStyle(e).backgroundColor);
 expect(stroke).toBe(north);
 const callout=map.locator('[data-callout="Ha Noi"]');await expect(callout).toContainText('Ha Noi');await expect(callout).toContainText('₫3,000');
});

test('bubble motion stops when the reader asks for reduced motion',async({page})=>{
 await page.emulateMedia({reducedMotion:'reduce'});await page.goto('/geography');
 const animation=await page.locator('circle[data-city="Ha Noi"]').evaluate(e=>getComputedStyle(e).animationName);
 expect(animation).toBe('none');
});

test('each ranked city bar uses its region colour, like its bubble',async({page})=>{
 await page.goto('/geography');
 const bar=await page.getByRole('list',{name:'Cities ranked by net revenue'}).getByRole('listitem').filter({hasText:'Ha Noi'}).locator('div[style*="width"]').evaluate(e=>getComputedStyle(e).backgroundColor);
 const stroke=await page.locator('circle[data-city="Ha Noi"]').evaluate(e=>getComputedStyle(e).stroke);
 expect(bar).toBe(stroke);
});

const THIRTEEN=[['Ho Chi Minh City',9000],['Ha Noi',8000],['Da Nang',3000],['Hai Phong',2500],['Can Tho',2000],['Bien Hoa',1800],['Nha Trang',1500],['Hue',1200],['Vung Tau',1000],['Thu Dau Mot',900],['Quy Nhon',800],['Buon Ma Thuot',700],['Ha Long',600]] as const;
const thirteen={...geography,total_net_revenue_minor:String(THIRTEEN.reduce((s,[,v])=>s+v,0)),cities:THIRTEEN.map(([n,v])=>city(n,String(v)))};
const useThirteen=async(page:import('@playwright/test').Page)=>{await page.route('**/api/v1/insights/geography',r=>r.fulfill({json:thirteen}));};

test('all thirteen cities plot',async({page})=>{
 await useThirteen(page);await page.goto('/geography');
 await expect(page.locator('circle[data-city]')).toHaveCount(13);
});

test('six callouts by default, more on hover',async({page})=>{
 await useThirteen(page);await page.goto('/geography');
 await expect(page.locator('[data-callout]:visible')).toHaveCount(6);
 await expect(page.locator('[data-callout="Hue"]')).toBeHidden();
 await page.getByRole('list',{name:'Cities ranked by net revenue'}).getByRole('listitem').filter({hasText:'Hue'}).hover();
 await expect(page.locator('[data-callout="Hue"]')).toBeVisible();
});

const expectNoOverlap=async(page:import('@playwright/test').Page,label:string)=>{
 const boxes=await page.locator('[data-callout]:visible').evaluateAll(els=>els.map(e=>{const r=e.getBoundingClientRect();return {n:e.getAttribute('data-callout'),l:r.left,r:r.right,t:r.top,b:r.bottom};}));
 for(let i=0;i<boxes.length;i++)for(let j=i+1;j<boxes.length;j++){const a=boxes[i],b=boxes[j];
  expect(a.r<=b.l||b.r<=a.l||a.b<=b.t||b.b<=a.t,`${label}: ${a.n} overlaps ${b.n}`).toBeTruthy();}
 return boxes.map(b=>b.n);
};

test('default callouts never overlap',async({page})=>{
 await useThirteen(page);await page.setViewportSize({width:1440,height:900});await page.goto('/geography');
 await expect(page.locator('[data-callout]:visible')).toHaveCount(6);
 await expectNoOverlap(page,'net revenue');
});

/** Story weights (vietnam-retail); the eight heaviest cities are the ones that can reach a top six. */
const STORY_WEIGHTS:[string,number][]=[['Ho Chi Minh City',28],['Ha Noi',24],['Bien Hoa',6],['Thu Dau Mot',6],['Hai Phong',6],['Da Nang',6],['Can Tho',5],['Nha Trang',4],
 ['Hue',3],['Vung Tau',3],['Quy Nhon',3],['Buon Ma Thuot',3],['Ha Long',3]];
const TOP_EIGHT=STORY_WEIGHTS.slice(0,8).map(([n])=>n);
const sixOfEight:string[][]=[];
for(let i=0;i<8;i++)for(let j=i+1;j<8;j++)sixOfEight.push(TOP_EIGHT.filter((_,k)=>k!==i&&k!==j));
/** Seed 360's real Customers ranking, which the old offsets overlapped (Bien Hoa / Thu Dau Mot). */
const SEED_360=['Ho Chi Minh City','Ha Noi','Thu Dau Mot','Da Nang','Bien Hoa','Can Tho'];
const RANKINGS=[SEED_360,...sixOfEight];
/** Values that put `top` first, in that order, sized like the story's top six (28, 24, 6, 6, 6, 6); every other city trails. */
const RANK_WEIGHTS=[28,24,6,6,6,6];
const valuesFor=(top:string[])=>Object.fromEntries(STORY_WEIGHTS.map(([n,w])=>{const at=top.indexOf(n);return [n,at>=0?String(1000*RANK_WEIGHTS[at]-at):String(100*w)];}));
const MEASURE_BUTTONS=[['Net revenue','net revenue'],['Customers','customers'],['Orders','orders']] as const;

test('every likely story ranking shows six non-overlapping callouts',async({page})=>{
 test.setTimeout(120_000);
 await page.setViewportSize({width:1440,height:900});
 for(let g=0;g<RANKINGS.length;g+=3){
  const group=[0,1,2].map(m=>RANKINGS[Math.min(g+m,RANKINGS.length-1)]);
  const [net,customers,orders]=group.map(valuesFor);
  const fixture={...geography,total_net_revenue_minor:String(Object.values(net).reduce((s,v)=>s+Number(v),0)),cities:STORY_WEIGHTS.map(([n])=>city(n,net[n],customers[n],orders[n]))};
  await page.unroute('**/api/v1/insights/geography');await page.route('**/api/v1/insights/geography',r=>r.fulfill({json:fixture}));
  await page.goto('/geography');
  for(let m=0;m<3;m++){const [button,measure]=MEASURE_BUTTONS[m];
   await page.getByRole('group',{name:'Map measure'}).getByRole('button',{name:button}).click();
   await expect(page.getByRole('list',{name:`Cities ranked by ${measure}`}).getByRole('listitem').first()).toContainText(group[m][0]);
   await expect(page.locator('[data-callout]:visible')).toHaveCount(6);
   const shown=await expectNoOverlap(page,group[m].join(', '));
   expect(new Set(shown),group[m].join(', ')).toEqual(new Set(group[m]));}
 }
});

test('a lower-ranked callout that would collide is skipped and the next city takes its place',()=>{
 const box=(l:number,t:number)=>({l,r:l+150,t,b:t+44});
 const placed=placeCallouts([{city:'A',box:box(0,0)},{city:'B',box:box(100,20)},{city:'C',box:box(0,44)},{city:'D',box:box(300,0)},{city:'E',box:box(300,100)}],3);
 expect([...placed]).toEqual(['A','C','D']);
});

test('focusing a hidden city in the ranking reveals its callout',async({page})=>{
 await useThirteen(page);await page.goto('/geography');
 const callout=page.locator('[data-callout="Hue"]');await expect(callout).toBeHidden();
 const rows=page.getByRole('list',{name:'Cities ranked by net revenue'}).getByRole('listitem');
 await rows.filter({hasText:'Hue'}).focus();
 await expect(callout).toBeVisible();
 await rows.filter({hasText:'Ha Noi'}).focus();
 await expect(callout).toBeHidden();
});

test('every city belongs to a region',async({page})=>{
 await useThirteen(page);await page.goto('/geography');
 const total=THIRTEEN.reduce((s,[,v])=>s+v,0);
 const regions=page.getByRole('region',{name:'Net revenue by region'});
 const text=await regions.getByRole('list',{name:'Region legend'}).innerText();
 expect(text).not.toContain('Other');
 const pct=[...text.matchAll(/([\d.]+)%/g)].map(m=>Number(m[1])).reduce((a,b)=>a+b,0);
 expect(Math.abs(pct-100)).toBeLessThan(0.5);expect(total).toBeGreaterThan(0);
});
