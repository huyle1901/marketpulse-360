import {test,expect,type Locator,type Page} from '@playwright/test';
import {expectReadableLegend} from './legend-marks';

const provenance={generation:'1',identity:'a'.repeat(64),model:'gold.campaign_performance_daily',data_through:'2026-01-03T00:00:00Z'};
const row=(id:string,name:string,platform:string|null,spend:string,net:string,orders:string)=>({campaign_id:id,campaign_name:name,currency:'VND',platform,objective:platform?'traffic':null,spend_minor:spend,net_revenue_minor:net,orders,clicks:'10',impressions:'100',gross_revenue_minor:net,source_roas:null});
const all=[row('g','Google Traffic','google','200','500','4'),row('m','Meta Traffic','meta','100','200','2'),row('t','TikTok Traffic','tiktok','80','150','2'),row('s','Shopee Traffic','shopee','60','240','3'),row('u','Unattributed',null,'0','90','1')];
const dashboardFor=(platform:string|null)=>({filters:{from_date:'2026-01-01',to_date:'2026-01-03'},campaigns:[],certified:{rows:all.filter(r=>!platform||r.platform===platform),totals:[{currency:'VND',spend_minor:'440',net_revenue_minor:'1180',gross_revenue_minor:'1180',orders:'12',clicks:'40',impressions:'400'}],daily_series:[],provenance,freshness:{historical:true,stale:true}},streaming:{status:'unavailable',source:'x',rows:[]}});

const counts={sessions:'2',page_view_sessions:'2',view_item_sessions:'1',add_to_cart_sessions:'1',checkout_sessions:'1',purchase_sessions:'1'};
const funnel={totals:counts,reach_ratios:{page_view_sessions:'1.000000',view_item_sessions:'0.500000',add_to_cart_sessions:'0.500000',checkout_sessions:'0.500000',purchase_sessions:'0.500000'},unit:'campaign-day session incidences',definition:'Stage reach.',filters:{from_date:'2026-01-01',to_date:'2026-01-03'},
 provenance:{identity:'a'.repeat(64),generation:'1',proof_digest:'b'.repeat(64),historical:true,data_through:'2026-01-03T00:00:00Z',model:'gold.funnel_daily',timestamp_resolution:'day'},
 campaign_options:[],channels:['google','meta','tiktok','shopee'].map(channel=>({channel,...counts})),campaigns:[]};

const money={gross_revenue_minor:'0',refund_amount_minor:'0',net_revenue_minor:'0',allocated_order_count:'0.000000'};
const attribution={provenance:funnel.provenance,currency:'VND',definitions:{},models:[],channels:[],campaigns:[],global_reconciliation:{balanced:true,scope:'All campaigns',source_totals:money,model_totals:[]}};
const journeys={provenance:funnel.provenance,status:'available',model:'last_touch',currency:'VND',representative:true,candidates_scanned:'0',context_unavailable:'0',candidate_scan_capped:false,sample_limit:'5',rows:[]};

async function setup(page:Page,theme:'light'|'dark'){
 await page.addInitScript(t=>localStorage.setItem('mp-theme',t),theme);
 await page.route('**/api/v1/**',r=>{const url=new URL(r.request().url()),p=url.pathname;
  return r.fulfill({json:p.endsWith('auth/session')?{user_id:'a',username:'admin',role:'admin'}:p.endsWith('dashboard')?dashboardFor(url.searchParams.get('platform')):p.endsWith('analysis/funnel')?funnel:p.endsWith('analysis/attribution')?attribution:p.includes('/analysis/')?journeys:[]});});
}
/** The computed colour a `var(--channel-key)` token resolves to in the page's current theme. */
const token=(page:Page,name:string)=>page.evaluate(n=>{const probe=document.createElement('span');probe.style.background=`var(--channel-${n})`;document.body.append(probe);const c=getComputedStyle(probe).backgroundColor;probe.remove();return c;},name);
const markOf=(legend:Locator,label:string)=>legend.getByRole('listitem').filter({hasText:label}).locator('[aria-hidden="true"]').first().evaluate(m=>getComputedStyle(m).backgroundColor);

for(const theme of ['light','dark'] as const){
 test(`channel tokens are defined and distinct in ${theme} mode`,async({page})=>{
  await setup(page,theme);await page.goto('/');
  const colours=await Promise.all(['google','meta','tiktok','shopee','unattributed'].map(k=>token(page,k)));
  for(const c of colours)expect(c).toMatch(/^rgb/);
  expect(new Set(colours).size).toBe(5);
 });
 test(`overview donut legend names Shopee with its own colour in ${theme} mode`,async({page})=>{
  await setup(page,theme);await page.goto('/');
  const legend=page.getByRole('list',{name:'Net revenue by channel · VND legend',exact:true});
  await expectReadableLegend(legend);
  await expect(legend.getByRole('listitem').filter({hasText:'Shopee'})).toBeVisible();
  for(const [key,label] of [['google','Google'],['meta','Meta'],['tiktok','TikTok'],['shopee','Shopee'],['unattributed','Unattributed']])expect(await markOf(legend,label)).toBe(await token(page,key));
 });
 test(`ads small multiples, share bars and scatter legend carry Shopee in ${theme} mode`,async({page})=>{
  await setup(page,theme);await page.goto('/ads');
  const legend=page.getByRole('list',{name:'Channel legend',exact:true});
  await expectReadableLegend(legend);await expect(legend.getByRole('listitem')).toHaveText(['Google','Meta','Shopee','TikTok']);
  const scatter=page.getByRole('list',{name:'Campaign channel legend',exact:true});
  await expectReadableLegend(scatter);expect(await markOf(scatter,'Shopee')).toBe(await token(page,'shopee'));
  const bar=page.getByRole('list',{name:'Net ROAS by channel · VND'}).getByRole('listitem').filter({hasText:'Shopee'}).locator('div[style*="width"]');
  expect(await bar.evaluate(b=>getComputedStyle(b).backgroundColor)).toBe(await token(page,'shopee'));
  await expect(page.getByRole('img',{name:/^Shopee: /})).toBeVisible();
 });
}

test('a channel keeps its colour when another channel is filtered out',async({page})=>{
 await setup(page,'light');await page.goto('/ads');
 const legend=page.getByRole('list',{name:'Channel legend',exact:true});
 const before=await markOf(legend,'Shopee');
 await page.getByLabel('Platform').selectOption('shopee');
 await expect(legend.getByRole('listitem')).toHaveText(['Shopee']);
 expect(await markOf(legend,'Shopee')).toBe(before);expect(before).toBe(await token(page,'shopee'));
 await page.getByLabel('Platform').selectOption('tiktok');
 await expect(legend.getByRole('listitem')).toHaveText(['TikTok']);
 expect(await markOf(legend,'TikTok')).toBe(await token(page,'tiktok'));
});

test('overview platform filter and analysis channel select offer Shopee',async({page})=>{
 await setup(page,'light');await page.goto('/');
 await expect(page.getByLabel('Platform').locator('option')).toHaveText(['All platforms','Google','Meta','TikTok','Shopee']);
 await page.goto('/analysis');
 await expect(page.getByLabel('Paid channel').locator('option')).toHaveText(['All channels','Google','Meta','TikTok','Shopee']);
 const bars=page.getByRole('list',{name:'Sessions by channel'});
 await expect(bars.getByRole('button',{name:'Explore channel shopee'})).toHaveText('Shopee');
 const fill=bars.getByRole('listitem').filter({hasText:'Shopee'}).locator('div[style*="width"]');
 expect(await fill.evaluate(b=>getComputedStyle(b).backgroundColor)).toBe(await token(page,'shopee'));
});
