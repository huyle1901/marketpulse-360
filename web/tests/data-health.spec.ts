import {test,expect} from '@playwright/test';
const now=new Date().toISOString();
const live={report_id:'l',scope:'live',status:'passed',observed_at:now,identity:'a'.repeat(64),reason:'certified_rowsets_and_source_finance',relations:[{relation:'gold.campaign_performance_daily',row_count:'12',checksum:'b'.repeat(64)}]};
const status={status:'available',telemetry:{status:'available',sources:[{name:'mp_source_up',labels:{source:'kafka'},value:'1'},{name:'mp_source_observed_timestamp_seconds',labels:{source:'kafka'},value:String(Date.now()/1000)},{name:'mp_source_up',labels:{source:'flink'},value:'0'},{name:'mp_source_observed_timestamp_seconds',labels:{source:'flink'},value:String(Date.now()/1000)}]},quality:live,probe_active:true};
const dashboard={filters:{from_date:'2026-01-01',to_date:'2026-01-03'},campaigns:[],certified:{rows:[],totals:[],provenance:{generation:'4',identity:'a'.repeat(64),model:'gold.campaign_performance_daily',data_through:'2026-01-03T00:00:00Z'},freshness:{historical:true,stale:true}},streaming:{status:'available',source:'marketing.gold.campaign-metrics.v1',rows:[]}};
test.beforeEach(async({page})=>{await page.route('**/api/v1/**',r=>{const path=new URL(r.request().url()).pathname;const data=path.endsWith('auth/session')?{user_id:'a',username:'admin',role:'admin'}:path.endsWith('/status')?status:path.endsWith('/quality')?[{report_id:'p',scope:'probe',status:'failed',observed_at:now},live]:path.endsWith('/lineage')?{identity:'a'.repeat(64),proof_digest:'b'.repeat(64),source_snapshots:{events:'1'},output_snapshots:{gold:'2'},projection:{row_count:'73',snapshot_id:'42'}}:path.endsWith('/dashboard')?dashboard:path.endsWith('/retention')?{session_ttl_hours:'8',expired_sessions:'2',cleanup_limit:'1000',protected_releases:'3',protected_sidecars:'1',analytical_deletion_enabled:false}:[];return r.fulfill({json:data});});});

test('data health leads with a compact status strip naming failing stages',async({page})=>{
 await page.goto('/operations');await expect(page.getByRole('heading',{name:'Data health',level:1})).toBeVisible();
 const strip=page.getByRole('region',{name:'Data health summary'});
 const tile=(name:string)=>strip.getByRole('article',{name});
 await expect(tile('Certified data')).toContainText('Through 2026-01-03');await expect(tile('Certified data')).toContainText('Generation 4');
 await expect(tile('Live quality')).toContainText('Passed');
 await expect(tile('Platform sources')).toContainText('1 available · 1 unavailable · 7 unknown');
 await expect(tile('Platform sources')).toContainText('Needs attention: Flink');
 await expect(tile('Platform sources')).toContainText('1 source needs attention');
 await expect(tile('Input & replay')).toContainText('Kafka available');await expect(tile('Input & replay')).toContainText('Replay controller status unavailable');
});
test('status strip quality ages locally and a release mismatch is not certified',async({page})=>{
 await page.clock.install();await page.goto('/operations');const tile=page.getByRole('region',{name:'Data health summary'}).getByRole('article',{name:'Live quality'});
 await expect(tile).toContainText('Passed');await page.clock.fastForward(181000);await expect(tile).toContainText('Unknown');
 await page.route('**/api/v1/dashboard*',r=>r.fulfill({json:{...dashboard,certified:{...dashboard.certified,provenance:{...dashboard.certified.provenance,identity:'f'.repeat(64)}}}}));
 await page.getByRole('button',{name:'Refresh operations'}).click();
 await expect(page.getByRole('region',{name:'Data health summary'}).getByRole('article',{name:'Certified data'})).toContainText('does not match validated lineage');
});
test('viewers cannot open data health',async({page})=>{
 await page.route('**/api/v1/auth/session',r=>r.fulfill({json:{user_id:'v',username:'viewer',role:'viewer'}}));
 await page.goto('/operations');await expect(page.getByRole('heading',{name:'Administrator access required'})).toBeVisible();
 await expect(page.getByRole('link',{name:'Data health',exact:true})).toHaveCount(0);
 await expect(page.getByRole('region',{name:'Data health summary'})).toHaveCount(0);
});
test('platform status shows source availability as a labelled meter with a marked legend',async({page})=>{
 await page.goto('/operations');
 await expect(page.getByRole('img',{name:'Source availability: 1 available, 1 unavailable or stale, 7 unknown of 9 sources'})).toBeVisible();
 const legend=page.getByRole('list',{name:'Source availability legend'});
 await expect(legend.getByRole('listitem')).toHaveCount(3);await expect(legend).toContainText('Unavailable / stale');
});
