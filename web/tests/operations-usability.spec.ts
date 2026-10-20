import {test,expect} from '@playwright/test';
const now=new Date().toISOString();
const live={report_id:'l',scope:'live',status:'passed',observed_at:now,identity:'a'.repeat(64),reason:'certified_rowsets_and_source_finance',relations:[{relation:'gold.campaign_performance_daily',row_count:'12',checksum:'b'.repeat(64)}]};
const status={status:'available',telemetry:{status:'available',sources:[{name:'mp_source_up',labels:{source:'kafka'},value:'1'},{name:'mp_source_observed_timestamp_seconds',labels:{source:'kafka'},value:String(Date.now()/1000)},{name:'mp_source_up',labels:{source:'flink'},value:'0'},{name:'mp_source_observed_timestamp_seconds',labels:{source:'flink'},value:String(Date.now()/1000)}]},quality:live,probe_active:true};
test.beforeEach(async({page})=>{await page.route('**/api/v1/**',r=>{const path=new URL(r.request().url()).pathname;const data=path.endsWith('auth/session')?{user_id:'a',username:'admin',role:'admin'}:path.endsWith('/status')?status:path.endsWith('/quality')?[{report_id:'p',scope:'probe',status:'failed',observed_at:now},live]:path.endsWith('/lineage')?{identity:'a'.repeat(64),proof_digest:'b'.repeat(64),source_snapshots:{events:'1'},output_snapshots:{gold:'2'},projection:{row_count:'73',snapshot_id:'42'}}:path.endsWith('/retention')?{session_ttl_hours:'8',expired_sessions:'2',cleanup_limit:'1000',protected_releases:'3',protected_sidecars:'1',analytical_deletion_enabled:false}:[];return r.fulfill({json:data});});});
test('operations shows readable source health, live quality and protected lineage',async({page})=>{
 await page.goto('/operations');
 await expect(page.getByRole('article',{name:'Kafka status'})).toContainText('Available');
 await expect(page.getByRole('article',{name:'Flink status'})).toContainText('Unavailable');
 await expect(page.getByRole('article',{name:'ClickHouse status'})).toContainText('Unknown');
 const quality=page.getByRole('region',{name:'Latest live quality'});await expect(quality).toContainText('Passed');await expect(quality).not.toContainText('failed');
 await expect(page.getByText('Controlled probe alert active',{exact:true})).toBeVisible();
 await expect(page.getByText('73 governed projection events',{exact:true})).toBeVisible();
 await expect(page.getByText('2 expired sessions',{exact:true})).toBeVisible();
 await expect(page.locator('pre:visible')).toHaveCount(0);await page.getByText('Technical platform evidence',{exact:true}).click();await expect(page.locator('pre:visible')).toHaveCount(1);
});
test('missing telemetry and failed lineage remain explicit unknown states',async({page})=>{
 await page.route('**/api/v1/ops/status',r=>r.fulfill({json:{status:'available',telemetry:{status:'unavailable',sources:[]},quality:null,probe_active:false}}));
 await page.route('**/api/v1/ops/lineage',r=>r.fulfill({status:503,json:{code:'dependency_unavailable'}}));
 await page.goto('/operations');await expect(page.getByRole('article',{name:'Kafka status'})).toContainText('Unknown');
 await expect(page.getByRole('region',{name:'Latest live quality'})).toContainText('Unavailable');
 await expect(page.getByText('Validated lineage unavailable',{exact:true})).toBeVisible();
 await expect(page.locator('main [role=alert]')).toContainText('lineage');
 await page.setViewportSize({width:390,height:844});expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBeTruthy();
});
