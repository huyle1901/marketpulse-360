import {test,expect} from '@playwright/test';
test('unauthenticated workspace requires sign-in and handles rejected credentials',async({page})=>{
 await page.route('**/api/v1/**',r=>r.fulfill({status:401,json:{code:'unauthorized'}}));
 await page.goto('/');await expect(page.getByRole('heading',{name:'Sign in to MarketPulse 360'})).toBeVisible();
 await page.getByLabel('Username',{exact:true}).fill('viewer-test');await page.getByLabel('Password',{exact:true}).fill('incorrect password');await page.getByRole('button',{name:'Sign in',exact:true}).click();await expect(page.locator('main [role=alert]')).toContainText('unauthorized');
});
test('viewer customers never fetch profiles and cannot open operations',async({page})=>{
 let profiles=0;
 await page.route('**/api/v1/**',r=>{const p=new URL(r.request().url()).pathname;if(p.includes('profiles'))profiles++;return r.fulfill({json:p.endsWith('auth/session')?{user_id:'v',username:'viewer',role:'viewer'}:{rows:[],provenance:{identity:'a'.repeat(64)},totals:{profiles_total:'0'}}});});
 await page.goto('/customers');await expect(page.getByRole('heading',{name:'Cohort retention'})).toBeVisible();await expect(page.getByText('Profile access requires operator role')).toBeVisible();expect(profiles).toBe(0);
 await page.goto('/operations');await expect(page.getByRole('heading',{name:'Administrator access required'})).toBeVisible();
});
test('admin failed action stays authoritative and sign-out removes content',async({page})=>{
 let signedOut=false;
 await page.route('**/api/v1/**',r=>{const p=new URL(r.request().url()).pathname;
 if(p.endsWith('auth/logout')){signedOut=true;return r.fulfill({json:{status:'signed_out'}});}
 if(p.endsWith('auth/session'))return r.fulfill({status:signedOut?401:200,json:{user_id:'a',username:'admin',role:'admin'}});
 if(p.endsWith('quality/probe'))return r.fulfill({status:503,json:{code:'dependency_unavailable'}});
 return r.fulfill({json:p.endsWith('users')||p.endsWith('audit')||p.endsWith('quality')?[]:{status:'available',probe_active:false}});
 });
 await page.goto('/operations');await expect(page.getByRole('heading',{name:'Operations & trust'})).toBeVisible();await page.getByRole('button',{name:'Run isolated corruption probe'}).click();await expect(page.locator('main [role=alert]')).toContainText('dependency_unavailable');
 await page.getByRole('button',{name:'Sign out'}).click();await expect(page.getByRole('heading',{name:'Sign in to MarketPulse 360'})).toBeVisible();
});
test('an expired protected read removes the workspace immediately',async({page})=>{
 await page.route('**/api/v1/auth/session',r=>r.fulfill({json:{user_id:'a',username:'admin',role:'admin'}}));
 await page.route('**/api/v1/ops/**',r=>r.fulfill({status:401,json:{code:'unauthorized'}}));
 await page.goto('/operations');await expect(page.getByRole('heading',{name:'Sign in to MarketPulse 360'})).toBeVisible();await expect(page.getByRole('heading',{name:'Operations & trust'})).toHaveCount(0);
});
test('a delayed earlier operations refresh cannot replace the latest',async({page})=>{
 let count=0;let release!:()=>void;const pending=new Promise<void>(resolve=>{release=resolve;});
 await page.route('**/api/v1/auth/session',r=>r.fulfill({json:{user_id:'a',username:'admin',role:'admin'}}));
 await page.route('**/api/v1/ops/**',async r=>{const p=new URL(r.request().url()).pathname;if(p.endsWith('status')){count++;if(count===1){await pending;await r.fulfill({json:{status:'obsolete'}});}else await r.fulfill({json:{status:'current'}});}else await r.fulfill({json:p.endsWith('users')||p.endsWith('audit')||p.endsWith('quality')?[]:{}});});
 await page.goto('/operations');await page.getByRole('button',{name:'Refresh operations'}).click();await expect(page.getByText('"status": "current"',{exact:false})).toBeVisible();release();await expect(page.getByText('"status": "obsolete"',{exact:false})).toHaveCount(0);
});
