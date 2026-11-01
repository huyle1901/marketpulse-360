import {expect,type Page} from '@playwright/test';

/** Visible text never shows machine timestamps (ISO with a time, offsets, fractional seconds). */
export async function expectNoMachineTimes(page:Page){
 const text=await page.locator('main').innerText();
 expect(text.match(/\d{4}-\d{2}-\d{2}T\d{2}:\d{2}[^\s]*/g)??[]).toEqual([]);
}
