import {expect,type Locator} from '@playwright/test';

/** Every colour mark in a legend list must be visible against its card and distinct from its siblings. */
export async function expectReadableLegend(legend:Locator){
 await expect(legend.getByRole('listitem').first()).toBeVisible();
 const colors=await legend.getByRole('listitem').evaluateAll(items=>items.map(item=>{
  const mark=item.querySelector('[aria-hidden="true"]') as HTMLElement|null;
  let card=item.parentElement;while(card&&getComputedStyle(card).backgroundColor==='rgba(0, 0, 0, 0)')card=card.parentElement;
  return {mark:mark?getComputedStyle(mark).backgroundColor:'none',card:card?getComputedStyle(card).backgroundColor:'none'};
 }));
 expect(colors.length).toBeGreaterThan(0);
 // A mark close to the card colour reads as missing; compare in sRGB with a generous threshold.
 const rgb=(c:string)=>(c.match(/[\d.]+/g)||[]).slice(0,3).map(Number);
 for(const {mark,card} of colors){
  const [a,b]=[rgb(mark),rgb(card)];
  expect(Math.hypot(a[0]-b[0],a[1]-b[1],a[2]-b[2]),`legend mark ${mark} on card ${card}`).toBeGreaterThan(60);
 }
 expect(new Set(colors.map(c=>c.mark)).size,'legend marks must be distinct: '+colors.map(c=>c.mark).join(' / ')).toBe(colors.length);
}
