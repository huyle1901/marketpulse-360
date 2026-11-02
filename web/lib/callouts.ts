/** A callout's box in SVG user units. */
export type Box={l:number;r:number;t:number;b:number};

/** Whether two boxes share any area (touching edges do not collide). */
export const collide=(a:Box,b:Box)=>a.l<b.r&&b.l<a.r&&a.t<b.b&&b.t<a.b;

/**
 * Greedy label placement (as Datawrapper hides colliding labels): walk cities in rank order, keep a callout only
 * if its box clears every callout already kept, and stop once `limit` are kept. A skipped city still shows on hover or focus.
 */
export function placeCallouts(ranked:{city:string;box:Box}[],limit:number):Set<string>{
 const kept:Box[]=[],cities=new Set<string>();
 for(const {city,box} of ranked){if(cities.size>=limit)break;if(kept.some(k=>collide(k,box)))continue;kept.push(box);cities.add(city);}
 return cities;
}
