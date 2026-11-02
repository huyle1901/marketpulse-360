// Regenerates lib/vietnam.json: Vietnam's outline from Natural Earth 1:50m (public domain),
// redistributed by the world-atlas package. Run: node scripts/extract-vietnam.mjs
import {readFileSync,writeFileSync} from 'node:fs';
import {createRequire} from 'node:module';
import {feature} from 'topojson-client';

const require=createRequire(import.meta.url);
const topology=JSON.parse(readFileSync(require.resolve('world-atlas/countries-50m.json'),'utf8'));
const vietnam=feature(topology,topology.objects.countries).features.find(f=>f.id==='704');
if(!vietnam)throw new Error('Vietnam (ISO 704) not found in world-atlas');
const round=value=>Array.isArray(value)?value.map(round):Math.round(value*1000)/1000;
const out={type:'Feature',properties:{name:'Viet Nam',source:'Natural Earth 1:50m via world-atlas 2'},geometry:{type:vietnam.geometry.type,coordinates:round(vietnam.geometry.coordinates)}};
writeFileSync(new URL('../lib/vietnam.json',import.meta.url),JSON.stringify(out)+'\n');
console.log('wrote lib/vietnam.json',JSON.stringify(out).length,'bytes');
