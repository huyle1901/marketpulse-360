/** Bundled coordinates (WGS84, approximate city centres) for the generator's cities; no runtime geodata download. */
/** label: which side the direct label sits on, so neighbouring cities (Ha Noi/Hai Phong, HCMC/Can Tho) never collide. */
export const CITY_COORDINATES:Record<string,{lat:number;lon:number;label:'left'|'right'}>={
 'Ha Noi':{lat:21.028,lon:105.854,label:'left'},
 'Hai Phong':{lat:20.845,lon:106.688,label:'right'},
 'Da Nang':{lat:16.054,lon:108.202,label:'right'},
 'Ho Chi Minh City':{lat:10.776,lon:106.701,label:'right'},
 'Can Tho':{lat:10.045,lon:105.747,label:'left'},
};
