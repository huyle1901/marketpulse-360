/** Bundled coordinates (WGS84, approximate city centres) for the generator's cities; no runtime geodata download. */
/** label: which side the leader line points to, so neighbouring cities (Ha Noi/Hai Phong, HCMC/Can Tho) never collide. */
export type Region='North'|'Central'|'South';
export const CITY_COORDINATES:Record<string,{lat:number;lon:number;label:'left'|'right'}>={
 'Ha Noi':{lat:21.028,lon:105.854,label:'left'},
 'Hai Phong':{lat:20.845,lon:106.688,label:'right'},
 'Da Nang':{lat:16.054,lon:108.202,label:'left'},
 'Ho Chi Minh City':{lat:10.776,lon:106.701,label:'right'},
 'Can Tho':{lat:10.045,lon:105.747,label:'left'},
};
/** Vietnam's three customary regions; a city not listed here counts as Other. */
export const CITY_REGION:Record<string,Region>={'Ha Noi':'North','Hai Phong':'North','Da Nang':'Central','Hue':'Central','Ho Chi Minh City':'South','Can Tho':'South'};
/** Representative islands of the Hoàng Sa and Trường Sa archipelagos, drawn on every map of Viet Nam. */
export const ARCHIPELAGOS=[
 {name:'Hoàng Sa',lat:16.6,lon:112.1,islands:[[111.6,16.5],[112.3,16.8],[111.9,16.2],[112.7,16.6],[111.2,15.8]] as [number,number][]},
 {name:'Trường Sa',lat:9.9,lon:114.4,islands:[[114.3,10.4],[113.9,8.9],[115.5,9.9],[112.9,8.6],[114.9,11.4],[114.1,9.6]] as [number,number][]},
];
