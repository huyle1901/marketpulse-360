/** Bundled coordinates (WGS84, approximate city centres) for the generator's cities; no runtime geodata download. */
/** label: which side the leader line points to; dy: vertical callout offset. Tuned so all 13 callout boxes stay apart at any bubble size
 * (South stacks Buon Ma Thuot, Thu Dau Mot, Bien Hoa, Can Tho on the left; Quy Nhon, Nha Trang, HCMC, Vung Tau on the right). */
export type Region='North'|'Central'|'South';
export const CITY_COORDINATES:Record<string,{lat:number;lon:number;label:'left'|'right';dy:number}>={
 'Ho Chi Minh City':{lat:10.776,lon:106.701,label:'right',dy:0},
 'Ha Noi':{lat:21.028,lon:105.854,label:'left',dy:0},
 'Bien Hoa':{lat:10.957,lon:106.843,label:'left',dy:-38},
 'Thu Dau Mot':{lat:10.98,lon:106.652,label:'left',dy:-87},
 'Hai Phong':{lat:20.845,lon:106.688,label:'right',dy:8},
 'Da Nang':{lat:16.054,lon:108.202,label:'left',dy:0},
 'Can Tho':{lat:10.045,lon:105.747,label:'left',dy:0},
 'Nha Trang':{lat:12.239,lon:109.197,label:'right',dy:-10},
 'Hue':{lat:16.463,lon:107.59,label:'left',dy:-40},
 'Vung Tau':{lat:10.346,lon:107.084,label:'right',dy:44},
 'Quy Nhon':{lat:13.776,lon:109.224,label:'right',dy:0},
 'Buon Ma Thuot':{lat:12.667,lon:108.038,label:'left',dy:-74},
 'Ha Long':{lat:20.951,lon:107.08,label:'right',dy:-40},
};
/** Vietnam's three customary regions; a city not listed here counts as Other. */
export const CITY_REGION:Record<string,Region>={'Ha Noi':'North','Hai Phong':'North','Ha Long':'North','Da Nang':'Central','Hue':'Central','Quy Nhon':'Central','Nha Trang':'Central','Buon Ma Thuot':'Central',
 'Ho Chi Minh City':'South','Bien Hoa':'South','Thu Dau Mot':'South','Vung Tau':'South','Can Tho':'South'};
/** Representative islands of the Hoàng Sa and Trường Sa archipelagos, drawn on every map of Viet Nam. */
export const ARCHIPELAGOS=[
 {name:'Hoàng Sa',lat:16.6,lon:112.1,islands:[[111.6,16.5],[112.3,16.8],[111.9,16.2],[112.7,16.6],[111.2,15.8]] as [number,number][]},
 {name:'Trường Sa',lat:9.9,lon:114.4,islands:[[114.3,10.4],[113.9,8.9],[115.5,9.9],[112.9,8.6],[114.9,11.4],[114.1,9.6]] as [number,number][]},
];
