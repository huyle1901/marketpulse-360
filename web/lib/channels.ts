/** The one list of paid channels: every view takes names, order and colour from here, never from a local copy. */
export type ChannelKey='google'|'meta'|'tiktok'|'shopee';
export type Channel={key:ChannelKey;label:string;color:string};

const channel=(key:ChannelKey,label:string):Channel=>({key,label,color:`var(--channel-${key})`});

/** Colour follows the channel, not its rank, so filtering one out never repaints the others. */
export const CHANNELS:readonly Channel[]=[channel('google','Google'),channel('meta','Meta'),channel('tiktok','TikTok'),channel('shopee','Shopee')];
export const CHANNEL_KEYS:readonly string[]=CHANNELS.map(c=>c.key);
export const UNATTRIBUTED_COLOR='var(--channel-unattributed)';

export const channelOf=(key:string|null|undefined):Channel|undefined=>CHANNELS.find(c=>c.key===key);
export const isChannel=(key:string|null|undefined):boolean=>channelOf(key)!==undefined;
/** Colour for any channel key; unknown or missing keys read as unattributed. */
export const channelColor=(key:string|null|undefined):string=>channelOf(key)?.color??UNATTRIBUTED_COLOR;
