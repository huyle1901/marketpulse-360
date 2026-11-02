import { defineConfig } from '@playwright/test';
export default defineConfig({testDir:'./tests',workers:1,use:{baseURL:'http://127.0.0.1:3100'},
  // Unmocked API calls must fail at once (connection refused), never wait on the network's DNS for 'api'.
  webServer:{command:'npm run dev',url:'http://127.0.0.1:3100',reuseExistingServer:false,env:{API_INTERNAL_URL:'http://127.0.0.1:9'}},reporter:'list'});
