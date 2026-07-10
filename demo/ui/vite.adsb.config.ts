import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// ADSB.fi demo UI — proxies to georedis-adsb on :3001
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5174,
    proxy: {
      '/api': {
        target:       'http://localhost:3001',
        changeOrigin: true,
      },
    },
  },
});
