import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// Live METAR weather demo — proxies to geo-redis-weather on :3001
// (when running alongside the aircraft tracker; standalone default is :3000)
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
