import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// Live METAR weather demo — proxies to proxima-weather on :3000
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5174,
    proxy: {
      '/api': {
        target:       'http://localhost:3000',
        changeOrigin: true,
      },
    },
  },
});
