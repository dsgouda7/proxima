import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// AISStream.io demo UI — proxies to georedis-ais on :3002
// The AIS server exposes /api/aircraft and /api/region aliases so the
// existing aircraft UI renders vessel data without modification.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5175,
    proxy: {
      '/api': {
        target:       'http://localhost:3002',
        changeOrigin: true,
      },
    },
  },
});
