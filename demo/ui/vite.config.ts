import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      // forward /api/* to the Rust backend during development
      '/api': {
        target:       'http://localhost:3000',
        changeOrigin: true,
      },
    },
  },
});
