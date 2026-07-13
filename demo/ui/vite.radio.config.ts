import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// proxima Radio Explorer — proxies to proxima-radio on :3002
export default defineConfig({
  plugins: [
    react(),
    {
      // Rewrite the root URL to the radio entry point so localhost:5176/ works.
      name: 'radio-root-rewrite',
      configureServer(server) {
        server.middlewares.use((req, _res, next) => {
          if (req.url === '/') req.url = '/index.radio.html';
          next();
        });
      },
    },
  ],
  build: {
    rollupOptions: {
      input: 'index.radio.html',
    },
  },
  server: {
    port: 5176,
    proxy: {
      '/api': {
        target:       'http://localhost:3002',
        changeOrigin: true,
      },
    },
  },
});
