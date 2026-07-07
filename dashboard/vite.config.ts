import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// Dashboard served from the tsdb metrics_server at GET /.  The dev server
// proxies every API path to a running node on localhost:28094; override
// with VITE_API_TARGET to hit a remote node during development.
const API = process.env.VITE_API_TARGET || 'http://127.0.0.1:28094';

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/sql':             { target: API, changeOrigin: true },
      '/tree':            { target: API, changeOrigin: true },
      '/cluster':         { target: API, changeOrigin: true },
      '/raft':            { target: API, changeOrigin: true },
      '/audit':           { target: API, changeOrigin: true },
      '/health':          { target: API, changeOrigin: true },
      '/metrics':         { target: API, changeOrigin: true },
      '/login':           { target: API, changeOrigin: true },
      '/logout':          { target: API, changeOrigin: true },
      '/backup':          { target: API, changeOrigin: true },
      '/retention':       { target: API, changeOrigin: true },
      '/pitr':            { target: API, changeOrigin: true },
      '/catalog':         { target: API, changeOrigin: true },
    },
  },
  build: {
    outDir: 'dist',
    emptyOutDir: true,
    sourcemap: false,
    // Hashed asset names land under /assets/ which the metrics_server
    // route matcher looks for.  Keep them there.
    assetsDir: 'assets',
  },
});
