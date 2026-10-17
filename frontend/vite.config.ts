import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// dev server proxies the API so `npm run dev` works against `docker compose up backend`
export default defineConfig({
  plugins: [react()],
  build: { chunkSizeWarningLimit: 1000 },
  server: {
    port: 5173,
    proxy: {
      "/api": "http://localhost:8000",
      "/ws": { target: "ws://localhost:8000", ws: true },
    },
  },
});
