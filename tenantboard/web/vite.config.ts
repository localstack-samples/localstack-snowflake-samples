import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// The dashboard calls /api/* on its own origin; the dev server forwards those
// requests to the FastAPI service, so the browser never needs CORS.
const apiPort = process.env.API_PORT ?? "8000";
const webPort = Number(process.env.WEB_PORT ?? "5173");

export default defineConfig({
  plugins: [react()],
  server: {
    host: "127.0.0.1",
    port: webPort,
    strictPort: true,
    proxy: {
      "/api": { target: `http://127.0.0.1:${apiPort}`, changeOrigin: false },
    },
  },
  preview: { host: "127.0.0.1", port: webPort, strictPort: true },
});
