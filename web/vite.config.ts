import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";
export default defineConfig({
  plugins: [react()],
  server: {
    host: "127.0.0.1",
    port: 5173,
    strictPort: true,
    proxy: {
      "/v1": {
        target: process.env.FORGESWARM_API ?? "http://127.0.0.1:8080",
        changeOrigin: false,
      },
      "/health": {
        target: process.env.FORGESWARM_API ?? "http://127.0.0.1:8080",
      },
    },
  },
  test: {
    environment: "jsdom",
    include: ["src/**/*.test.tsx", "src/**/*.test.ts"],
    maxWorkers: 2,
  },
});
