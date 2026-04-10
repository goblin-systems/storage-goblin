import { resolve } from "node:path";
import { defineConfig } from "vite";

const host = process.env.TAURI_DEV_HOST;

export default defineConfig(async () => ({
  clearScreen: false,
  build: {
    rollupOptions: {
      input: {
        main: resolve(__dirname, "index.html"),
      },
    },
  },
  server: {
    port: 1432,
    strictPort: true,
    host: host || false,
    hmr: host
      ? {
          protocol: "ws",
          host,
          port: 1433,
        }
      : undefined,
    watch: {
      ignored: ["**/src-tauri/**"],
    },
  },
}));
