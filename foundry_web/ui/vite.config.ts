import path from "path"
import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite"

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  const backendProxy = env.FOUNDRY_BACKEND_PROXY ?? "http://localhost:8085";

  return {
    plugins: [react()],
    resolve: {
      alias: {
        "@": path.resolve(__dirname, "./src")
      }
    },
    server: {
      port: 5173,
      proxy: {
        "/api": {
          target: backendProxy,
          changeOrigin: true
        },
        "/healthz": {
          target: backendProxy,
          changeOrigin: true
        }
      }
    }
  };
});
