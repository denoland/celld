import { defineConfig } from "vite";
import preact from "@preact/preset-vite";

// In order to be able to test the docker build of this application easily
// we run vite on port 8000 and proxy to celld on 8100. The github oauth app is
// configured for localhost:8000 callback.
// When running on docker, we will use port 8000 as well but celld serves
// everything.

// https://vite.dev/config/
export default defineConfig({
  plugins: [preact()],
  server: {
    port: 8000,
    proxy: {
      "/cell": {
        target: "http://localhost:8100",
        changeOrigin: true,
        ws: true,
      },
    },
  },
});
