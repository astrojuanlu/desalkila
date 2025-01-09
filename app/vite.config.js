import { defineConfig } from "vite";
import wasm from "vite-plugin-wasm";
import topLevelAwait from "vite-plugin-top-level-await";

export default defineConfig({
  plugins: [wasm(), topLevelAwait()],
  build: {
    sourcemap: true,
    assetsInlineLimit: 0,  // Because fuck you, that's why
  }
});

  //   target: "esnext",
  //   modulePreload: {
  //     polyfill: false,
  //   },
  //   rollupOptions: {
  //     // This ensures proper WASM loading in different environments
  //     output: {
  //       manualChunks: {
  //         geoparquet: ["@geoarrow/geoparquet-wasm"],
  //       },
  //     },
  //   },
  // },
  // optimizeDeps: {
  //   // This is needed to prevent Vite from trying to bundle WASM modules during dev
  //   exclude: ["@geoarrow/geoparquet-wasm"],
  // },
  // resolve: {
  //   alias: {
  //     // This allows the module loader to work in both dev and prod
  //     "@geoarrow/geoparquet-wasm":
  //       process.env.NODE_ENV === "production"
  //         ? "@geoarrow/geoparquet-wasm/esm/index.js"
  //         : "@geoarrow/geoparquet-wasm/node/index.js",
  //   },
  // },