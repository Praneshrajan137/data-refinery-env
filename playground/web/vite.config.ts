import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

export default defineConfig({
  base: process.env.VITE_BASE ?? "/playground/",
  plugins: [react()],
  build: {
    outDir: "dist",
    sourcemap: true,
    rollupOptions: {
      output: {
        // Third-party code is split from application code so the bundle budget is
        // ATTRIBUTABLE: a single monolithic chunk tells you that the bundle grew but
        // never which half grew, which is most of why an unbounded budget survived.
        // The split is also the caching-honest shape -- app code changes on every
        // deploy, these four dependencies do not.
        manualChunks: (id) => {
          if (!id.includes("node_modules")) {
            return undefined;
          }
          // `motion` gets its own chunk because it is the single largest dependency: measured
          // at 41.4 KiB gzip, roughly a quarter of all shipped JS. Folded into `vendor` its
          // growth was attributable only to "dependencies", which is the same blindness a
          // monolithic bundle has, one level down. Isolated, it carries its own ceiling.
          if (id.includes("node_modules/motion") || id.includes("framer-motion")) {
            return "motion";
          }
          return "vendor";
        },
      },
    },
  },
  test: {
    environment: "jsdom",
    globals: true,
    setupFiles: "./src/test/setup.ts",
    // scripts/ is included so the perceptual measurement kernel can be unit-tested.
    // It lives outside src/ on purpose: culori is a devDependency and
    // SPEC_color_system.md requires that no colour engine ship in the browser bundle,
    // so keeping the engine out of src/ makes that structural rather than dependent on
    // tree-shaking.
    include: ["src/**/*.test.ts", "src/**/*.test.tsx", "scripts/**/*.test.mjs"],
    css: true,
  },
});
