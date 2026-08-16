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
        manualChunks: undefined,
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
