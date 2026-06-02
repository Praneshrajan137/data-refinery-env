#!/usr/bin/env node
import { createRequire } from "node:module";

const require = createRequire(new URL("../../playground/web/package.json", import.meta.url));
const { chromium, devices } = require("playwright");

const DEFAULT_FRONTEND_URL = "https://dataforge.praneshrajan15.workers.dev/playground";
const DEFAULT_BACKEND_URL = "https://Praneshrajan15-dataforge-playground.hf.space";
const SAMPLE_CSV = "id,amount,state\n1,100,AL\n2,1020,AX\n3,105,AL\n";

function parseArgs(argv) {
  const args = {
    frontendUrl: DEFAULT_FRONTEND_URL,
    backendUrl: DEFAULT_BACKEND_URL,
    json: false,
  };
  for (let index = 0; index < argv.length; index += 1) {
    const value = argv[index];
    if (value === "--frontend-url") {
      args.frontendUrl = argv[index + 1];
      index += 1;
    } else if (value === "--backend-url") {
      args.backendUrl = argv[index + 1];
      index += 1;
    } else if (value === "--json") {
      args.json = true;
    } else {
      throw new Error(`Unknown argument: ${value}`);
    }
  }
  return args;
}

function originFor(url) {
  return new URL(url).origin;
}

async function withPage(name, contextOptions, frontendUrl, fn) {
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({ ...contextOptions, acceptDownloads: true });
  await context.grantPermissions(["clipboard-read", "clipboard-write"], {
    origin: originFor(frontendUrl),
  });
  const page = await context.newPage();
  const consoleMessages = [];
  page.on("console", (message) => {
    if (["error", "warning"].includes(message.type())) {
      consoleMessages.push(`${message.type()}: ${message.text()}`);
    }
  });
  page.on("pageerror", (error) => {
    consoleMessages.push(`pageerror: ${error.message}`);
  });

  const started = Date.now();
  try {
    await fn(page);
    return { name, ok: true, ms: Date.now() - started, console: consoleMessages };
  } catch (error) {
    return {
      name,
      ok: false,
      ms: Date.now() - started,
      error: error instanceof Error ? error.message : String(error),
      console: consoleMessages,
    };
  } finally {
    await browser.close();
  }
}

async function runDesktopAudit(frontendUrl) {
  return withPage(
    "desktop_upload_profile_repair_copy_export_error",
    { viewport: { width: 1440, height: 950 } },
    frontendUrl,
    async (page) => {
      await page.goto(frontendUrl, { waitUntil: "load", timeout: 30_000 });
      await page.getByRole("button", { name: "Profile" }).waitFor({ state: "visible", timeout: 30_000 });
      await page.locator("#csv-upload").setInputFiles({
        name: "audit.csv",
        mimeType: "text/csv",
        buffer: Buffer.from(SAMPLE_CSV),
      });
      await page.getByText("audit.csv").waitFor({ timeout: 10_000 });
      await page.getByRole("button", { name: "Profile" }).click();
      await page.getByText("repair_contract_v2").waitFor({ timeout: 30_000 });
      await page.getByRole("button", { name: "Repair dry run" }).click();
      await page.getByRole("button", { name: "Copy" }).waitFor({ timeout: 30_000 });
      await page.getByRole("tab", { name: "Journal" }).click();
      await page.getByLabel("Dry-run transaction journal").waitFor({ timeout: 10_000 });
      await page.getByRole("button", { name: "Copy" }).click();
      await page.getByRole("button", { name: "Copied" }).waitFor({ timeout: 10_000 });
      const clipboard = await page.evaluate(() => navigator.clipboard.readText());
      if (!clipboard.includes("transaction_journal")) {
        throw new Error("Clipboard evidence is missing transaction_journal.");
      }

      const downloadPromise = page.waitForEvent("download", { timeout: 10_000 });
      await page.getByRole("button", { name: "Export" }).click();
      const download = await downloadPromise;
      if (!download.suggestedFilename().includes("dataforge-dry-run")) {
        throw new Error(`Unexpected export filename: ${download.suggestedFilename()}`);
      }

      await page.locator("#csv-upload").setInputFiles({
        name: "broken.csv",
        mimeType: "text/csv",
        buffer: Buffer.from('id,name\n1,"unterminated'),
      });
      await page.getByRole("alert").waitFor({ timeout: 10_000 });
    },
  );
}

async function runMobileAudit(frontendUrl) {
  return withPage("mobile_sample_profile_repair_layout", devices["Pixel 7"], frontendUrl, async (page) => {
    await page.goto(frontendUrl, { waitUntil: "load", timeout: 30_000 });
    await page.getByRole("button", { name: /Hospital/ }).click();
    await page.getByText("Current CSV").waitFor({ timeout: 15_000 });
    await page.getByRole("button", { name: "Profile" }).click();
    await page.getByText("repair_contract_v2").waitFor({ timeout: 30_000 });
    await page.getByRole("button", { name: "Repair dry run" }).click();
    await page.getByRole("button", { name: "Copy" }).waitFor({ timeout: 30_000 });
    const layout = await page.evaluate(() => ({
      scrollWidth: document.documentElement.scrollWidth,
      innerWidth: window.innerWidth,
    }));
    if (layout.scrollWidth > layout.innerWidth + 1) {
      throw new Error(`Mobile body overflow: ${layout.scrollWidth} > ${layout.innerWidth}`);
    }
  });
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const results = [
    await runDesktopAudit(args.frontendUrl),
    await runMobileAudit(args.frontendUrl),
  ];
  const report = {
    ok: results.every((result) => result.ok && result.console.length === 0),
    frontend_url: args.frontendUrl,
    backend_url: args.backendUrl,
    results,
  };
  if (args.json) {
    console.log(JSON.stringify(report, null, 2));
  } else {
    for (const result of results) {
      const status = result.ok && result.console.length === 0 ? "ok" : "fail";
      console.log(`${status.padEnd(4)} ${result.name}: ${result.ms}ms`);
      for (const message of result.console) {
        console.log(`     console: ${message}`);
      }
      if (result.error) {
        console.log(`     error: ${result.error}`);
      }
    }
  }
  process.exit(report.ok ? 0 : 1);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
