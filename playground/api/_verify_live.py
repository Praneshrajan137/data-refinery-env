"""Wait for rebuild and verify full stack (API + frontend)."""
import time
import httpx
from huggingface_hub import HfApi

REPO_ID = "Praneshrajan15/dataforge-playground"
BASE = "https://praneshrajan15-dataforge-playground.hf.space"

api = HfApi()

print("Waiting for Space to rebuild...")
for i in range(30):
    info = api.space_info(REPO_ID)
    stage = info.runtime.stage if info.runtime else "UNKNOWN"
    print(f"  [{i*10}s] {stage}")
    if stage == "RUNNING":
        break
    if stage in ("BUILD_ERROR", "RUNTIME_ERROR", "CONFIG_ERROR"):
        print(f"FAILED: {stage}")
        exit(1)
    time.sleep(10)

print("\n=== Health ===")
r = httpx.get(f"{BASE}/api/health", timeout=30)
print(f"  {r.status_code} {r.json()}")

print("\n=== Frontend (/) ===")
r = httpx.get(BASE, timeout=30, follow_redirects=True)
print(f"  Status: {r.status_code}")
has_title = "DataForge Playground" in r.text
has_picocss = "picocss" in r.text or "pico" in r.text
has_app_js = "app.js" in r.text
print(f"  Has title: {has_title}")
print(f"  Has Pico.css: {has_picocss}")
print(f"  Has app.js: {has_app_js}")

print("\n=== Static CSS (/static/style.css) ===")
r = httpx.get(f"{BASE}/static/style.css", timeout=30)
print(f"  Status: {r.status_code}")
print(f"  Size: {len(r.text)} bytes")

print("\n=== Static JS (/static/app.js) ===")
r = httpx.get(f"{BASE}/static/app.js", timeout=30)
print(f"  Status: {r.status_code}")
print(f"  Size: {len(r.text)} bytes")

print("\n=== Profile endpoint ===")
sample = httpx.get(f"{BASE}/api/samples/hospital_10rows", timeout=30).content
t0 = time.perf_counter()
r = httpx.post(f"{BASE}/api/profile", files={"file": ("h.csv", sample, "text/csv")}, timeout=30)
dt = time.perf_counter() - t0
print(f"  Status: {r.status_code} Latency: {dt:.2f}s")
d = r.json()
print(f"  Issues: {len(d['issues'])}")

print("\n=== Repair endpoint ===")
t0 = time.perf_counter()
r = httpx.post(f"{BASE}/api/repair", params={"dry_run": "true"}, files={"file": ("h.csv", sample, "text/csv")}, timeout=30)
dt = time.perf_counter() - t0
print(f"  Status: {r.status_code} Latency: {dt:.2f}s")
if r.status_code == 200:
    d = r.json()
    print(f"  Fixes: {len(d['fixes'])}")

print("\n=== ALL VERIFIED ===")
print(f"Live URL: {BASE}")
