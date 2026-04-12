#!/usr/bin/env python3
"""Verify that the HF Space returns scores strictly in (0, 1)."""

import json
import sys
import urllib.request
import http.cookiejar

BASE = "https://praneshrajan15-data-quality-env.hf.space"
TASKS = [
    "task_1_format_fixer",
    "task_2_duplicate_detective",
    "task_3_integrity_auditor",
]

def test_task(task_id: str) -> float:
    """Reset + finalize a task and return the score."""
    cj = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))

    # Reset
    req = urllib.request.Request(
        f"{BASE}/reset",
        data=json.dumps({"task_id": task_id}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    opener.open(req, timeout=30).read()

    # Finalize immediately (worst case: score should be > 0)
    req2 = urllib.request.Request(
        f"{BASE}/step",
        data=json.dumps({"action": {"action_type": "finalize"}}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    resp = json.loads(opener.open(req2, timeout=30).read())
    return float(resp.get("reward", 0))


def main():
    print("Score Range Validator")
    print("=" * 50)
    all_pass = True
    
    for task_id in TASKS:
        try:
            score = test_task(task_id)
            ok = 0.0 < score < 1.0
            status = "PASS" if ok else "FAIL"
            print(f"  [{status}] {task_id}: {score}")
            if not ok:
                all_pass = False
        except Exception as e:
            print(f"  [ERROR] {task_id}: {e}")
            all_pass = False
    
    print("=" * 50)
    if all_pass:
        print("ALL SCORES STRICTLY IN (0, 1) — READY TO SUBMIT")
    else:
        print("SOME SCORES OUT OF RANGE — FIX NEEDED")
    
    return 0 if all_pass else 1


if __name__ == "__main__":
    sys.exit(main())
