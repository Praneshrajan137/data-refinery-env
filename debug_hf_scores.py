#!/usr/bin/env python3
"""Deep investigation: test what HF Space actually returns for every task."""

import json
import urllib.request
import http.cookiejar

BASE = "https://praneshrajan15-data-quality-env.hf.space"
TASKS = [
    "task_1_format_fixer",
    "task_2_duplicate_detective",
    "task_3_integrity_auditor",
]


def post(opener, path, data):
    req = urllib.request.Request(
        BASE + path,
        data=json.dumps(data).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    return json.loads(opener.open(req, timeout=30).read())


def test_task_zero_work(task_id):
    """Zero-work scenario: reset -> finalize immediately."""
    cj = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))

    # Reset
    r1 = post(opener, "/reset", {"task_id": task_id})
    reset_done = r1.get("done")
    reset_reward = r1.get("reward")

    # Finalize
    r2 = post(opener, "/step", {"action": {"action_type": "finalize"}})
    final_done = r2.get("done")
    final_reward = r2.get("reward")
    obs = r2.get("observation", {})
    cum = obs.get("cumulative_reward")

    return {
        "task": task_id,
        "reset_done": reset_done,
        "reset_reward": reset_reward,
        "final_done": final_done,
        "final_reward": final_reward,
        "obs_cumulative_reward": cum,
    }


def test_task_max_steps(task_id):
    """Max-steps scenario: reset -> 100 inspects (hit max_steps)."""
    cj = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))

    # Reset
    post(opener, "/reset", {"task_id": task_id})

    # Do many inspects until done
    last_resp = None
    for i in range(100):
        try:
            r = post(opener, "/step", {
                "action": {"action_type": "inspect", "row_indices": [i % 50]}
            })
            last_resp = r
            if r.get("done"):
                break
        except Exception:
            break

    if last_resp:
        return {
            "task": task_id,
            "final_done": last_resp.get("done"),
            "final_reward": last_resp.get("reward"),
            "obs_cumulative_reward": last_resp.get("observation", {}).get("cumulative_reward"),
        }
    return {"task": task_id, "error": "no response"}


print("=" * 60)
print("  HF SPACE SCORE INVESTIGATION")
print("=" * 60)

# Test 1: Zero-work finalize
print("\n--- SCENARIO 1: Zero-work finalize ---")
for task_id in TASKS:
    try:
        result = test_task_zero_work(task_id)
        r = result["final_reward"]
        ok = r is not None and 0.0 < r < 1.0
        status = "PASS" if ok else "FAIL"
        print("  [{status}] {task}: reward={r}, done={d}, cum={c}".format(
            status=status, task=task_id,
            r=result["final_reward"],
            d=result["final_done"],
            c=result["obs_cumulative_reward"],
        ))
        if result["reset_reward"] == 0.0:
            print("    WARNING: reset reward is exactly 0.0 (done={d})".format(
                d=result["reset_done"]
            ))
    except Exception as e:
        print("  [ERROR] {task}: {err}".format(task=task_id, err=str(e)[:200]))

# Test 2: Max-steps auto-finalize
print("\n--- SCENARIO 2: Max-steps auto-finalize ---")
for task_id in TASKS:
    try:
        result = test_task_max_steps(task_id)
        r = result.get("final_reward")
        ok = r is not None and 0.0 < r < 1.0
        status = "PASS" if ok else "FAIL"
        print("  [{status}] {task}: reward={r}, done={d}, cum={c}".format(
            status=status, task=task_id,
            r=result.get("final_reward"),
            d=result.get("final_done"),
            c=result.get("obs_cumulative_reward"),
        ))
    except Exception as e:
        print("  [ERROR] {task}: {err}".format(task=task_id, err=str(e)[:200]))

# Test 3: Check the raw reset response
print("\n--- SCENARIO 3: Raw reset observation ---")
for task_id in TASKS:
    try:
        cj = http.cookiejar.CookieJar()
        opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
        r = post(opener, "/reset", {"task_id": task_id})
        print("  {task}: TOP-LEVEL keys={keys}".format(
            task=task_id, keys=sorted(r.keys())
        ))
        print("    done={d}, reward={r}".format(d=r.get("done"), r=r.get("reward")))
        if r.get("reward") == 0.0:
            print("    *** RESET REWARD IS 0.0 ***")
    except Exception as e:
        print("  [ERROR] {task}: {err}".format(task=task_id, err=str(e)[:200]))

print("\n" + "=" * 60)
