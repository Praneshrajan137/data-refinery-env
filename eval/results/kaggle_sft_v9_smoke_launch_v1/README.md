# SFT-v9 Smoke Launch Evidence

This folder freezes the launch evidence for the private Kaggle SFT-v9 action-envelope smoke run.

- Dataset: `praneshrajan15/dataforge-sft-v9-handoff`
- Kernel: `praneshrajan15/dataforge-0-5b-sft-v9-candidate`
- Stage: `smoke`
- Kaggle push result: kernel version `1` pushed successfully
- Public claim update: not allowed
- GRPO-v4: still blocked

The local preflight was already passing: completion parse success `1.0`, held-out leakage `0`, `finish_with_repairs` `0`, and zero negative-contrast target leakage.

The Kaggle CLI accepted the private dataset/kernel upload, but read-side status commands returned private-kernel permission/401 errors immediately after push. This record is therefore a launch receipt, not a completed training report. The next action is to pull targeted JSON outputs after the notebook finishes.
