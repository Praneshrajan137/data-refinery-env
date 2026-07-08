# DataForge Selective-Repair Calibration Benchmark

Distribution-free certified auto-apply coverage for the LLM corrector, framed as selective classification. Thresholds are certified on a calibration split (conformal risk control) and measured on a disjoint test split - never an in-sample number - and summarized by a risk-coverage curve (AURC), an alpha sweep, real-data split validity, and a reliability diagram.

Method: the auto-apply gate is a selective classifier (Geifman & El-Yaniv, 2017). Per-class thresholds are certified with conformal risk control (Angelopoulos et al., 2022; RCPS, Bates et al., 2021) on a calibration split and measured on a disjoint test split. We report the risk-coverage curve and its AURC, an alpha sweep of certified coverage, a K random-split validity check, and a reliability diagram with ECE (Guo et al., 2017).

Settings: primary alpha=0.05, delta=0.05, min_support=30, splits=200.

| Condition | Model | Dataset | Pooled n | ECE | prec@auto | AURC | Certified coverage | Promoted |
|---|---|---|---|---|---|---|---|---|
| minimal | gpt-5-mini | hospital | 81 | 0.8436 | 0.0508 | 0.951149 | 0.0 | False |
| medium | gpt-5-mini | hospital | 27 | 0.8889 | 0.0435 | 0.957018 | 0.0 | False |

Conclusion: No condition earned any distribution-free certified auto-apply coverage at the tested alphas (primary alpha=0.05). Propose-not-apply is the provably correct policy; calibration - not model capability or effort - is the binding constraint.

Scope and limits: the conformal guarantee holds for data exchangeable with the calibration sample (a distribution-shift monitor downgrades auto-apply otherwise); error classes are assigned by the versioned heuristic labeler (LABELER_VERSION v1); classes below min_support are reported as insufficient support rather than certified. The SMT verifier and safety constitution remain the hard floor beneath the calibration layer.
