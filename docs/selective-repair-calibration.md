# DataForge Selective-Repair Calibration Benchmark

Distribution-free certified auto-apply coverage for the LLM corrector, framed as selective classification. Thresholds are certified on a calibration split (conformal risk control) and measured on a disjoint test split - never an in-sample number - and summarized by a risk-coverage curve (AURC), an alpha sweep, real-data split validity, and a reliability diagram.

Method: the auto-apply gate is a selective classifier (Geifman & El-Yaniv, 2017). Per-class thresholds are certified with conformal risk control (Angelopoulos et al., 2022; RCPS, Bates et al., 2021) on a calibration split and measured on a disjoint test split. We report the risk-coverage curve and its AURC, an alpha sweep of certified coverage, a K random-split validity check, and a reliability diagram with ECE (Guo et al., 2017).

Settings: primary alpha=0.05, delta=0.05, min_support=30, splits=200.

| Condition | Model | Dataset | Pooled n | ECE | prec@auto | AURC | Certified coverage | Promoted |
|---|---|---|---|---|---|---|---|---|
| minimal | gpt-5-mini | hospital | 51 | 0.7974 | 0.0811 | 0.922218 | 0.0 | False |
| medium | gpt-5-mini | hospital | 18 | 0.8704 | 0.0714 | 0.930335 | 0.0 | False |

Conclusion: No condition earned any distribution-free certified auto-apply coverage at the tested alphas (primary alpha=0.05). Propose-not-apply is the provably correct policy; calibration - not model capability or effort - is the binding constraint.

## Post-hoc calibration (does it move the wall?)

Post-hoc calibration (`dataforge/calibration_map.py`, isotonic via pool-adjacent-violators or Platt) is fit per issue type on a calibration split and measured on a disjoint test split. It makes the reported confidence an honest probability, but is monotone: it preserves proposal ranking and therefore does NOT change the conformal-certifiable coverage reported above.

- minimal: ECE 0.8533 -> 0.0 on a disjoint n=25 test split. Read honestly, this is a degenerate regime, not a calibration triumph: the corrector's precision is 0.0588, so isotonic collapses its confidence toward 0 (trivially well-calibrated) -- the number proves the confidence is now honest, not that the corrector improved.

- medium: ECE 0.8333 -> 0.0 on a disjoint n=10 test split. Read honestly, this is a degenerate regime, not a calibration triumph: the corrector's precision is 0.0556, so isotonic collapses its confidence toward 0 (trivially well-calibrated) -- the number proves the confidence is now honest, not that the corrector improved.

## What would it take to certify (the honest data budget)

Auto-apply is bounded by correctness, not calibration. With zero observed errors the Clopper-Pearson upper bound is `1 - delta**(1/n)`; certifying precision `1 - alpha` needs that bound `<= alpha`, i.e. `n >= ln(delta) / ln(1 - alpha)` accepted-and-correct samples above the threshold. At alpha=0.05, delta=0.05 that floor is **59** all-correct accepted samples -- the floor even for a PERFECT corrector. The unlock for LLM auto-apply is therefore more labelled outcomes from a more precise corrector, not more calibration math.

Scope and limits: the conformal guarantee holds for data exchangeable with the calibration sample (a distribution-shift monitor downgrades auto-apply otherwise); error classes are assigned by the versioned heuristic labeler (LABELER_VERSION v1); classes below min_support are reported as insufficient support rather than certified. The SMT verifier and safety constitution remain the hard floor beneath the calibration layer.
