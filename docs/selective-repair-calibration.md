# DataForge Selective-Repair Calibration Benchmark

Distribution-free certified auto-apply coverage for the LLM corrector, framed as selective classification. Thresholds are certified on a calibration split (conformal risk control) and measured on a disjoint test split - never an in-sample number - and summarized by a risk-coverage curve (AURC), an alpha sweep, real-data split validity, and a reliability diagram.

Method: the auto-apply gate is a selective classifier (Geifman & El-Yaniv, 2017). Per-class thresholds are certified with conformal risk control (Angelopoulos et al., 2022; RCPS, Bates et al., 2021) on a calibration split and measured on a disjoint test split. We report the risk-coverage curve and its AURC, an alpha sweep of certified coverage, a K random-split validity check, and a reliability diagram with ECE (Guo et al., 2017).

Settings: primary alpha=0.05, delta=0.05, min_support=30, splits=200.

| Condition | Model | Dataset | Pooled n | ECE | prec@auto | AURC | Certified coverage | Promoted |
|---|---|---|---|---|---|---|---|---|
| minimal | gpt-5-mini | hospital | 81 | 0.8436 | 0.0508 | 0.951149 | 0.0 | False |
| medium | gpt-5-mini | hospital | 27 | 0.8889 | 0.0435 | 0.957018 | 0.0 | False |

Conclusion: No condition earned any distribution-free certified auto-apply coverage at the tested alphas (primary alpha=0.05). Propose-not-apply is the provably correct policy; calibration - not model capability or effort - is the binding constraint.

## Post-hoc calibration (does it move the wall?)

Post-hoc calibration (`dataforge/calibration_map.py`, isotonic via pool-adjacent-violators or Platt) is fit per issue type on a calibration split and measured on a disjoint test split (n=18). On the real Azure `gpt-5-mini` samples it drops the Expected Calibration Error from **0.807 to 0.0** (`eval/results/corrector_calibration.json`). Read honestly, this is a degenerate regime, not a calibration triumph: the corrector is ~4% precise, so isotonic maps its confidence toward 0, which is trivially well-calibrated - the number proves the confidence is now honest (near-zero), not that the corrector improved.

But it does **not** move the auto-apply wall. Isotonic/Platt maps are monotone, so they preserve the ranking of proposals; the conformal certification depends only on that ranking, so certified auto-apply coverage stays **0.0 before and after calibration**. This is the precise, honest decomposition: calibration fixes *probability honesty* (ECE), while auto-apply coverage is bounded by *correctness* (precision at coverage), which a weak corrector does not have. The calibrated score is still wired into the engine gate (behind differential SMT, a PSI drift guard, and the certified threshold) so a genuinely precise future model applies safely - the gate never manufactures coverage from a poorly-calibrated-but-also-imprecise model.

Scope and limits: the conformal guarantee holds for data exchangeable with the calibration sample (a distribution-shift monitor downgrades auto-apply otherwise); error classes are assigned by the versioned heuristic labeler (LABELER_VERSION v1); classes below min_support are reported as insufficient support rather than certified. The SMT verifier and safety constitution remain the hard floor beneath the calibration layer.

## What would it take to certify (the honest data budget)

Auto-apply is bounded by *correctness*, not calibration. With zero observed errors the Clopper-Pearson upper bound is `1 - delta**(1/n)`; certifying precision `1 - alpha` needs that bound `<= alpha`, i.e. `n >= ln(delta) / ln(1 - alpha)` accepted-and-correct samples above the threshold. At the primary `alpha = delta = 0.05` this floor is **59** all-correct accepted samples per issue type - and that is the floor for a *perfect* corrector; a corrector with any error rate needs more. The shipped `gpt-5-mini` artifact has 36 `fd_violation` outcomes at ~4% precision, so no threshold is certifiable and every class is parked at the disabled `1.01` sentinel with a machine-readable reason in `policy.uncertified_classes` (see `dataforge.conformal.min_samples_for_certification` and `certification_reason`). The unlock for LLM auto-apply is therefore *more labelled outcomes from a more precise corrector*, not more calibration math.
