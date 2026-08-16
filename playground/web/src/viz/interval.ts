/**
 * Exact confidence intervals for a proportion.
 *
 * WHY THIS EXISTS
 * ---------------
 * The frontend rendered no uncertainty at all. Searching for `interval`, `error bar`,
 * `quantile`, `variance`, `stderr` and `percentile` across `src/` returned nothing; the only
 * carrier of uncertainty was an optional free-text string on a workflow stage. Meanwhile the
 * backend's `TrustLedger.as_dict()` emits a Clopper-Pearson bound and a scope caveat BY
 * CONSTRUCTION, precisely so a consumer cannot read a point estimate alone -- and the
 * frontend was a consumer that could only read point estimates.
 *
 * The confidence histogram is where this bites first. Each bin is a count out of a known
 * class total, so every bar is a binomial proportion with a computable interval, and drawing
 * the bar alone states a precision the data does not have.
 *
 * METHOD
 * ------
 * Clopper-Pearson, the same method `dataforge/metrics/trust_ledger.py` uses, so the product
 * has one definition of "interval" rather than one per language. Verified against the same
 * numeric goldens as the Python tests -- an independent implementation agreeing on shared
 * values, which is the pattern already established for the attestation verifier.
 *
 * Computed in LOG SPACE. The Python version multiplies `math.comb(n, k)` by powers directly,
 * which is fine at n = 17 or n = 40 but not here: a measured class holds 10,261 cells, and
 * `comb(10261, 5000)` overflows a float long before the powers underflow to compensate. Log
 * gamma keeps every term finite.
 */

/** Two-sided coverage. 0.95 leaves 2.5% in each tail. */
export const DEFAULT_CONFIDENCE = 0.95;

export interface ProportionInterval {
  /** count / total. */
  estimate: number;
  lower: number;
  upper: number;
}

/**
 * Log gamma via the Lanczos approximation (g = 7, n = 9).
 *
 * Accurate to roughly 15 significant digits across the range used here, which is far more
 * than a 40-pixel bar requires; the reason for the precision is that the bisection below
 * compares tail masses that can be very small.
 */
function logGamma(x: number): number {
  const coefficients = [
    0.99999999999980993, 676.5203681218851, -1259.1392167224028, 771.32342877765313,
    -176.61502916214059, 12.507343278686905, -0.13857109526572012, 9.9843695780195716e-6,
    1.5056327351493116e-7,
  ];
  if (x < 0.5) {
    // Reflection formula, so the approximation is only ever used above 0.5.
    return Math.log(Math.PI / Math.sin(Math.PI * x)) - logGamma(1 - x);
  }
  const shifted = x - 1;
  let series = coefficients[0];
  for (let index = 1; index < 9; index += 1) {
    series += coefficients[index] / (shifted + index);
  }
  const t = shifted + 7.5;
  return 0.5 * Math.log(2 * Math.PI) + (shifted + 0.5) * Math.log(t) - t + Math.log(series);
}

function logBinomialCoefficient(n: number, k: number): number {
  return logGamma(n + 1) - logGamma(k + 1) - logGamma(n - k + 1);
}

/** P(X <= k) for X ~ Binomial(n, p), summed in log space then exponentiated. */
function binomialCdf(k: number, n: number, p: number): number {
  if (p <= 0) {
    return 1;
  }
  if (p >= 1) {
    return k >= n ? 1 : 0;
  }
  const logP = Math.log(p);
  const logQ = Math.log1p(-p);
  let total = 0;
  for (let index = 0; index <= k; index += 1) {
    total += Math.exp(logBinomialCoefficient(n, index) + index * logP + (n - index) * logQ);
  }
  return Math.min(1, total);
}

/** P(X >= k) for X ~ Binomial(n, p). */
function binomialUpperTail(k: number, n: number, p: number): number {
  if (k <= 0) {
    return 1;
  }
  return Math.max(0, 1 - binomialCdf(k - 1, n, p));
}

/**
 * Bisection for the p where `tail(p)` crosses `target`.
 *
 * 200 iterations, matching the Python implementation, which is far past the point where a
 * double stops changing. Monotonicity of the binomial tail in p is what makes this valid.
 */
function bisect(tail: (p: number) => number, target: number, decreasing: boolean): number {
  let low = 0;
  let high = 1;
  for (let iteration = 0; iteration < 200; iteration += 1) {
    const mid = (low + high) / 2;
    const value = tail(mid);
    if (decreasing ? value > target : value < target) {
      low = mid;
    } else {
      high = mid;
    }
  }
  return decreasing ? high : low;
}

/**
 * The Clopper-Pearson interval for `count` successes in `total` trials.
 *
 * Edge cases are closed forms rather than bisection results, because they are the cases a
 * reader is most likely to misread: 0 of n is NOT a proportion of zero with no uncertainty,
 * and n of n is not certainty.
 */
export function proportionInterval(
  count: number,
  total: number,
  confidence: number = DEFAULT_CONFIDENCE,
): ProportionInterval {
  if (total <= 0) {
    // No evidence establishes nothing. The interval is the whole range, not a point at zero.
    return { estimate: 0, lower: 0, upper: 1 };
  }
  const clamped = Math.max(0, Math.min(total, count));
  const estimate = clamped / total;
  const alpha = 1 - confidence;

  const lower =
    clamped === 0
      ? 0
      : bisect((p) => binomialUpperTail(clamped, total, p), alpha / 2, false);
  const upper =
    clamped === total
      ? 1
      : bisect((p) => binomialCdf(clamped, total, p), alpha / 2, true);

  return { estimate, lower, upper };
}

/**
 * The one-sided upper bound, matching `clopper_pearson_upper` in the Python metrics module.
 *
 * Exposed separately because the honest headline for a zero-failure observation is an upper
 * bound, not a point estimate: 0 corruptions over 40 applied cells is a bound near 700 per
 * 10,000, which is the difference between "we measured no corruption" and "corruption is
 * impossible".
 */
export function upperBound(
  failures: number,
  trials: number,
  confidence: number = DEFAULT_CONFIDENCE,
): number {
  if (trials <= 0) {
    return 1;
  }
  if (failures >= trials) {
    return 1;
  }
  const alpha = 1 - confidence;
  if (failures === 0) {
    return 1 - alpha ** (1 / trials);
  }
  return bisect((p) => binomialCdf(failures, trials, p), alpha, true);
}
