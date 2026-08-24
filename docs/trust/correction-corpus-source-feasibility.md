# Corpus-source feasibility: measured, and it reorders the plan

Measured 2026-08-24. No API spend; these are public archives.

The plan for closing the corrector gap named ClinicalTrials.gov version history as the primary
source and SEC XBRL as secondary. **Measurement reverses that.** This document records what was
measured, because the reversal rests on numbers rather than preference.

## ClinicalTrials.gov: works, but the economics do not

The endpoint is real and returns exactly what is needed. `/api/int/studies/{nct}/history` gives a
per-version change list with the modules touched, and `/api/int/studies/{nct}/history/{n}` returns
the **full structured record at that version** -- enrollment counts, dates, statuses, postal codes,
geo-points. Field-level diffing across versions is entirely possible.

Four measured obstacles:

| finding | measurement |
| --- | --- |
| The endpoint is undocumented | `/api/int/` is absent from the published OpenAPI v2 spec, which exposes only the latest version |
| A WAF rejects custom User-Agents | `User-Agent: dataforge-research/1.0` returns **HTTP 403**; httpx's default UA returns 200. A spoofed browser UA also returns 403 |
| Rate limited near 1 request/second | 12 studies at 0.3s spacing failed partway with a non-JSON body; 8 studies at 1.0s spacing returned 200 throughout |
| Most studies have very few versions | Sampled studies showed 1-3 versions each; a single-version study yields no changes at all |

The economics follow from the last two. A usable correction corpus needs on the order of hundreds
of *corrected* cells. With few versions per study, several field changes per version pair, and only
a minority of changes being error corrections, that implies thousands of studies, each costing one
history call plus one call per version -- **roughly 10⁴ requests at ~1 req/s, so hours of
wall-clock time against an undocumented endpoint that rate-limits and blocks identifying itself
honestly.**

The last point is the one that decides it. Being unable to send a descriptive User-Agent means
either misrepresenting the client or relying on a library default that could change. Neither is a
foundation for a corpus this project would cite.

## SEC XBRL: one request buys the whole quarter

| finding | measurement |
| --- | --- |
| Bulk archives, stable URLs | 2009 Q1 through 2026 Q1, one ZIP per quarter |
| Descriptive User-Agent is required and accepted | SEC's access policy asks for a contact address, and supplying one works |
| Data per request | 2024 Q1 is 124 MB and holds **6,028 submissions**; 2009 Q2 is 145 KB, useful for cheap format validation |
| Amendments are directly identifiable | `sub.txt` carries `form`, so `10-K/A` (63) and `10-Q/A` (81) are a simple filter -- **144 relevant amendments per quarter** |
| SEC marks superseded filings itself | `prevrpt=1` on **309** submissions in 2024 Q1 |
| Cross-quarter joins are necessary | only **83 of 370** amendments have their original in the same quarter, because an amendment usually revises an earlier period |

Structure: `sub.txt` (submissions), `num.txt` (numeric facts keyed by `adsh, tag, version, ddate,
qtrs, uom, segments, coreg`), `pre.txt` (presentation), `tag.txt` (taxonomy).

A restatement is a later submission reporting a different `value` for the same `(cik, tag, ddate,
qtrs, uom)`. That is a join, not a crawl.

## Why this source is also better on the merits, not just cheaper

SEC facts are **numeric**, and numeric is precisely where this project has no positive evidence at
all. `docs/trust/cell-level-detection-result.md` records `OutlierDetector` and
`DecimalShiftDetector` with **zero true positives on every corpus measured** -- rayyan, hospital and
flights alike. `decimal_shift` was removed from `CONSTRAINT_CHECKABLE_DETECTORS` after 263,428 false
rewrites on TPC-H money columns.

A financial figure restated by a factor of ten is a decimal-shift error with an auditor-verified
correction. **This would be the first corpus capable of confirming or refuting those two
detectors**, rather than only accumulating evidence against them.

The cost is narrowness: numeric only, so no dates, categoricals or formats. A broad corpus that
takes hours to fetch and might yield a few dozen cells is worth less than a narrow one that exists.

## The validity problem this does not solve

**Not every restatement is an error.** Reclassification between line items, adoption of a new
accounting standard, and discontinued-operations restatements all change a number without anyone
having been wrong. The reason lives in the filing narrative, not in `num.txt`.

This is the same problem ClinicalTrials.gov would have posed as legitimate-update versus
correction, and switching source does not avoid it. It is why the corpus needs the three-way change
classification the plan specifies, with **mechanical, pre-registered** rules, and why a random
unfiltered holdout is mandatory.

The circularity risk is the sharp one and is recorded in
`docs/trust/constraint-circularity.md` already: selecting changes by error signatures drawn from
DataForge's own taxonomy -- power-of-ten ratios, digit transpositions, sign flips -- builds a corpus
guaranteed to favour DataForge's detectors. A corpus assembled that way could "confirm"
`decimal_shift` by construction, which would be worse than the current position of having no
evidence for it.

## Decision

**SEC XBRL becomes the primary source. ClinicalTrials.gov is deferred, not abandoned**, and remains
the right second source for type breadth if the User-Agent and rate-limit constraints can be
resolved -- for instance via the CTTI AACT database, which republishes ClinicalTrials.gov in bulk
and would remove the per-study crawl entirely. That route is untested here.

**The corpus is not built in this pass.** What stopped it is not the download but the
classification: the rules that separate a corrected error from a legitimate accounting change are
the entire validity of the corpus, and they must be written and pre-registered carefully rather
than assembled alongside a multi-quarter join at the end of a long session. Building the fetch
first and the rules second is how the circularity trap gets sprung.

Recorded so the next pass starts from measurements rather than repeating them.
