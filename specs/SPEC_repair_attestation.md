# SPEC: repair-attestation

> Status: Accepted
> Owner: dataforge
> Last updated: 2026-08-13

## 1. Purpose

Define `dataforge.repair.attestation/v1`: a portable, versioned, signable statement that
a specific mutation to a specific dataset was verified and is reversible, checkable by a
third party who does not have DataForge installed and does not trust whoever produced
the file.

The product's thesis is that trust is mechanical. That only holds if the proof is
*consumable*. Today it is not: the receipt is dumped to stdout, reprojected lossily over
HTTP, wrapped a third way by the browser, carries no tool version or timestamp, embeds
none of the constraints it was verified against, is joined to the transaction journal by
a bare string, and is unsigned. `docs/STRATEGY.md` states the consequence plainly: *"A
certificate with a named consumer is a product; one without is a log line."*

This spec turns the log line into an artifact.

## 2. Outcomes

- [x] One normative wire format with a published JSON Schema and a stable
      `predicateType` URI. Schema: [repair_attestation.schema.json](repair_attestation.schema.json),
      **generated** from the verifier's own required-field tuples and the closed
      vocabularies by `scripts/ci/generate_attestation_schema.py`, and enforced by
      `make lint`. It was promised here and absent until 2026-08-29, while §6.2 below
      already cited it as the normative source of the enums.
- [ ] A **normative** verification tier that is pure-stdlib, solver-free, and
      reimplementable in any language from this document alone.
- [ ] Two independent conforming implementations (Python and TypeScript) that agree on
      every committed test vector, including every rejection case.
- [ ] Committed golden vectors covering acceptance and each distinct rejection reason.
- [ ] Fails closed: an unknown enum value, a missing required field, or an unrecognised
      version is a REJECT, never a pass-with-warning.
- [ ] Optional DSSE signing that binds the payload to a keyholder, with unsigned
      attestations reported as `unsigned` and never as authentic.

## 3. Scope

**IN**:

- The statement envelope, the predicate schema, and the normative check list.
- Canonicalisation and signing rules.
- Golden vectors and the cross-implementation conformance requirement.

**OUT**:

- **Key distribution and trust roots.** Signing proves a keyholder produced the payload.
  Deciding *which* keys to trust is deployment policy. No transparency log, no Sigstore
  or Rekor integration, no public root. Stating this is part of the promise: an
  attestation format that implied a trust root it does not have would be the same class
  of overclaim this project has corrected before.
- **Re-execution.** Re-running the SMT verifier is the advisory tier (section 7) and is
  explicitly not part of the normative spec.
- **Semantic correctness of the repair.** The attestation says a change was verified
  against stated constraints and is reversible. It does not say the new value is *true*.

## 4. Constraints

- Compatibility: Python `>=3.11,<3.13`. The normative verifier imports only the standard
  library plus `dataforge.domain.vocabulary` (itself stdlib-only).
- Structure: an [in-toto Statement v1] object. `subject` is content-addressed;
  `predicateType` identifies the schema.
- Signing: a [DSSE] envelope. `SIGNATURE = Sign(PAE(UTF8(PAYLOAD_TYPE), SERIALIZED_BODY))`
  where `PAE(type, body) = "DSSEv1" + SP + LEN(type) + SP + type + SP + LEN(body) + SP + body`.
- Safety: producing or verifying an attestation must never mutate user data.

## 5. Prior decisions

- 2026-08-13 (`DECISIONS.md`): the certificate carried a stale copy of the trust
  vocabulary. All closed vocabularies now come from `dataforge/domain/vocabulary.py`, and
  this format embeds them by reference to that single source.
- 2026-08-09 (`docs/trust/authority-is-mutable.md`): authority is per column, never per
  table. The predicate therefore records `authoritative_columns`, and a `proven` label on
  an untrusted provenance is credited only on a covered column.
- `docs/trust/certification-promises.md`: a certified threshold is a claim about a *set*,
  never about one cell. This format inherits that limit and does not soften it.

## 6. The format

### 6.1 Envelope

Unsigned, the artifact is a bare in-toto Statement:

```json
{
  "_type": "https://in-toto.io/Statement/v1",
  "subject": [
    { "name": "hospital.csv", "digest": { "sha256": "<post-state hex>" } }
  ],
  "predicateType": "https://dataforge.dev/RepairAttestation/v1",
  "predicate": { }
}
```

Signed, that Statement becomes the DSSE `SERIALIZED_BODY`:

```json
{
  "payload": "<base64(statement JSON)>",
  "payloadType": "application/vnd.in-toto+json",
  "signatures": [{ "keyid": "<hex>", "sig": "<base64>" }]
}
```

`subject` names the artifact the attestation is *about*: for an applied repair, the
repaired bytes; for a dry run, the unchanged source. Subjects are matched by digest, so
a renamed file still verifies.

**Why signing bytes and not canonical JSON.** DSSE signs the payload bytes it carries,
so there is no canonical-JSON rule to agree on. This is not incidental. The transaction
journal already demonstrates the failure mode: `dataforge/transactions/log.py` computes
its hash preimage with compact separators but writes the on-disk line with default
separators, so the bytes on disk are not the bytes hashed, and a reimplementation must
rediscover that by experiment. A format whose integrity depends on two parties
independently producing byte-identical JSON has a latent interoperability bug. Verifiers
MUST NOT re-parse the envelope after signature verification to obtain the payload.

### 6.2 Predicate

| Field | Type | Required | Notes |
|---|---|---|---|
| `attestation_version` | `"1"` | yes | Unrecognised value is a REJECT, not a warning. |
| `tool.name` | string | yes | `"dataforge"`. |
| `tool.version` | string | yes | The receipt has never carried this. Without it a reader cannot tell which verifier's semantics applied. |
| `tool.contract_version` | string | yes | The repair contract, distinct from this format's version. |
| `produced_at` | RFC 3339 UTC | yes | The receipt has no timestamp today. |
| `mode` | `"apply" \| "dry_run"` | yes | A dry run attests intent, not a mutation. |
| `applied` | bool | yes | |
| `reversible` | bool | yes | |
| `source.digest.sha256` | hex(64) | yes | Pre-state. |
| `post.digest.sha256` | hex(64) | when `applied` | Post-state; MUST equal the subject digest. |
| `authority.authoritative_columns` | string[] | yes | May be empty. Empty means only a deterministic fix can honestly be `proven`. |
| `authority.accepted_constraint_ids` | string[] | yes | |
| `authority.constraints_digest.sha256` | hex(64) \| null | yes | Digest of the constraint set. |
| `authority.constraints` | object \| null | yes | **The constraint set embedded in full.** Null only when no authority existed. |
| `fixes[]` | object[] | yes | One entry per applied cell; empty for a dry run. |
| `fixes[].row` | int >= 0 | yes | |
| `fixes[].column` | string | yes | |
| `fixes[].provenance` | `Provenance` | yes | Closed vocabulary. Unknown value is a REJECT. |
| `fixes[].verification_strength` | `VerificationStrength` | yes | **Not nullable.** Absent was previously read as proven. |
| `fixes[].detector_id` | string | yes | |
| `held[]` | object[] | yes | Held proposals, each with a `review_reason` from the closed vocabulary. |
| `verdicts.verifier` | `VerifierVerdict` | yes | |
| `verdicts.safety` | `SafetyVerdict` | yes | |
| `verdicts.independent_verification` | `"agreed" \| "not_run"` | yes | |
| `journal.txn_id` | string \| null | yes | |
| `journal.head_sha256` | hex(64) \| null | yes | Binds the hash chain INTO the signed payload. Previously the receipt referenced the journal only by name. |
| `revert_command` | string \| null | yes | Required when `applied`. |
| `model` | object \| null | yes | Provider and model identity when any fix is LLM-origin; null otherwise. |
| `limitations` | string[] | yes | Durable record of every downgrade. |
| `verification.checks_available` | string[] | yes | Which normative checks this producer expects to be runnable. |

Vocabularies (`Provenance`, `VerificationStrength`, `ReviewReason`, `VerifierVerdict`,
`SafetyVerdict`) are exactly those in `dataforge/domain/vocabulary.py`, published as
enums in [the JSON Schema](repair_attestation.schema.json) and generated into TypeScript.
They are not restated here, because restating a closed vocabulary is how three drifts
shipped.

Structural validity is **necessary and not sufficient**. The schema cannot express the
checks that carry the guarantee: re-deriving trust strength from provenance and column
authority, comparing the subject digest against the data, requiring every subject to name
the artifact the predicate describes, and verifying the DSSE signature. A document that
satisfies the schema and fails `verify_attestation` is expected, and the schema's own
`description` says so — a published schema otherwise invites "it validates, therefore it
is proven", which is the over-claim this spec exists to retire.

### 6.3 Constraints must be embedded, not referenced

`authority.constraints` carries the constraint set itself. A digest and an id list are
dangling pointers: today `reverify_certificate` must be handed the schema out-of-band as
`object | None`, and when it is absent the verifier **re-infers constraints from the
repaired data** and silently skips its cross-check. Inferring constraints from data that
was repaired to satisfy those constraints is circular, and a verifier that quietly
performs fewer checks reports the same `ok` for a weaker result. Embedding is what makes
independent verification possible at all.

## 7. Two tiers, named and separated

**Normative.** Pure stdlib, no solver, no inference, reimplementable from this document.
The checks, in order:

1. `envelope_recognised` - `_type` and `predicateType` are exactly the expected URIs.
2. `version_recognised` - `attestation_version` is `"1"`.
3. `schema_complete` - every required field present and well-typed.
4. `vocabulary_closed` - every enum value is a member of its published set.
5. `subject_matches_post_state` - subject digest equals `post.digest` when applied.
6. `data_identity` - SHA-256 of the supplied bytes equals the expected digest.
7. `reversibility_complete` - `applied` implies `reversible`, `txn_id`, `revert_command`.
8. `verdicts_accepting` - verifier `accept`, safety `allow`.
9. `strength_is_earned` - for every fix, strength derived from provenance and the
   column's authority; a recorded `plausibility_only` is always believed.
10. `constraints_present` - a `proven` untrusted fix requires embedded constraints.
11. `signature` - when signed, DSSE verification against a supplied public key.

Every check is reported individually. A verifier MUST NOT collapse "passed" and "skipped"
into one status, and MUST fail closed on anything unrecognised.

**Advisory.** Today's `reverify_certificate`: re-runs the real SMT and Direct verifiers
to re-derive ACCEPT per cell. Strictly stronger, and strictly **not** a spec — it depends
on z3, on `infer_verification_schema`, on two specific checkers, and on DataForge's CSV
round-trip. It is labelled advisory so nobody mistakes "DataForge agrees with itself" for
independent verification.

## 8. Verification

- `tests/unit/test_attestation.py` - build, sign, verify, and every rejection path.
- `tests/fixtures/attestation/*.json` - golden vectors, machine-independent (no absolute
  paths inside any signed payload; the journal's path-dependent hashes are a known
  weakness recorded in `docs/trust/`).
- `playground/web/src/attestation/verify.test.ts` - the TypeScript implementation over
  the same vectors.
- `scripts/ci/attestation_conformance.py --check` - runs both implementations over every
  vector and fails on any disagreement. This is the gate that makes this a spec rather
  than one program's behaviour.
- `scripts/ci/mutate_attestation.py` - mutation harness; every normative check must have
  a mutant that kills it.

## 9. Acceptance gate

- [ ] Both implementations agree on every vector, acceptance and rejection.
- [ ] Every normative check has a killing mutant.
- [ ] Unknown enum, unknown version, and missing field all REJECT.
- [ ] An unsigned attestation reports `unsigned`, never `verified`.
- [ ] A wrong-key signature REJECTS.
- [ ] `hospital` correction F1 unchanged at 0.7926.

## Appendix A - Toy cases

| Case | Expected |
|---|---|
| Deterministic fix, applied, no schema | verifies; `strength_is_earned` passes on provenance alone |
| `external` fix on a column in `authoritative_columns` | verifies |
| Same fix, column absent from `authoritative_columns` | REJECT (`strength_is_earned`) |
| `entity_consensus` fix, no authority, recorded `proven` | REJECT |
| Any fix recorded `plausibility_only` | REJECT while `applied` |
| `provenance: "some_future_corrector"` | REJECT (`vocabulary_closed`) |
| `attestation_version: "2"` | REJECT (`version_recognised`) |
| Payload byte flipped after signing | REJECT (`signature`) |
| Signed, no public key supplied | `signature` reported `unsigned`, overall not authentic |
| Subject digest not equal to `post.digest` | REJECT |

[in-toto Statement v1]: https://github.com/in-toto/attestation/blob/main/spec/v1/statement.md
[DSSE]: https://github.com/secure-systems-lab/dsse/blob/master/protocol.md
