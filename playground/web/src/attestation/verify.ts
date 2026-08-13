/**
 * Independent implementation of the NORMATIVE attestation verifier.
 *
 * Written from specs/SPEC_repair_attestation.md, sharing no code with the Python
 * reference implementation. That independence is the point: a format only becomes a
 * specification when a second implementation, written from the document rather than from
 * the code, reaches the same verdict on the same inputs. Until then it is one program's
 * behaviour with a version number attached.
 *
 * Both implementations run the committed vectors in tests/fixtures/attestation/vectors.json
 * and `scripts/ci/attestation_conformance.py` fails CI on any disagreement.
 *
 * The only shared artifact is the vocabulary, which is generated from the same source for
 * both languages -- and that is deliberate too. Re-typing a closed vocabulary by hand is
 * exactly what produced three drifts, one of them inside the certificate verifier.
 *
 * Signature verification is separated into an async function because Ed25519 requires
 * WebCrypto. Every other check is synchronous, pure, and dependency-free, which is what
 * makes this reimplementable in any language.
 */

import {
  PROVENANCE_ORDER,
  REVIEW_REASONS,
  SAFETY_VERDICTS,
  TRUSTED_PROVENANCE,
  VERIFICATION_STRENGTHS,
  VERIFIER_VERDICTS,
} from "../domain/vocabulary.generated";

export const STATEMENT_TYPE = "https://in-toto.io/Statement/v1";
export const PREDICATE_TYPE = "https://dataforge.dev/RepairAttestation/v1";
export const PAYLOAD_TYPE = "application/vnd.in-toto+json";
export const ATTESTATION_VERSION = "1";

const HEX64 = /^[0-9a-f]{64}$/;

export interface AttestationCheck {
  name: string;
  ok: boolean;
  detail: string;
  skipped: boolean;
}

export interface AttestationVerification {
  ok: boolean;
  checks: AttestationCheck[];
  failures: AttestationCheck[];
  skipped: AttestationCheck[];
}

function check(name: string, ok: boolean, detail: string, skipped = false): AttestationCheck {
  return { name, ok, detail, skipped };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function asArray(value: unknown): unknown[] {
  return Array.isArray(value) ? value : [];
}

function records(value: unknown): Record<string, unknown>[] {
  return asArray(value).filter(isRecord);
}

function strings(value: unknown): string[] {
  return asArray(value).map((entry) => String(entry));
}

/**
 * Derive strength from provenance and column authority.
 *
 * Reads the TRUSTED allowlist, never an untrusted denylist. The denylist form fails open:
 * a provenance nobody anticipated reads as trustworthy, which is how an `entity_consensus`
 * value was once reported as `proven`.
 */
function verificationStrengthFor(
  provenance: unknown,
  authoritativeSchemaPresent: boolean,
): "proven" | "plausibility_only" {
  const trusted = typeof provenance === "string" && TRUSTED_PROVENANCE.has(provenance);
  return trusted || authoritativeSchemaPresent ? "proven" : "plausibility_only";
}

function digestOf(value: unknown): string | null {
  if (!isRecord(value)) {
    return null;
  }
  const digest = value.digest;
  if (!isRecord(digest)) {
    return null;
  }
  const sha = digest.sha256;
  return typeof sha === "string" ? sha : null;
}

/**
 * Verify an attestation's normative tier, synchronously.
 *
 * Accepts a bare in-toto Statement. For a DSSE envelope, verify the signature with
 * `verifyDsseSignature` first and pass the payload it returns -- never re-parse the
 * envelope afterwards, which the DSSE spec explicitly forbids.
 */
export function verifyStatement(
  statement: unknown,
  options: { dataBytes?: Uint8Array | null; dataSha256?: string | null } = {},
): AttestationVerification {
  const checks: AttestationCheck[] = [];

  const finish = (): AttestationVerification => {
    const failures = checks.filter((entry) => !entry.ok && !entry.skipped);
    return {
      ok: failures.length === 0,
      checks,
      failures,
      skipped: checks.filter((entry) => entry.skipped),
    };
  };

  if (!isRecord(statement)) {
    checks.push(check("envelope_recognised", false, "statement is not an object"));
    return finish();
  }

  const typeOk = statement._type === STATEMENT_TYPE;
  const predicateTypeOk = statement.predicateType === PREDICATE_TYPE;
  checks.push(
    check(
      "envelope_recognised",
      typeOk && predicateTypeOk,
      `_type=${JSON.stringify(statement._type)} predicateType=${JSON.stringify(
        statement.predicateType,
      )}`,
    ),
  );
  if (!typeOk || !predicateTypeOk) {
    return finish();
  }

  const predicate = statement.predicate;
  if (!isRecord(predicate)) {
    checks.push(check("schema_complete", false, "predicate is not an object"));
    return finish();
  }

  const version = predicate.attestation_version;
  checks.push(
    check(
      "version_recognised",
      version === ATTESTATION_VERSION,
      `attestation_version=${JSON.stringify(version)}`,
    ),
  );
  if (version !== ATTESTATION_VERSION) {
    return finish();
  }

  // --- schema_complete -------------------------------------------------------
  const missing: string[] = [];
  for (const field of [
    "tool",
    "produced_at",
    "mode",
    "applied",
    "reversible",
    "source",
    "authority",
    "fixes",
    "held",
    "verdicts",
    "journal",
    "limitations",
  ]) {
    if (!(field in predicate)) {
      missing.push(field);
    }
  }
  const tool = predicate.tool;
  if (isRecord(tool)) {
    for (const field of ["name", "version"]) {
      const value = tool[field];
      if (typeof value !== "string" || value.length === 0) {
        missing.push(`tool.${field}`);
      }
    }
  } else {
    missing.push("tool");
  }
  const subject = asArray(statement.subject);
  if (subject.length === 0) {
    missing.push("subject");
  }
  checks.push(
    check(
      "schema_complete",
      missing.length === 0,
      missing.length === 0 ? "all required fields present" : `missing/invalid: ${missing}`,
    ),
  );

  // --- vocabulary_closed -----------------------------------------------------
  const provenances = new Set<string>(PROVENANCE_ORDER);
  const strengths = new Set<string>(VERIFICATION_STRENGTHS as readonly string[]);
  const reasons = new Set<string>(REVIEW_REASONS as readonly string[]);
  const verifierVerdicts = new Set<string>(VERIFIER_VERDICTS as readonly string[]);
  const safetyVerdicts = new Set<string>(SAFETY_VERDICTS as readonly string[]);

  const vocabularyProblems: string[] = [];
  for (const fix of records(predicate.fixes)) {
    if (typeof fix.provenance !== "string" || !provenances.has(fix.provenance)) {
      vocabularyProblems.push(`fix provenance ${JSON.stringify(fix.provenance)}`);
    }
    if (
      typeof fix.verification_strength !== "string" ||
      !strengths.has(fix.verification_strength)
    ) {
      vocabularyProblems.push(
        `fix verification_strength ${JSON.stringify(fix.verification_strength)}`,
      );
    }
  }
  for (const held of records(predicate.held)) {
    if (typeof held.review_reason !== "string" || !reasons.has(held.review_reason)) {
      vocabularyProblems.push(`held review_reason ${JSON.stringify(held.review_reason)}`);
    }
  }
  const verdicts = predicate.verdicts;
  if (isRecord(verdicts)) {
    if (typeof verdicts.verifier !== "string" || !verifierVerdicts.has(verdicts.verifier)) {
      vocabularyProblems.push(`verifier verdict ${JSON.stringify(verdicts.verifier)}`);
    }
    if (typeof verdicts.safety !== "string" || !safetyVerdicts.has(verdicts.safety)) {
      vocabularyProblems.push(`safety verdict ${JSON.stringify(verdicts.safety)}`);
    }
    if (
      verdicts.independent_verification !== "agreed" &&
      verdicts.independent_verification !== "not_run"
    ) {
      vocabularyProblems.push(
        `independent_verification ${JSON.stringify(verdicts.independent_verification)}`,
      );
    }
  } else {
    vocabularyProblems.push("verdicts is not an object");
  }
  if (predicate.mode !== "apply" && predicate.mode !== "dry_run") {
    vocabularyProblems.push(`mode ${JSON.stringify(predicate.mode)}`);
  }
  checks.push(
    check(
      "vocabulary_closed",
      vocabularyProblems.length === 0,
      vocabularyProblems.length === 0
        ? "every enum value is a published member"
        : `unrecognised values: ${vocabularyProblems}`,
    ),
  );

  // --- identity --------------------------------------------------------------
  const applied = predicate.applied === true;
  const sourceDigest = digestOf(predicate.source);
  const postDigest = digestOf(predicate.post);
  const expected = applied ? postDigest : sourceDigest;
  const subjectDigest = digestOf(subject[0]);

  checks.push(
    check(
      "subject_matches_post_state",
      typeof subjectDigest === "string" &&
        HEX64.test(subjectDigest) &&
        subjectDigest === expected,
      `subject=${JSON.stringify(subjectDigest)} expected=${JSON.stringify(expected)}`,
    ),
  );

  const suppliedDigest = options.dataSha256 ?? null;
  if (suppliedDigest === null) {
    checks.push(
      check(
        "data_identity",
        true,
        "no data supplied, so the digest claim was NOT checked",
        true,
      ),
    );
  } else {
    checks.push(
      check(
        "data_identity",
        suppliedDigest === expected,
        `sha256(data)=${suppliedDigest} expected=${JSON.stringify(expected)}`,
      ),
    );
  }

  // --- reversibility ---------------------------------------------------------
  if (!applied) {
    checks.push(
      check(
        "reversibility_complete",
        true,
        "dry run: nothing was written, so there is nothing to reverse",
      ),
    );
  } else {
    const journal = predicate.journal;
    const txnId = isRecord(journal) ? journal.txn_id : undefined;
    const problems: string[] = [];
    if (predicate.reversible !== true) {
      problems.push("reversible is false for an applied run");
    }
    if (typeof txnId !== "string" || txnId.length === 0) {
      problems.push("journal.txn_id is missing");
    }
    if (typeof predicate.revert_command !== "string" || predicate.revert_command.length === 0) {
      problems.push("revert_command is missing");
    }
    checks.push(
      check(
        "reversibility_complete",
        problems.length === 0,
        problems.length === 0 ? "applied run is fully reversible" : `${problems}`,
      ),
    );
  }

  // --- verdicts --------------------------------------------------------------
  if (!isRecord(verdicts)) {
    checks.push(check("verdicts_accepting", false, "verdicts missing"));
  } else if (!applied) {
    checks.push(
      check(
        "verdicts_accepting",
        verdicts.safety !== "deny",
        `dry run: safety=${JSON.stringify(verdicts.safety)}`,
      ),
    );
  } else {
    checks.push(
      check(
        "verdicts_accepting",
        verdicts.verifier === "accept" && verdicts.safety === "allow",
        `verifier=${JSON.stringify(verdicts.verifier)} safety=${JSON.stringify(
          verdicts.safety,
        )}`,
      ),
    );
  }

  // --- strength and constraints ---------------------------------------------
  const authority = predicate.authority;
  const covered = new Set<string>(
    isRecord(authority) ? strings(authority.authoritative_columns) : [],
  );
  const constraints = isRecord(authority) ? authority.constraints : null;

  const unproven: string[] = [];
  let needsConstraints = false;
  for (const fix of records(predicate.fixes)) {
    const column = typeof fix.column === "string" ? fix.column : "";
    const recorded = fix.verification_strength;
    if (recorded === "plausibility_only") {
      unproven.push(`${column} (recorded plausibility_only)`);
      continue;
    }
    const onAuthority = covered.has(column);
    const derived = verificationStrengthFor(fix.provenance, onAuthority);
    if (derived !== "proven") {
      unproven.push(`${column}:${JSON.stringify(fix.provenance)} (recorded ${JSON.stringify(recorded)})`);
    } else if (!(typeof fix.provenance === "string" && TRUSTED_PROVENANCE.has(fix.provenance))) {
      needsConstraints = true;
    }
  }
  checks.push(
    check(
      "strength_is_earned",
      !(applied && unproven.length > 0),
      applied && unproven.length > 0
        ? `unproven applied writes: ${unproven}`
        : "every applied fix is proven by provenance or by column authority",
    ),
  );

  if (needsConstraints) {
    const hasConstraints = isRecord(constraints) && Object.keys(constraints).length > 0;
    checks.push(
      check(
        "constraints_present",
        hasConstraints,
        hasConstraints
          ? "embedded constraints support the schema-proven writes"
          : "a write is proven only by schema authority, but no constraints are embedded",
      ),
    );
  } else {
    checks.push(check("constraints_present", true, "no write depends on schema authority"));
  }

  return finish();
}

/**
 * DSSE Pre-Authentication Encoding.
 *
 * `PAE(type, body) = "DSSEv1" + SP + LEN(type) + SP + type + SP + LEN(body) + SP + body`
 *
 * Lengths are byte lengths, so the type must be encoded before measuring.
 */
export function pae(payloadType: string, body: Uint8Array): Uint8Array {
  const encoder = new TextEncoder();
  const typeBytes = encoder.encode(payloadType);
  const prefix = encoder.encode(
    `DSSEv1 ${typeBytes.length} ${payloadType} ${body.length} `,
  );
  const out = new Uint8Array(prefix.length + body.length);
  out.set(prefix, 0);
  out.set(body, prefix.length);
  return out;
}

function base64ToBytes(value: string): Uint8Array {
  const binary = atob(value);
  const out = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1) {
    out[index] = binary.charCodeAt(index);
  }
  return out;
}

export interface DsseResult {
  check: AttestationCheck;
  statement: unknown | null;
}

/**
 * Verify a DSSE envelope and return the statement parsed from the VERIFIED bytes.
 *
 * Async because Ed25519 lives behind WebCrypto. When no key is supplied the signature is
 * reported as `unsigned` and skipped -- never as verified, because absence of a key is not
 * evidence of authenticity.
 */
export async function verifyDsseSignature(
  envelope: unknown,
  publicKeyRaw: Uint8Array | null,
): Promise<DsseResult> {
  if (!isRecord(envelope)) {
    return { check: check("signature", false, "envelope is not an object"), statement: null };
  }
  const payload = envelope.payload;
  const payloadType = envelope.payloadType;
  if (typeof payload !== "string" || typeof payloadType !== "string") {
    return {
      check: check("signature", false, "envelope is missing payload fields"),
      statement: null,
    };
  }
  if (payloadType !== PAYLOAD_TYPE) {
    return {
      check: check("signature", false, `unexpected payloadType ${payloadType}`),
      statement: null,
    };
  }

  let body: Uint8Array;
  let statement: unknown;
  try {
    body = base64ToBytes(payload);
    statement = JSON.parse(new TextDecoder().decode(body));
  } catch {
    return { check: check("signature", false, "payload is not valid base64 JSON"), statement: null };
  }

  if (publicKeyRaw === null) {
    return {
      check: check(
        "signature",
        true,
        "unsigned: no public key supplied, so authenticity was NOT established",
        true,
      ),
      statement,
    };
  }

  const signatures = asArray(envelope.signatures).filter(isRecord);
  if (signatures.length === 0) {
    return { check: check("signature", false, "envelope carries no signatures"), statement: null };
  }

  const key = await crypto.subtle.importKey(
    "raw",
    publicKeyRaw as BufferSource,
    { name: "Ed25519" },
    false,
    ["verify"],
  );
  const preauth = pae(PAYLOAD_TYPE, body);

  for (const entry of signatures) {
    if (typeof entry.sig !== "string") {
      continue;
    }
    let ok = false;
    try {
      ok = await crypto.subtle.verify(
        { name: "Ed25519" },
        key,
        base64ToBytes(entry.sig) as BufferSource,
        preauth as BufferSource,
      );
    } catch {
      ok = false;
    }
    if (ok) {
      return { check: check("signature", true, "DSSE signature verified"), statement };
    }
  }

  return {
    check: check("signature", false, "no signature verified against the key"),
    statement: null,
  };
}

/**
 * Verify either a bare Statement or a DSSE envelope.
 *
 * Composed so the synchronous normative core stays free of crypto: that core is the part
 * a third implementation would port first.
 */
export async function verifyAttestation(
  document: unknown,
  options: { dataSha256?: string | null; publicKeyRaw?: Uint8Array | null } = {},
): Promise<AttestationVerification> {
  if (isRecord(document) && "payload" in document && "payloadType" in document) {
    const { check: signatureCheck, statement } = await verifyDsseSignature(
      document,
      options.publicKeyRaw ?? null,
    );
    if (statement === null) {
      return {
        ok: false,
        checks: [signatureCheck],
        failures: [signatureCheck],
        skipped: [],
      };
    }
    const inner = verifyStatement(statement, { dataSha256: options.dataSha256 ?? null });
    const checks = [signatureCheck, ...inner.checks];
    const failures = checks.filter((entry) => !entry.ok && !entry.skipped);
    return {
      ok: failures.length === 0,
      checks,
      failures,
      skipped: checks.filter((entry) => entry.skipped),
    };
  }
  return verifyStatement(document, { dataSha256: options.dataSha256 ?? null });
}
