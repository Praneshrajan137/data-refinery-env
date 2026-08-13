import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { verifyAttestation, pae, PAYLOAD_TYPE } from "./verify";

/**
 * The TypeScript half of the conformance suite.
 *
 * These are the SAME committed vectors the Python implementation runs, and the assertions
 * check the same thing: not merely accept/reject, but WHICH named checks failed. An
 * implementation that rejects a vector via a different check has not implemented the same
 * specification, and a boolean-only assertion would hide that.
 */

const VECTORS_PATH = resolve(
  import.meta.dirname,
  "../../../../tests/fixtures/attestation/vectors.json",
);

interface VectorCase {
  description: string;
  expect_ok: boolean;
  expect_failures: string[];
  document: unknown;
}

interface VectorFile {
  format: string;
  test_public_key_hex: string;
  vectors: Record<string, VectorCase>;
}

const file: VectorFile = JSON.parse(readFileSync(VECTORS_PATH, "utf8"));

function hexToBytes(hex: string): Uint8Array {
  const out = new Uint8Array(hex.length / 2);
  for (let index = 0; index < out.length; index += 1) {
    out[index] = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16);
  }
  return out;
}

const PUBLIC_KEY = hexToBytes(file.test_public_key_hex);

describe("attestation conformance (TypeScript)", () => {
  it("loads a suite that is not vacuous", () => {
    // Mirrors the Python guard: a suite that decayed to a couple of happy paths would
    // pass everything below while proving nothing.
    const names = Object.keys(file.vectors);
    expect(names.length).toBeGreaterThanOrEqual(15);
    expect(names.filter((n) => file.vectors[n].expect_ok).length).toBeGreaterThanOrEqual(3);
    expect(names.filter((n) => !file.vectors[n].expect_ok).length).toBeGreaterThanOrEqual(10);
  });

  for (const name of Object.keys(file.vectors).sort()) {
    const testCase = file.vectors[name];
    it(`${name}`, async () => {
      const document = testCase.document as Record<string, unknown>;
      const signed = "payload" in document;
      const result = await verifyAttestation(document, {
        publicKeyRaw: signed ? PUBLIC_KEY : null,
      });

      expect(
        result.ok,
        `${name}: failures=${JSON.stringify(result.failures.map((f) => f.name))}`,
      ).toBe(testCase.expect_ok);

      const actual = result.failures.map((f) => f.name).sort();
      expect(actual, `${name}: rejected by the wrong checks`).toEqual(
        testCase.expect_failures,
      );
    });
  }
});

describe("DSSE encoding", () => {
  it("matches the published PAE test vector", () => {
    // From the DSSE spec's own example, so the encoding is right by construction rather
    // than by agreement with our own Python (which would be circular).
    const body = new TextEncoder().encode("hello world");
    const encoded = pae("http://example.com/HelloWorld", body);
    expect(new TextDecoder().decode(encoded)).toBe(
      "DSSEv1 29 http://example.com/HelloWorld 11 hello world",
    );
  });

  it("declares the in-toto payload type", () => {
    expect(PAYLOAD_TYPE).toBe("application/vnd.in-toto+json");
  });
});

describe("data identity", () => {
  it("is reported as skipped, never silently passed", async () => {
    const valid = file.vectors["valid-deterministic-applied"].document;
    const result = await verifyAttestation(valid);
    const names = result.skipped.map((c) => c.name);
    expect(names).toContain("data_identity");
    // Skipped must not make the whole verification fail either -- it is unknown, not bad.
    expect(result.ok).toBe(true);
  });

  it("rejects a digest that does not match the claim", async () => {
    const valid = file.vectors["valid-deterministic-applied"].document;
    const result = await verifyAttestation(valid, { dataSha256: "0".repeat(64) });
    expect(result.ok).toBe(false);
    expect(result.failures.map((f) => f.name)).toContain("data_identity");
  });
});
