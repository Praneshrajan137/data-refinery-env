"""Unit tests for the missing-value detector and FD-derivable repairer."""

from __future__ import annotations

import pandas as pd

from dataforge.cli.common import load_schema
from dataforge.detectors.missing_value import MissingValueDetector
from dataforge.repairers.missing_value import MissingValueRepairer


def _detect(df: pd.DataFrame, schema=None):  # noqa: ANN001
    return MissingValueDetector().detect(df, schema)


class TestMissingValueDetector:
    def test_flags_missing_in_populated_column(self) -> None:
        df = pd.DataFrame({"city": ["NY", "LA", "", "SF", "BOS", "DC", "LA", "NY"]})
        issues = _detect(df)
        assert [i.row for i in issues] == [2]
        assert issues[0].issue_type == "missing_value"

    def test_flags_sentinels(self) -> None:
        df = pd.DataFrame({"city": ["NY", "LA", "N/A", "SF", "BOS", "DC", "LA", "unknown"]})
        rows = sorted(i.row for i in _detect(df))
        assert rows == [2, 7]

    def test_ignores_sparse_column(self) -> None:
        # Mostly empty -> missingness is legitimate, not flagged.
        df = pd.DataFrame({"note": ["", "", "", "", "", "", "x", "y"]})
        assert _detect(df) == []


class TestMissingValueRepairer:
    def _schema(self, tmp_path):  # noqa: ANN001
        path = tmp_path / "schema.yaml"
        path.write_text(
            "columns:\n  zip: str\n  city: str\n"
            "functional_dependencies:\n  - determinant: [zip]\n    dependent: city\n",
            encoding="utf-8",
        )
        return load_schema(path)

    def test_fills_fd_derivable_value(self, tmp_path) -> None:  # noqa: ANN001
        schema = self._schema(tmp_path)
        # zip 10001 -> NY is known from other rows; the missing one is derivable.
        df = pd.DataFrame(
            {
                "zip": ["10001", "10001", "94105", "10001", "94105", "10001", "94105", "10001"],
                "city": ["NY", "NY", "SF", "", "SF", "NY", "SF", "NY"],
            }
        )
        issues = [i for i in _detect(df, schema) if i.row == 3]
        assert issues, "expected a missing_value issue at row 3"
        fix = MissingValueRepairer().propose(issues[0], df, schema)
        assert fix is not None
        assert fix.fix.new_value == "NY"

    def test_abstains_without_fd(self, tmp_path) -> None:  # noqa: ANN001
        # No schema/FD -> cannot derive -> detection-only (abstain).
        df = pd.DataFrame({"city": ["NY", "LA", "", "SF", "BOS", "DC", "LA", "NY"]})
        issues = [i for i in _detect(df) if i.row == 2]
        assert MissingValueRepairer().propose(issues[0], df, None) is None

    def test_abstains_when_fd_value_ambiguous(self, tmp_path) -> None:  # noqa: ANN001
        schema = self._schema(tmp_path)
        # zip 10001 maps to both NY and NJ -> ambiguous -> abstain.
        df = pd.DataFrame(
            {
                "zip": ["10001", "10001", "10001", "10001", "10001", "10001", "10001", "10001"],
                "city": ["NY", "NJ", "NY", "", "NJ", "NY", "NJ", "NY"],
            }
        )
        issues = [i for i in _detect(df, schema) if i.row == 3]
        assert MissingValueRepairer().propose(issues[0], df, schema) is None


class TestUnanimityIsWeakestWhereThePremiseIsMostLikelyFalse:
    """The mined-premise arm of this repairer had no test at all before 2026-08-25.

    `missing_value` is one of only two detectors permitted to auto-apply, and
    docs/trust/bypass-allowlist-evidence.md measured it at write precision 1.0000 on 427
    flights writes -- the strongest write result in the project. That measurement was taken
    under a DECLARED premise. Under a mined premise the same repairer inherits whatever
    dependencies the miner emitted, and docs/trust/premise-quality-result.md measured that
    16 of hospital's 85 mined dependencies are false on ground truth.

    The safety property is unanimity: `_lookup` fills only when every non-missing dependent
    value in the determinant group agrees. These tests encode why that guard is structurally
    weakest exactly where it is needed most -- a group holding a single non-missing witness
    is unanimous BY CONSTRUCTION, so a false dependency with sparse groups passes the guard
    trivially.

    The corrupting test below is a CHARACTERISATION test. It documents behaviour that is
    known-unsafe and shipped, because pretending the guard covers this case would be the
    more dangerous choice. Do not "fix" it by changing the assertion.

    **Why unit tests are the only possible evidence here.** Measured 2026-08-25, the mined
    arm of this repairer is unreachable on every corpus this project has, and for a different
    reason each time:

    | corpus | missing_value issues | mined FDs | columns that are both |
    | --- | --- | --- | --- |
    | hospital | **0** | 85 | 0 |
    | flights | 2370 | **0** | 0 |
    | rayyan | 1155 | **0** | 0 |
    | tax | 206 | 4 | **0** (no overlap) |

    So every one of the 427 writes behind this repairer's measured write precision of 1.0000
    came from the **declared** premise arm. Not one measured write in the project's history
    came from the mined arm. The path is not dead -- any user table holding both missing values
    and a minable dependency on the same column reaches it -- it is live and unmeasured, which
    is the worst combination and the reason these tests exist.
    """

    def _schema(self, tmp_path):  # noqa: ANN001, ANN202
        path = tmp_path / "schema.yaml"
        path.write_text(
            "columns:\n"
            "  zip: str\n"
            "  hospital: str\n"
            "functional_dependencies:\n"
            "  - determinant: [zip]\n"
            "    dependent: hospital\n",
            encoding="utf-8",
        )
        return load_schema(path)

    def test_a_single_witness_satisfies_unanimity_and_writes_on_that_evidence(
        self, tmp_path
    ) -> None:  # noqa: ANN001
        """The measured corruption mode, in miniature.

        `ZipCode -> HospitalName` is false: one zip code hosts several hospitals. It was
        nonetheless mined, and it accounted for 23 of the 25 sampled clean-cell corruptions.

        Row 3's hospital is missing and its determinant group holds exactly ONE non-missing
        witness, so unanimity is satisfied at a sample size of one and the repairer fills from
        that single neighbour.

        Note carefully what this fixture does and does not show. It does not show the value is
        wrong -- it shows the write rests on one row. And that is the whole difficulty: to
        satisfy unanimity the group must NOT contain visible disagreement, so **a false
        dependency is most dangerous precisely when the data has not yet revealed that it is
        false.** On hospital, where ground truth exists, writes of exactly this shape were
        checked and found wrong.
        """
        schema = self._schema(tmp_path)
        df = pd.DataFrame(
            {
                "zip": ["35201", "35201", "35202", "35201", "35202", "35202", "35203", "35203"],
                # Zip 35201 appears three times and only ONE of those rows records a
                # hospital, so the group row 3 is matched against contributes a single
                # witness and cannot exhibit disagreement.
                "hospital": [
                    "st vincents",
                    "",
                    "brookwood",
                    "",
                    "brookwood",
                    "brookwood",
                    "shelby",
                    "shelby",
                ],
            }
        )

        issues = [i for i in _detect(df, schema) if i.row == 3]
        assert issues, "expected a missing_value issue at row 3"
        fix = MissingValueRepairer().propose(issues[0], df, schema)

        assert fix is not None, (
            "the repairer filled this cell in the measured run; if it now abstains the "
            "guard has been strengthened, which is good news that belongs in a trust doc"
        )
        assert fix.fix.new_value == "st vincents"
        # The honest part: the only evidence for this write is ONE other row.
        group = df[(df["zip"] == "35201") & (df["hospital"] != "")]
        assert len(group) == 1, (
            "unanimity was satisfied by a single witness, so the guard is a majority of one"
        )

    def test_two_disagreeing_witnesses_do_make_it_abstain(self, tmp_path) -> None:  # noqa: ANN001
        """Non-vacuity: the guard is real once the group actually contains disagreement.

        This is the difference that matters. The guard is not broken -- it is sample-size
        blind, and a false dependency with sparse groups never gives it anything to see.
        """
        schema = self._schema(tmp_path)
        df = pd.DataFrame(
            {
                "zip": ["35201", "35201", "35201", "35201", "35202", "35202", "35203", "35203"],
                "hospital": [
                    "st vincents",
                    "brookwood",
                    "st vincents",
                    "",
                    "shelby",
                    "shelby",
                    "dch",
                    "dch",
                ],
            }
        )

        issues = [i for i in _detect(df, schema) if i.row == 3]
        assert issues
        assert MissingValueRepairer().propose(issues[0], df, schema) is None, (
            "two disagreeing witnesses must block the fill"
        )

    def test_a_missing_determinant_blocks_the_fill(self, tmp_path) -> None:  # noqa: ANN001
        """A cell whose determinant is itself missing has no group to be derived from."""
        schema = self._schema(tmp_path)
        df = pd.DataFrame(
            {
                "zip": ["35201", "35201", "35202", "", "35202", "35202", "35203", "35203"],
                "hospital": [
                    "st vincents",
                    "st vincents",
                    "brookwood",
                    "",
                    "brookwood",
                    "brookwood",
                    "shelby",
                    "shelby",
                ],
            }
        )

        issues = [i for i in _detect(df, schema) if i.row == 3]
        assert issues
        assert MissingValueRepairer().propose(issues[0], df, schema) is None
