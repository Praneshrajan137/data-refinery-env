"""Unit tests for label-derived SFT trajectory generation."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from dataforge.datasets.real_world import GroundTruthCell, RealWorldDataset
from scripts.data.build_oracle_sft_trajectories import (
    COLLECTION_METHOD,
    HOSPITAL_SYNTHETIC_DATASET,
    ORACLE_MODEL,
    ORACLE_PROVIDER,
    SYNTHETIC_COLLECTION_METHOD,
    OracleSettings,
    build_dataset_records,
    build_hospital_synthetic_records,
    build_split_manifest,
    deterministic_row_split,
)
from tests.support.corpora import build_fixture_metadata

FIXTURE_REVISION = "fixture"
FIXTURE_DIRTY_SHA256 = "d" * 64
FIXTURE_CLEAN_SHA256 = "c" * 64


def _dataset(name: str = "flights") -> RealWorldDataset:
    dirty_df = pd.DataFrame(
        {
            "src": ["aa", "aa", "aa", "aa"],
            "flight": ["1", "2", "3", "4"],
            "sched_dep_time": ["", "Fri Dec 2 5:11 a.m.", "", ""],
            "act_dep_time": ["7:14 p.m.", "11:08 p.m. On Time", "8:00 a.m.", "9:00 a.m."],
            "sched_arr_time": ["", "", "", ""],
            "act_arr_time": ["10:29 p.m.", "Delayed", "9:00 a.m.", "10:00 a.m."],
        }
    )
    clean_df = pd.DataFrame(
        {
            "src": ["aa", "aa", "aa", "aa"],
            "flight": ["1", "2", "3", "4"],
            "sched_dep_time": ["7:00 p.m.", "5:11 a.m.", "", ""],
            "act_dep_time": ["7:14 p.m.", "11:08 p.m.", "8:00 a.m.", "9:00 a.m."],
            "sched_arr_time": ["10:15 p.m.", "", "", ""],
            "act_arr_time": ["10:29 p.m.", "Delayed", "9:00 a.m.", "10:00 a.m."],
        }
    )
    return RealWorldDataset(
        metadata=build_fixture_metadata(
            name=name,
            domain="aviation",
            n_rows=len(dirty_df.index),
            n_columns=len(dirty_df.columns),
            error_types=("missing_value", "formatting"),
            source_urls=("dirty", "clean"),
            source_revision=FIXTURE_REVISION,
            dirty_sha256=FIXTURE_DIRTY_SHA256,
            clean_sha256=FIXTURE_CLEAN_SHA256,
            citation="fixture citation",
        ),
        dirty_df=dirty_df,
        clean_df=clean_df,
        canonical_columns=tuple(str(column) for column in clean_df.columns),
        ground_truth=(
            GroundTruthCell(
                row=0,
                column="sched_dep_time",
                dirty_value="",
                clean_value="7:00 p.m.",
            ),
            GroundTruthCell(
                row=0,
                column="sched_arr_time",
                dirty_value="",
                clean_value="10:15 p.m.",
            ),
            GroundTruthCell(
                row=1,
                column="sched_dep_time",
                dirty_value="Fri Dec 2 5:11 a.m.",
                clean_value="5:11 a.m.",
            ),
            GroundTruthCell(
                row=1,
                column="act_dep_time",
                dirty_value="11:08 p.m. On Time",
                clean_value="11:08 p.m.",
            ),
        ),
        dirty_sha256=FIXTURE_DIRTY_SHA256,
        clean_sha256=FIXTURE_CLEAN_SHA256,
    )


def _hospital_dataset() -> RealWorldDataset:
    dirty_df = pd.DataFrame(
        {
            "ProviderNumber": ["10018", "10019", "10020", "10021"],
            "HospitalName": ["Alpha", "Beta", "Gamma", "Delta"],
            "City": ["birmingham", "sheffield", "mobile", "selma"],
            "State": ["al", "al", "al", "al"],
            "ZipCode": ["35233", "35660", "36601", "36701"],
            "PhoneNumber": ["2053258100", "2563861600", "2515550100", "3345550101"],
            "EmergencyService": ["yes", "no", "yes", "no"],
        }
    )
    clean_df = dirty_df.copy()
    return RealWorldDataset(
        metadata=build_fixture_metadata(
            name="hospital",
            domain="healthcare",
            n_rows=len(dirty_df.index),
            n_columns=len(dirty_df.columns),
            error_types=("synthetic_fixture",),
            source_urls=("dirty", "clean"),
            source_revision=FIXTURE_REVISION,
            dirty_sha256=FIXTURE_DIRTY_SHA256,
            clean_sha256=FIXTURE_CLEAN_SHA256,
            citation="fixture citation",
        ),
        dirty_df=dirty_df,
        clean_df=clean_df,
        canonical_columns=tuple(str(column) for column in clean_df.columns),
        ground_truth=(),
        dirty_sha256=FIXTURE_DIRTY_SHA256,
        clean_sha256=FIXTURE_CLEAN_SHA256,
    )


def test_oracle_records_use_exact_dirty_clean_labels_without_teacher_discovery() -> None:
    records = build_dataset_records(
        _dataset(),
        difficulty="easy",
        split_seed=7,
        eval_fraction=0.25,
        min_eval_rows=1,
        chunk_rows=2,
        context_window_rows=1,
    )

    assert records
    for record in records:
        assert record["teacher"] == {"provider": ORACLE_PROVIDER, "model": ORACLE_MODEL}
        assert record["provenance"]["collection_method"] == COLLECTION_METHOD
        assert record["metrics"]["llm_calls"] == 0
        assert record["state"]["split"] == "train"
        assert record["state"]["normalization_candidates"] == []
        user_payload = json.loads(record["messages"][1]["content"])
        assert "normalization_candidates" not in user_payload
        assistant_payload = json.loads(record["messages"][-1]["content"])
        assert assistant_payload["action"] == "submit_repairs"
        assert assistant_payload["repairs"] == record["fix"]

    fixes = {
        (fix["row"], fix["column"], fix["new_value"]) for record in records for fix in record["fix"]
    }
    eval_rows = set(records[0]["provenance"]["eval_rows"])
    expected = {
        (cell.row, cell.column, cell.clean_value)
        for cell in _dataset().ground_truth
        if cell.row not in eval_rows
    }
    assert fixes == expected


def test_oracle_records_exclude_eval_rows_from_targets_context_and_fixes() -> None:
    dataset = _dataset()
    split = deterministic_row_split(
        dataset_name=dataset.metadata.name,
        n_rows=len(dataset.dirty_df.index),
        split_seed=3,
        eval_fraction=0.25,
        min_eval_rows=1,
    )

    records = build_dataset_records(
        dataset,
        difficulty="medium",
        split_seed=3,
        eval_fraction=0.25,
        min_eval_rows=1,
        chunk_rows=1,
        context_window_rows=10,
    )

    eval_rows = set(split.eval_rows)
    for record in records:
        state = record["state"]
        target_rows = {int(row["_row"]) for row in state["target_rows"]}
        context_rows = {int(row["_row"]) for row in state["context_rows"]}
        fix_rows = {fix["row"] for fix in record["fix"]}
        assert not eval_rows.intersection(target_rows)
        assert not eval_rows.intersection(context_rows)
        assert not eval_rows.intersection(fix_rows)
        assert state["normalization_candidates"] == []


def test_oracle_records_do_not_expose_clean_label_fields_in_user_visible_state() -> None:
    records = build_dataset_records(
        _dataset(),
        difficulty="easy",
        split_seed=7,
        eval_fraction=0.25,
        min_eval_rows=1,
        chunk_rows=2,
        context_window_rows=1,
    )

    for record in records:
        user_visible = json.dumps(
            {
                "state": record["state"],
                "user_message": record["messages"][1],
            },
            sort_keys=True,
        )
        assert record["state"]["normalization_candidates"] == []
        assert "suggested_value" not in user_visible
        assert "new_value" not in user_visible
        assert "repairs" not in user_visible


def test_oracle_builder_can_emit_noop_finish_examples() -> None:
    records = build_dataset_records(
        _dataset(),
        difficulty="easy",
        split_seed=7,
        eval_fraction=0.25,
        min_eval_rows=1,
        chunk_rows=1,
        context_window_rows=0,
        include_noop_records=True,
    )

    noop_records = [record for record in records if not record["fix"]]

    assert noop_records
    for record in noop_records:
        assistant_payload = json.loads(record["messages"][-1]["content"])
        assert assistant_payload == {"action": "finish", "repairs": []}
        assert record["metrics"]["chunk_f1"] == 1.0


def test_expert_v4_emits_only_deterministic_repairs_and_abstentions() -> None:
    records = build_dataset_records(
        _dataset(),
        difficulty="easy",
        split_seed=0,
        eval_fraction=0.25,
        min_eval_rows=1,
        chunk_rows=1,
        context_window_rows=1,
        include_noop_records=True,
        schema_version="expert_v4",
        prompt_contract_version="repair_contract_v2",
        abstain_noninferable=True,
        include_context_derivable=False,
    )

    assert records
    by_label = {}
    for record in records:
        by_label.setdefault(record["inferability"], []).append(record)
        assistant_payload = json.loads(record["messages"][-1]["content"])
        assert assistant_payload["repairs"] == record["fix"]

    assert "deterministic_normalization" in by_label
    assert "external_reference_required" in by_label
    assert "not_inferable_from_prompt" in by_label
    assert all(record["fix"] for record in by_label["deterministic_normalization"])
    assert all(not record["fix"] for record in by_label["external_reference_required"])
    assert all(not record["fix"] for record in by_label["not_inferable_from_prompt"])
    assert all(
        json.loads(record["messages"][-1]["content"]) == {"action": "finish", "repairs": []}
        for label in ("external_reference_required", "not_inferable_from_prompt")
        for record in by_label[label]
    )


def test_hospital_synthetic_records_are_split_safe_and_provenance_marked() -> None:
    dataset = _hospital_dataset()
    split = deterministic_row_split(
        dataset_name="hospital",
        n_rows=len(dataset.dirty_df.index),
        split_seed=7,
        eval_fraction=0.25,
        min_eval_rows=1,
    )

    records = build_hospital_synthetic_records(
        dataset,
        difficulty="easy",
        split_seed=7,
        eval_fraction=0.25,
        min_eval_rows=1,
        records_per_difficulty=4,
        schema_version="expert_v4",
        prompt_contract_version="repair_contract_v2",
    )

    assert records
    eval_rows = set(split.eval_rows)
    for record in records:
        assert record["dataset"] == HOSPITAL_SYNTHETIC_DATASET
        assert record["provenance"]["collection_method"] == SYNTHETIC_COLLECTION_METHOD
        assert record["provenance"]["base_dataset"] == "hospital"
        assert record["state"]["schema_summary"]["base_dataset"] == "hospital"
        assert record["inferability"] == "deterministic_normalization"
        exposed_rows = {int(row["_row"]) for row in record["state"]["target_rows"]}
        exposed_rows.update(fix["row"] for fix in record["fix"])
        assert not exposed_rows & eval_rows
        user_payload = json.loads(record["messages"][1]["content"])
        assert user_payload["label_source"] == SYNTHETIC_COLLECTION_METHOD
        assistant_payload = json.loads(record["messages"][-1]["content"])
        assert assistant_payload["repairs"] == record["fix"]


def test_oracle_trajectory_ids_are_stable() -> None:
    kwargs = {
        "difficulty": "easy",
        "split_seed": 11,
        "eval_fraction": 0.25,
        "min_eval_rows": 1,
        "chunk_rows": 2,
        "context_window_rows": 1,
    }

    first = build_dataset_records(_dataset("flights"), **kwargs)
    second = build_dataset_records(_dataset("flights"), **kwargs)

    assert [record["trajectory_id"] for record in first] == [
        record["trajectory_id"] for record in second
    ]


def test_split_manifest_contains_only_dirty_row_hashes(monkeypatch) -> None:
    dataset = _dataset("flights")

    def fake_loader(name, *, cache_root=None):
        assert name == "flights"
        return dataset

    monkeypatch.setattr(
        "scripts.data.build_oracle_sft_trajectories.load_real_world_dataset",
        fake_loader,
    )
    settings = OracleSettings(
        datasets=("flights",),
        difficulties=("easy",),
        split_seed=7,
        eval_fraction=0.25,
        min_eval_rows=1,
        chunk_rows=2,
        context_window_rows=1,
        include_noop_records=True,
        schema_version="expert_v1",
        prompt_contract_version="repair_contract_v1",
        max_repairs_per_record=None,
        min_noop_ratio=0.0,
        ready_min_records=2,
        output=Path("unused.jsonl"),
        manifest_output=Path("unused_manifest.json"),
        overwrite=True,
    )

    manifest = build_split_manifest(settings)

    assert manifest["schema_version"] == "split_manifest_v1"
    assert manifest["collection_method"] == COLLECTION_METHOD
    assert manifest["datasets"][0]["train_rows"] == 3
    assert manifest["datasets"][0]["eval_rows"] == 1
    assert manifest["datasets"][0]["source_revision"] == FIXTURE_REVISION
    assert manifest["datasets"][0]["dirty_sha256"] == FIXTURE_DIRTY_SHA256
    assert manifest["datasets"][0]["clean_sha256"] == FIXTURE_CLEAN_SHA256
    assert manifest["datasets"][0]["ground_truth_cells"] == len(dataset.ground_truth)
    serialized = json.dumps(manifest, sort_keys=True)
    assert "clean_value" not in serialized
    assert "new_value" not in serialized
    assert "suggested_value" not in serialized
    for split_name in ("train", "eval"):
        for row in manifest["datasets"][0][split_name]:
            assert set(row) == {"row", "dirty_row_sha256"}
            assert len(row["dirty_row_sha256"]) == 64


def test_split_manifest_tracks_synthetic_hospital_without_labels(monkeypatch) -> None:
    dataset = _hospital_dataset()

    def fake_loader(name, *, cache_root=None):
        assert name == "hospital"
        return dataset

    monkeypatch.setattr(
        "scripts.data.build_oracle_sft_trajectories.load_real_world_dataset",
        fake_loader,
    )
    settings = OracleSettings(
        datasets=("hospital",),
        difficulties=("easy",),
        split_seed=7,
        eval_fraction=0.25,
        min_eval_rows=1,
        chunk_rows=2,
        context_window_rows=1,
        include_noop_records=True,
        schema_version="expert_v4",
        prompt_contract_version="repair_contract_v2",
        max_repairs_per_record=None,
        min_noop_ratio=0.0,
        ready_min_records=2,
        output=Path("unused.jsonl"),
        manifest_output=Path("unused_manifest.json"),
        overwrite=True,
        include_hospital_synthetic=True,
        hospital_synthetic_records_per_difficulty=4,
    )

    manifest = build_split_manifest(settings)
    by_dataset = {entry["dataset"]: entry for entry in manifest["datasets"]}

    assert HOSPITAL_SYNTHETIC_DATASET in by_dataset
    synthetic = by_dataset[HOSPITAL_SYNTHETIC_DATASET]
    assert synthetic["base_dataset"] == "hospital"
    assert synthetic["collection_method"] == SYNTHETIC_COLLECTION_METHOD
    serialized = json.dumps(synthetic, sort_keys=True)
    assert "clean_value" not in serialized
    assert "new_value" not in serialized
    assert "suggested_value" not in serialized
