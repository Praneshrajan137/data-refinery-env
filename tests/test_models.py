"""Tests for domain models."""

import pytest

from dataforge.models import (
    Column,
    ColumnType,
    Constraint,
    Issue,
    IssueType,
    IssueSeverity,
    ProposedFix,
    Schema,
    Transaction,
    TransactionOperation,
)


class TestIssue:
    """Test Issue model."""

    def test_issue_creation(self):
        """Test creating an issue."""
        issue = Issue(
            issue_type=IssueType.TYPE_MISMATCH,
            severity=IssueSeverity.ERROR,
            row=5,
            column="age",
            value="twenty",
            expected=20,
            message="Type mismatch: expected int, got str",
            detector="type_mismatch_detector",
        )

        assert issue.row == 5
        assert issue.column == "age"
        assert issue.severity == IssueSeverity.ERROR
        assert str(issue) == "type_mismatch at row 5, column 'age': Type mismatch: expected int, got str"

    def test_issue_immutable(self):
        """Test that issues are immutable."""
        issue = Issue(
            issue_type=IssueType.TYPE_MISMATCH,
            severity=IssueSeverity.ERROR,
            row=5,
            column="age",
            value="twenty",
            expected=20,
            message="Type mismatch",
            detector="detector",
        )

        with pytest.raises(Exception):  # FrozenInstanceError
            issue.row = 10


class TestColumn:
    """Test Column model."""

    def test_column_creation(self):
        """Test creating a column."""
        col = Column(
            name="age",
            type=ColumnType.INTEGER,
            nullable=False,
            cardinality=50,
        )

        assert col.name == "age"
        assert col.type == ColumnType.INTEGER
        assert col.nullable is False

    def test_column_with_constraints(self):
        """Test column with constraints."""
        constraint = Constraint(
            name="age_positive",
            type="check",
            columns=("age",),
            expression="age > 0",
        )

        col = Column(
            name="age",
            type=ColumnType.INTEGER,
            constraints=(constraint,),
        )

        assert len(col.constraints) == 1


class TestSchema:
    """Test Schema model."""

    def test_schema_creation(self):
        """Test creating a schema."""
        columns = {
            "id": Column(name="id", type=ColumnType.INTEGER),
            "name": Column(name="name", type=ColumnType.STRING),
        }

        schema = Schema(columns=columns)

        assert len(schema.columns) == 2
        assert schema.has_column("id")
        assert schema.get_column("name").type == ColumnType.STRING

    def test_schema_missing_column(self):
        """Test accessing missing column."""
        schema = Schema(columns={})
        assert schema.get_column("missing") is None


class TestProposedFix:
    """Test ProposedFix model."""

    def test_fix_creation(self):
        """Test creating a proposed fix."""
        fix = ProposedFix(
            row=5,
            column="age",
            original_value="twenty",
            proposed_value=20,
            confidence=0.95,
            reason="Type conversion",
            repair_type="type_cast",
        )

        assert fix.row == 5
        assert fix.confidence == 0.95
        assert "confidence: 95%" in str(fix)


class TestTransaction:
    """Test Transaction model."""

    def test_transaction_creation(self):
        """Test creating a transaction."""
        txn = Transaction(
            id="txn-123",
            timestamp="2024-01-01T12:00:00",
            operation=TransactionOperation.REPAIR,
            file_path="data.csv",
            changes=[(5, "age", "twenty", 20)],
        )

        assert txn.operation == TransactionOperation.REPAIR
        assert len(txn.changes) == 1
