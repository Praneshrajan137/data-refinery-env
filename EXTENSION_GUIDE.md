# DataForge15 Extension Guide

This guide shows how to extend DataForge15 with custom detectors and repairers.

## Quick Start: Adding a Custom Detector

### Step 1: Create a Detector Class

```python
# dataforge/detectors/my_detector.py

from dataforge.core import Detector
from dataforge.models import Issue, IssueType, IssueSeverity
from dataforge.types import TableData

class MyCustomDetector(Detector):
    """Detects my custom data quality issues."""

    name = "my_custom"
    description = "Detects values that violate my custom rule"

    def detect(self, data: TableData, schema=None) -> list[Issue]:
        """
        Detect issues according to my custom logic.
        
        Args:
            data: List of row dictionaries
            schema: Optional schema
            
        Returns:
            List of detected issues
        """
        issues = []
        
        for row_idx, row in enumerate(data):
            for col_name, value in row.items():
                # Your detection logic here
                if self._is_problematic(col_name, value):
                    issue = Issue(
                        issue_type=IssueType.TYPE_MISMATCH,  # or other type
                        severity=IssueSeverity.WARNING,
                        row=row_idx,
                        column=col_name,
                        value=value,
                        expected="valid value",
                        message=f"Column '{col_name}' has problematic value: {value}",
                        detector=self.name,
                        context={"rule": "my_custom_rule"},
                    )
                    issues.append(issue)
        
        return issues
    
    def _is_problematic(self, column: str, value) -> bool:
        """Check if value violates the rule."""
        # Implement your detection logic
        return False
```

### Step 2: Register Your Detector

```python
# In your application initialization code:

from dataforge.detectors.my_detector import MyCustomDetector
from dataforge.core import DetectorRegistry

detector = MyCustomDetector()
DetectorRegistry.register(detector)

# Now when you run profile/repair, your detector will be included
```

### Step 3: Use It in Your Code

```python
from dataforge.services import ProfileService
from dataforge.config import DataForgeConfig

# Your detector is automatically used
config = DataForgeConfig(
    detectors__enabled_detectors=["my_custom", "type_mismatch"]
)
profiler = ProfileService(config)
result = profiler.profile_file("data.csv")
```

## Adding a Custom Repairer

### Step 1: Create a Repairer Class

```python
# dataforge/repairers/my_repairer.py

from dataforge.core import Repairer
from dataforge.models import Issue, ProposedFix
from dataforge.types import TableData

class MyCustomRepairer(Repairer):
    """Repairs issues detected by my custom detector."""

    name = "my_custom_repairer"
    description = "Applies my custom repair strategy"

    def repair(self, data: TableData, issues: list[Issue], schema=None) -> list[ProposedFix]:
        """
        Propose repairs for detected issues.
        
        Args:
            data: Original data
            issues: Issues detected by detectors
            schema: Optional schema
            
        Returns:
            List of proposed fixes
        """
        fixes = []
        
        # Filter to only issues from my detector
        my_issues = [i for i in issues if i.detector == "my_custom"]
        
        for issue in my_issues:
            # Your repair logic here
            proposed_value = self._compute_repair(
                data[issue.row], issue.column, issue.value
            )
            
            fix = ProposedFix(
                row=issue.row,
                column=issue.column,
                original_value=issue.value,
                proposed_value=proposed_value,
                confidence=0.85,  # How confident are you?
                reason="Applied my custom repair strategy",
                repair_type="my_custom_repair",
            )
            fixes.append(fix)
        
        return fixes
    
    def _compute_repair(self, row: dict, column: str, value) -> any:
        """Compute the repair for a value."""
        # Implement your repair logic
        return value
```

### Step 2: Register Your Repairer

```python
from dataforge.repairers.my_repairer import MyCustomRepairer
from dataforge.core import RepairerRegistry

repairer = MyCustomRepairer()
RepairerRegistry.register(repairer)
```

## Building a Custom Pipeline

You can also create complete custom workflows:

```python
from dataforge.services import ProfileService, RepairEngine
from dataforge.io import CSVReader, CSVWriter, SchemaStore
from dataforge.config import DataForgeConfig

# Setup
config = DataForgeConfig(input_csv="data.csv")
reader = CSVReader()
writer = CSVWriter()
schema_mgr = SchemaManager()

# 1. Load data
data = reader.read("data.csv")

# 2. Load or infer schema
schema = schema_mgr.load_schema("schema.json")

# 3. Run custom detection
profiler = ProfileService(config)
profile = profiler.profile_data(data, schema)

# 4. Filter issues by severity
critical_issues = profile.get_issues_by_severity("critical")

# 5. Apply custom repair
repair_engine = RepairEngine(config)
plan = repair_engine.plan_repairs(data, critical_issues, schema)
outcome = repair_engine.apply_repairs(data, plan)

# 6. Save results
if outcome.success:
    writer.write("repaired.csv", outcome.repaired_data)
```

## Advanced: Custom Verifier

```python
from dataforge.core import Verifier
from dataforge.models import ProposedFix, VerificationResult

class MyCustomVerifier(Verifier):
    """Custom verification logic using my rule system."""

    def verify(self, data, proposed_fix, schema=None) -> VerificationResult:
        """Verify a repair using custom logic."""
        
        # Apply fix to copy of data
        test_data = [row.copy() for row in data]
        test_data[proposed_fix.row][proposed_fix.column] = proposed_fix.proposed_value
        
        # Check constraints
        is_valid = self._check_constraints(test_data, schema)
        
        return VerificationResult(
            valid=is_valid,
            proposed_fix=proposed_fix,
            reason="Passed custom constraints" if is_valid else "Failed custom constraints",
        )
```

## Testing Your Extensions

```python
# tests/test_my_detector.py

import pytest
from dataforge.detectors.my_detector import MyCustomDetector
from dataforge.models import IssueType

def test_my_detector_finds_issues():
    detector = MyCustomDetector()
    
    data = [
        {"id": "1", "value": "valid"},
        {"id": "2", "value": "invalid"},  # Should trigger
    ]
    
    issues = detector.detect(data)
    
    assert len(issues) >= 1
    assert any(i.row == 1 for i in issues)

def test_my_detector_empty_data():
    detector = MyCustomDetector()
    issues = detector.detect([])
    assert issues == []
```

## Best Practices

### 1. **Input Validation**
```python
def detect(self, data, schema=None):
    if not data:
        return []  # Handle empty data gracefully
    
    if schema is None:
        # Handle missing schema
        return []
```

### 2. **Error Handling**
```python
def detect(self, data, schema=None):
    issues = []
    
    for row_idx, row in enumerate(data):
        try:
            # Detection logic
            pass
        except Exception as e:
            # Log but don't fail entire detection
            print(f"Error in row {row_idx}: {e}")
            continue
    
    return issues
```

### 3. **Performance**
```python
def detect(self, data, schema=None):
    # For large datasets, limit processing
    sample = data[:min(10000, len(data))]
    
    issues = []
    for row_idx, row in enumerate(sample):
        # Detection logic
        pass
    
    return issues
```

### 4. **Confidence Scoring**
```python
# Be realistic about confidence levels
fix = ProposedFix(
    row=row_idx,
    column=col_name,
    original_value=old,
    proposed_value=new,
    confidence=0.95,  # Very high confidence
    reason="Simple type conversion",
    repair_type="type_cast",
)
```

## Configuration

Register your extensions in config:

```python
from dataforge.config import DataForgeConfig, DetectorConfig, RepairerConfig

config = DataForgeConfig(
    detectors=DetectorConfig(
        enabled_detectors=[
            "my_custom",
            "type_mismatch",
            "decimal_shift",
        ]
    ),
    repairers=RepairerConfig(
        enabled_repairers=[
            "my_custom_repairer",
            "type_mismatch",
        ]
    ),
)
```

## Debugging

```python
from dataforge.logging import setup_logging
from dataforge.config import LogLevel

# Enable debug logging
setup_logging(LogLevel.DEBUG, debug=True)

# Now your detector's print statements will show up
print("[DEBUG] Starting detection...")
```

## Limits and Performance

- CSVs up to ~1M rows process in seconds with default settings
- Sample-based detection (configurable) for faster analysis
- Incremental verification with Z3 timeouts

## Questions?

Refer to:
- `ARCHITECTURE_NEW.md` for system design
- Existing detectors in `dataforge/detectors/` for examples
- Tests in `tests/` for usage patterns
