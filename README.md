# Pandas Provenance Tracker

Pandas Provenance Tracker is a Python library that adds provenance and lineage tracking to pandas workflows.

It is designed for data science and analytics pipelines where reproducibility, explainability, and auditability matter.

## Problem

In many pandas workflows, output DataFrames are easy to compute but hard to explain later:
- Which source rows caused this output row?
- What transformations were applied over time?
- Can another person reproduce the same result confidently?

Without lineage tracking, debugging and auditing become expensive and error-prone.

## Solution

This project captures provenance at two levels:
- **Operation-level metadata**: transformation type, table hash, schema, dimensions, timestamp
- **Row-level why-provenance**: witness sets that explain why each output row exists

Provenance is persisted in JSON for inspection, sharing, and reproducibility.

## Why This Matters

- **Reproducibility**: re-trace the exact pipeline from source to output
- **Explainability**: answer *why did this row appear?*
- **Auditability**: maintain an inspectable transformation history
- **Debuggability**: isolate where unexpected rows/results were introduced

## Current Feature Scope (`v0.1.0`)

Supported tracked operations:
- `read_csv`
- `filter`
- `drop_columns`
- `merge`

Included provenance query APIs:
- `get_table_why_provenance`
- `get_row_why_provenance`

## Architecture

Core components:
- `ProvenanceTracker` in `Pandas_Provenance_Project/src/Pandas_Provenance/provenance_tracker.py`
- Hash utilities in `Pandas_Provenance_Project/src/Pandas_Provenance/table_utils.py`
- Persistent operation log in `provenance/provenance_log.json`
- In-memory why-provenance witness store keyed by table hash and row index

High-level execution flow:
1. A transformation runs (`filter`, `merge`, etc.)
2. Output table hash is generated
3. Why-provenance witness sets are generated/propagated
4. Operation metadata + rationale is appended to log
5. Provenance can be queried at table or row level

## API Reference

| Method | Purpose | Input | Output |
|---|---|---|---|
| `track_table_transformation` | Logs any table transformation | DataFrame + metadata | `(dataframe, table_name)` |
| `read_csv` | Reads CSV and initializes source provenance | `filepath` | `(dataframe, table_name)` |
| `filter` | Applies `DataFrame.query` with provenance propagation | `df, condition` | `(filtered_df, table_name)` |
| `drop_columns` | Drops selected columns with provenance propagation | `df, columns_to_drop` | `(df_out, table_name)` |
| `merge` | Merges two DataFrames and composes witness sets | `df1, df2, how, on` | `(merged_df, table_name)` |
| `get_table_why_provenance` | Returns full table row-level provenance map | `dataframe_or_hash` | `dict` |
| `get_row_why_provenance` | Returns one row’s witness details | `dataframe_or_hash, row_index` | `dict` |

## Why-Provenance Data Model

Each output row has one or more witness sets.

```json
{
  "witness_sets": [
    [
      {"table_hash": "...", "row_index": 2},
      {"table_hash": "...", "row_index": 0}
    ]
  ],
  "source": "merge"
}
```

Interpretation:
- Each inner array is one sufficient witness set for producing the output row.
- In merge operations, witnesses usually include one contributing tuple from each side.

## Quick Start

From repository root:

```bash
pip install -e .
python Pandas_Provenance_Project/examples/example_usage.py
```

Git Bash with local virtual environment:

```bash
cd Pandas_Provenance_Project
source venv/Scripts/activate
python examples/example_usage.py
```

## End-to-End Example

```python
import pandas as pd
from src.Pandas_Provenance import ProvenanceTracker

tracker = ProvenanceTracker()

df = pd.DataFrame({"A": [1, 2, 3, 4, 5], "B": [10, 20, 30, 40, 50]})
df, _ = tracker.track_table_transformation(df, transformation_type="source")

filtered_df, _ = tracker.filter(df, "A > 2")
reduced_df, _ = tracker.drop_columns(filtered_df, ["B"])

df2 = pd.DataFrame({"A": [3, 4, 5, 6], "C": ["a", "b", "c", "d"]})
merged_df, _ = tracker.merge(reduced_df, df2, on="A")

print(tracker.get_row_why_provenance(merged_df, 0))
```

Sample output (shape may vary):

```text
{'witness_sets': [[
  {'table_hash': '...', 'row_index': 2},
  {'table_hash': '...', 'row_index': 0}
]], 'source': 'merge'}
```

## Repository Structure

- `Pandas_Provenance_Project/src/Pandas_Provenance/provenance_tracker.py`
- `Pandas_Provenance_Project/src/Pandas_Provenance/table_utils.py`
- `Pandas_Provenance_Project/examples/example_usage.py`
- `provenance/provenance_log.json`

## Limitations (`v0.1.0`)

- No `groupby/agg` why-provenance yet
- Limited coverage of pandas operations
- Why-provenance currently in-memory during runtime (operation logs are persisted)
- Test suite and CI pipeline are not yet complete

## Roadmap

Planned next milestones:
- `groupby` and aggregate why-provenance
- broader pandas operation coverage
- stronger test suite and CI automation
- release hardening and documentation expansion

## Release & Distribution

Current public release process:
1. Merge feature branch into `main`
2. Build: `python -m build`
3. Validate: `python -m twine check dist/*`
4. Upload to TestPyPI
5. Upload to PyPI

## Tech Stack

- Python
- pandas
- JSON provenance persistence
- setuptools / wheel / twine

## Contributing

Contributions are welcome through issues and pull requests.

## License

MIT
