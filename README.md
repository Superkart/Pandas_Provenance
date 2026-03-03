# Pandas Provenance Tracker

Pandas Provenance Tracker is a Python library that adds provenance and lineage tracking to pandas workflows.

It is designed for data science and analytics pipelines where reproducibility, explainability, and auditability matter.

## Executive Summary

This project captures how output data is produced from input data by logging:
- transformation metadata (operation type, timestamp, schema, dimensions),
- stable table identifiers (hash-based),
- row-level why-provenance witness sets.

The result is a traceable history of DataFrame transformations that helps teams debug pipelines, explain outcomes, and verify data processing logic.

## Key Capabilities

- Operation-level provenance logging to JSON
- Table-level identity via deterministic hashing
- Row-level why-provenance for:
  - read_csv
  - filter
  - drop_columns
  - merge
- Query APIs to inspect provenance at table or row granularity

## Why This Project Is Useful

- Reproducibility: understand exactly how a result table was derived
- Explainability: answer why a row exists in output
- Auditing: maintain an inspectable transformation history
- Debugging: identify unexpected lineage through complex joins and filters

## Architecture

Core components:
- ProvenanceTracker: main API for tracked transformations
- Hash utilities: deterministic table hashing and naming
- Provenance store:
  - persistent operation log (JSON)
  - in-memory row-level why-provenance witness mapping

High-level flow:
1. Execute a tracked transformation
2. Create/update row-level witness sets
3. Persist operation metadata to provenance log
4. Expose provenance through query APIs

## Current API (v0.1.0)

Primary methods:
- track_table_transformation
- read_csv
- filter
- drop_columns
- merge
- get_table_why_provenance
- get_row_why_provenance

## Why-Provenance Model

For each output row, provenance is represented as one or more witness sets.

Example structure:

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
- Each inner list is one sufficient witness set for producing the output row
- For merge, a witness typically includes one contributing tuple from each side

## Setup and Run

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

## Repository Structure

- Pandas_Provenance_Project/src/Pandas_Provenance/provenance_tracker.py
- Pandas_Provenance_Project/src/Pandas_Provenance/table_utils.py
- Pandas_Provenance_Project/examples/example_usage.py
- provenance/provenance_log.json

## Engineering Roadmap

Planned next milestones:
- groupby and aggregate why-provenance
- broader pandas operation coverage
- test suite expansion
- packaging and release hardening

## Tech Stack

- Python
- pandas
- JSON-based provenance persistence

## Contributing

Contributions are welcome through issues and pull requests.

## License

MIT
