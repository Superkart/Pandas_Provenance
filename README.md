# Pandas Provenance Tracker

**A Python Library for Transparent and Reproducible Data Transformation Tracking**

---

## Overview

**Pandas Provenance Tracker** is a Python library that implements comprehensive provenance tracking for data transformations performed on pandas DataFrames. It automatically captures and logs metadata for every data manipulation operation, creating an auditable trail of how data evolves through complex analysis pipelines.

The library addresses a critical need in data science and research:  understanding the complete history of data transformations.  By wrapping standard pandas operations with provenance tracking capabilities, it enables transparency, reproducibility, and accountability without disrupting existing workflows.

---

## Problem Statement

Data scientists, researchers, and analysts face several challenges when working with complex data pipelines:

- **Lack of Transparency**: Difficult to trace which operations were applied and in what order
- **Reproducibility Issues**: Unable to recreate exact analysis steps from memory or incomplete documentation
- **Debugging Complexity**: Hard to identify where data quality issues or errors originated
- **Collaboration Barriers**: Team members cannot easily understand or verify each other's data transformations
- **Compliance Requirements**: Regulatory and academic standards often require detailed documentation of data processing
- **Version Control Gaps**: Git tracks code changes but not runtime data transformations

Pandas Provenance Tracker solves these problems by automatically maintaining a detailed, timestamped log of all DataFrame operations, including the rationale behind each transformation and its impact on the data. 

---

## Key Features

### Automatic Provenance Logging
- **Comprehensive Metadata Capture**: Records operation type, parameters, timestamps, and data dimensions
- **Content-Based Hashing**: Uses SHA-256 hashing to uniquely identify DataFrame states
- **Transformation Rationale**: Captures why-provenance explaining relationships between inputs and outputs
- **Persistent JSON Logs**: Stores provenance data in human-readable JSON format

### Wrapper Functions for Pandas Operations
- **Non-Intrusive Design**: Seamlessly wraps standard pandas operations without changing syntax
- **Supported Operations**:
  - `read_csv()` - Data loading with source file tracking
  - `filter()` / `query()` - Row filtering with condition preservation
  - `drop_columns()` - Column removal tracking
  - `merge()` / `join()` - Multi-DataFrame operations with relationship mapping
- **Return Value Consistency**: Returns both transformed DataFrame and unique table identifier

### Detailed Transformation Tracking
- **Input-Output Relationships**: Maps which input rows/columns produced which outputs
- **Row-Level Provenance**: Tracks specific row indices affected by filters and joins
- **Column Lineage**: Records column additions, removals, and transformations
- **Hash-Based Identification**:  Generates unique table names based on content hashes

### JSON Provenance Log Format
- **Structured Metadata**: Organized JSON with consistent schema
- **Human-Readable**: Easy to inspect and understand without special tools
- **Machine-Processable**: Suitable for automated analysis and visualization
- **Incremental Updates**: Appends new entries without rewriting entire log

---

## Use Cases

### Academic Research
- Document data preprocessing steps for publications
- Enable peer review of data transformation logic
- Support reproducible research initiatives
- Meet journal requirements for data provenance

### Data Science and Machine Learning
- Track feature engineering pipelines
- Debug model performance issues by tracing data quality
- Create audit trails for production ML systems
- Document data preparation for model cards

### Regulatory Compliance
- Maintain GDPR-compliant data processing records
- Support FDA validation requirements for clinical data
- Create audit trails for financial data analysis
- Document data quality controls for compliance

### Collaborative Projects
- Share data transformation history with team members
- Review and verify each other's data processing steps
- Onboard new team members with clear data lineage
- Coordinate distributed data workflows

### Debugging and Quality Assurance
- Identify where data corruption or errors occurred
- Trace unexpected values back to their source
- Validate data transformation correctness
- Compare different preprocessing approaches

---

## Technical Architecture

### Core Components

```
Pandas_Provenance_Project/
├── provenance/
│   ├── __init__.  py                  # Package initialization
│   ├── provenance_tracker.py        # Main tracking class
│   └── table_utils.py               # Hashing and naming utilities
├── tests/
│   └── test_provenance_tracker.py   # Unit tests
└── ShowCase. py                      # Example usage
```

### Provenance Tracker Class

**Key Methods:**

```python
class ProvenanceTracker:
    def __init__(self, log_File="provenance/provenance_log.json")
    def Track_Table_Transformation(...)
    def Extract_Transformation_Rationale(...)
    def read_csv(filepath)
    def filter(df, condition)
    def drop_columns(df, columns_to_drop)
    def merge(df1, df2, how="inner", on=None)
```

### Provenance Log Schema

Each transformation entry contains: 

```json
{
  "table_name": "table_a1b2c3d4",
  "table_hash": "a1b2c3d4e5f6.. .",
  "source_file":  "data/input. csv",
  "transformation_type": "filter",
  "transformation_specifics": "Age > 25",
  "dataframe_columns": ["Name", "Age", "City"],
  "dataframe_dimensions": [100, 3],
  "recorded_at": "2024-01-15T10:30:45.  123456",
  "transformation_rationale": {
    "type": "single_dataframe_operation",
    "input_output_relationship": {... },
    "applied_conditions": "Age > 25",
    "rows_satisfying_condition": [2, 5, 7, ...]
  }
}
```

---

## Installation

### From Source

```bash
# Clone the repository
git clone https://github.com/Superkart/Pandas_Provenance. git
cd Pandas_Provenance

# Install dependencies
pip install pandas numpy

# Install the package
pip install -e . 
```

### Requirements

- Python 3.7+
- pandas >= 1.0.0
- numpy >= 1.18.0

---

## Quick Start

### Basic Usage Example

```python
from provenance.provenance_tracker import ProvenanceTracker
import pandas as pd

# Initialize tracker
tracker = ProvenanceTracker(log_File="provenance/my_analysis_log.json")

# Load data with provenance tracking
df, table_name = tracker.read_csv("data/employees.csv")
print(f"Loaded table: {table_name}")

# Filter data - automatically logs condition and affected rows
filtered_df, filtered_table = tracker.filter(df, "Salary > 50000")
print(f"Filtered to:  {filtered_table}")

# Drop columns - logs which columns were removed
clean_df, clean_table = tracker.drop_columns(filtered_df, ["SSN", "TempID"])
print(f"Cleaned table: {clean_table}")

# Merge operations - tracks input-output relationships
merged_df, merged_table = tracker.merge(
    clean_df, 
    department_df, 
    how="left", 
    on="DeptID"
)
print(f"Merged table: {merged_table}")
```

### Examining the Provenance Log

```python
import json

# Read the provenance log
with open("provenance/my_analysis_log.json", "r") as f:
    provenance_log = json.load(f)

# Display all transformations
for entry in provenance_log:
    print(f"Operation: {entry['transformation_type']}")
    print(f"  Table: {entry['table_name']}")
    print(f"  Timestamp: {entry['recorded_at']}")
    print(f"  Dimensions: {entry['dataframe_dimensions']}")
    print(f"  Details: {entry['transformation_specifics']}\n")
```

---

## Detailed Usage

### 1. Loading Data

```python
# CSV files - tracks source file path
df, table_id = tracker.read_csv("data/sales_2024.csv")

# The provenance log records: 
# - Source file path
# - Column names
# - Data dimensions
# - Unique content hash
```

### 2. Filtering Operations

```python
# Simple condition
filtered, table_id = tracker.filter(df, "Age >= 18")

# Complex condition
filtered, table_id = tracker.filter(
    df, 
    "(Age >= 18) & (Country == 'USA')"
)

# The provenance log records:
# - Filter condition
# - Row indices that satisfied the condition
# - Input/output row counts
# - Complete why-provenance
```

### 3. Column Operations

```python
# Drop single column
cleaned, table_id = tracker.drop_columns(df, ["TempColumn"])

# Drop multiple columns
cleaned, table_id = tracker.drop_columns(
    df, 
    ["Col1", "Col2", "Col3"]
)

# The provenance log records:
# - Which columns were dropped
# - Remaining column list
# - Transformation type
```

### 4. Merge Operations

```python
# Inner join
result, table_id = tracker.merge(
    customers_df,
    orders_df,
    how="inner",
    on="CustomerID"
)

# Left join with multiple keys
result, table_id = tracker.merge(
    df1,
    df2,
    how="left",
    on=["Key1", "Key2"]
)

# The provenance log records:
# - Join type (inner, left, right, outer)
# - Join keys
# - Input DataFrame hashes
# - Row counts before/after merge
# - Input-output relationship mapping
```

---

## Advanced Features

### Custom Log File Location

```python
# Specify custom log file path
tracker = ProvenanceTracker(log_File="logs/experiment_2024_01.json")
```

### Content-Based Table Naming

```python
# Tables are named based on SHA-256 hash of their content
# Same content = same table name (deterministic)
df1, name1 = tracker.filter(original, "Age > 25")
df2, name2 = tracker.filter(original, "Age > 25")
# name1 == name2 (identical content)

df3, name3 = tracker.filter(original, "Age > 30")
# name3 != name1 (different content)
```

### Transformation Rationale Extraction

The `Extract_Transformation_Rationale` method provides detailed why-provenance: 

```python
# For filter operations: 
{
    "type": "single_dataframe_operation",
    "input_row_indices": [0, 1, 2, 3, 4, ... ],
    "output_row_indices": [0, 1, 2, ... ],
    "applied_conditions":  "Age > 25",
    "rows_satisfying_condition": [2, 4, 7, 9, ...]
}

# For merge operations:
{
    "type": "multi_dataframe_operation",
    "first_input_dataframe_rows": 100,
    "second_input_dataframe_rows": 50,
    "output_rows":  75,
    "input_dataframe_hashes": ["abc123.. .", "def456... "]
}
```

---

## Code Architecture

### Provenance Tracker Implementation

```python
class ProvenanceTracker: 
    def __init__(self, log_File="provenance/provenance_log.json"):
        """Initialize tracker with log file path"""
        self.log_File_Path = os.path.abspath(log_File)
        self.provenance_Entries = []
        self.Initialize_Log_Storage()
    
    def Track_Table_Transformation(
        self, 
        dataframe, 
        source_file=None,
        transformation_Type=None,
        transformation_Details=None,
        input_Dataframes=None
    ):
        """Core tracking method - captures all metadata"""
        # Generate unique table identifier
        table_hash = calculate_hash(dataframe)
        table_name = generate_table_name(table_hash)
        
        # Extract transformation rationale
        rationale = self.Extract_Transformation_Rationale(...)
        
        # Create provenance record
        record = {
            "table_name": table_name,
            "table_hash": table_hash,
            "transformation_type": transformation_Type,
            # ... additional metadata
        }
        
        # Persist to JSON log
        self.provenance_Entries.append(record)
        self.Persist_Provenance_Log()
        
        return dataframe, table_name
```

### Utility Functions

```python
# table_utils.py
def calculate_hash(dataframe):
    """Generate SHA-256 hash of DataFrame content"""
    hash_object = hashlib.sha256(
        pd.util.hash_pandas_object(dataframe).values
    )
    return hash_object.hexdigest()

def generate_table_name(table_hash):
    """Create human-readable table name from hash"""
    return f"table_{table_hash[:8]}"
```

---

## Testing

### Running Tests

```bash
# Run all tests
pytest Pandas_Provenance_Project/tests/

# Run with verbose output
pytest -v Pandas_Provenance_Project/tests/

# Run specific test
pytest Pandas_Provenance_Project/tests/test_provenance_tracker.py:: test_filter_operation
```

### Test Coverage

The test suite includes: 
- CSV reading with provenance
- Filter operation tracking
- Column dropping operations
- Merge operation logging
- Provenance log persistence
- Hash calculation correctness

---

## Example Workflow

### Complete Data Analysis Pipeline

```python
from provenance.provenance_tracker import ProvenanceTracker

# Initialize tracker
tracker = ProvenanceTracker(log_File="analysis/sales_pipeline.json")

# Step 1: Load raw data
sales_df, sales_table = tracker.read_csv("data/raw_sales.csv")
print(f"Step 1: Loaded {sales_table}")

# Step 2: Filter valid transactions
valid_df, valid_table = tracker.filter(
    sales_df, 
    "(Amount > 0) & (Status == 'completed')"
)
print(f"Step 2: Filtered to {valid_table}")

# Step 3: Remove sensitive columns
clean_df, clean_table = tracker.drop_columns(
    valid_df,
    ["CustomerSSN", "CreditCardNumber"]
)
print(f"Step 3: Cleaned to {clean_table}")

# Step 4: Merge with product information
products_df, prod_table = tracker.read_csv("data/products.csv")
enriched_df, enriched_table = tracker.merge(
    clean_df,
    products_df,
    how="left",
    on="ProductID"
)
print(f"Step 4: Enriched to {enriched_table}")

# Now the provenance log contains complete pipeline history! 
```

---

## Jupyter Notebook Integration

The project includes `ProvenanceOnPandas.ipynb` demonstrating interactive usage:

```python
# In Jupyter Notebook
from provenance.provenance_tracker import ProvenanceTracker

tracker = ProvenanceTracker()

# Load data
df, _ = tracker.read_csv("data. csv")
display(df. head())

# Apply transformations
filtered, _ = tracker.filter(df, "value > threshold")
display(filtered)

# View provenance log inline
import json
with open("provenance/provenance_log.json") as f:
    print(json.dumps(json.load(f), indent=2))
```

---

## Provenance Log Analysis

### Querying the Log

```python
import json
import pandas as pd

# Load provenance log
with open("provenance/provenance_log.json") as f:
    log = json.load(f)

# Convert to DataFrame for analysis
log_df = pd.DataFrame(log)

# Find all filter operations
filters = log_df[log_df['transformation_type'] == 'filter']
print(f"Total filters applied: {len(filters)}")

# Trace a specific table's lineage
target_table = "table_a1b2c3d4"
lineage = log_df[log_df['table_name'] == target_table]
print(lineage[['transformation_type', 'recorded_at']])
```

### Visualizing Provenance

```python
import matplotlib.pyplot as plt

# Transformation type distribution
log_df['transformation_type'].value_counts().plot(kind='bar')
plt.title('Data Transformation Distribution')
plt.xlabel('Operation Type')
plt.ylabel('Frequency')
plt.show()

# Data size evolution
sizes = [(entry['dataframe_dimensions'][0], entry['recorded_at']) 
         for entry in log]
timestamps, row_counts = zip(*[(t, s) for s, t in sizes])
plt.plot(row_counts)
plt.title('Dataset Size Over Time')
plt.ylabel('Row Count')
plt.show()
```

---

## Performance Considerations

**Overhead:**
- Hashing:  ~10-50ms for typical DataFrames (10k-100k rows)
- JSON writing: ~5-20ms per operation
- Total overhead: ~15-70ms per tracked operation

**Optimization Tips:**
- Use batch operations when possible
- Disable tracking for exploratory analysis
- Clear old log entries periodically for long-running pipelines

---

## Future Enhancements

### Planned Features
- **Graphical Provenance Visualization**: Interactive lineage graphs
- **Time-Travel Debugging**: Restore DataFrame to any historical state
- **Diff Visualization**: Compare DataFrame versions
- **Export Formats**: SQL, CSV, Neo4j graph database
- **Additional Operations**: Group-by, pivot, aggregation tracking
- **Performance Profiling**: Track execution time per operation
- **Data Quality Metrics**: Automatic data quality scoring
- **Integration with DVC**: Data Version Control compatibility

### Research Extensions
- PROV-O compliant RDF export
- Integration with Apache Atlas
- Blockchain-based immutable provenance
- Machine learning for anomaly detection in pipelines

---

## Academic Citations

If you use this library in academic work, please cite:

```
@software{pandas_provenance_2024,
  author = {Superkart},
  title = {Pandas Provenance Tracker},
  year = {2024},
  url = {https://github.com/Superkart/Pandas_Provenance}
}
```

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## Developer

**Superkart**

- GitHub: [@Superkart](https://github.com/Superkart)
- Project Repository: [Pandas_Provenance](https://github.com/Superkart/Pandas_Provenance)

---

## Acknowledgments

- **Pandas Development Team** for the excellent data manipulation library
- **Provenance Research Community** for W3C PROV standards
- **Open Science Community** for reproducibility best practices

---

**Transparent Data Workflows | Reproducible Research | Accountable Analytics**
