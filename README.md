# df-diff

<p align="center">
  <img src="logo.svg" alt="df-diff logo" width="256" height="256">
</p>

<p align="center">
  <strong>Compare DataFrames. Find differences. Trust your data.</strong>
</p>

---

A Python toolkit for comparing two rectangular datasets (CSV, Parquet) and generating detailed difference reports.

Built for validating data migrations, comparing SQL vs Spark outputs, and verifying ETL pipelines.

> ⚠️ **Headers Required**: All input files must have a header row. The first row is always treated as column names, not data. Files without headers will produce incorrect results.

## Why df-diff?

Pandas has a built-in `df.diff()` method. Why use this instead?

**They solve different problems:**

| Feature | `pandas df.diff()` | `df-diff` |
|---------|-------------------|-----------|
| **Purpose** | Compute row-to-row differences *within* one DataFrame | Compare *two different* DataFrames cell-by-cell |
| **Output** | Numeric deltas (current row - previous row) | Match/mismatch report with categories |
| **Use case** | Time series analysis, rate of change | Data validation, migration testing, QA |

### What df-diff Does That pandas Can't

```python
# Pandas df.diff() - differences WITHIN a single DataFrame
df = pd.DataFrame({'value': [100, 105, 103]})
df.diff()
#    value
# 0    NaN
# 1    5.0   ← 105 - 100
# 2   -2.0   ← 103 - 105

# df-diff - differences BETWEEN two DataFrames
# "Is my Spark export identical to my SQL export?"
# "What changed between yesterday's data and today's?"
```

### df-diff Features

| Capability | pandas built-ins | df-diff |
|------------|------------------|---------|
| Compare two files directly | ❌ Load manually | ✅ `df-diff a.csv b.parquet` |
| Key-based row matching | ❌ DIY merge logic | ✅ `--key customer_id` |
| Composite keys | ❌ DIY | ✅ `--key id --key date` |
| Schema comparison | ❌ | ✅ Column names, order, types |
| Null normalization | ❌ | ✅ `NA`=`NULL`=`None`=`""` |
| Whitespace tolerance | ❌ | ✅ `"John "` = `"John"` |
| Numeric tolerance | ❌ | ✅ `3.14159` ≈ `3.14159000001` |
| Case insensitive option | ❌ | ✅ `--ignore-case` |
| Missing row detection | ❌ | ✅ Keys only in A, only in B |
| Detailed report | ❌ | ✅ Markdown with samples |
| CI/CD exit codes | ❌ | ✅ 0=match, 1=different |

### Scale Comparison

| Dataset | pandas manual comparison | df-diff V2 | df-diff V3 |
|---------|-------------------------|------------|------------|
| 10K rows | ~5 sec | ~2 sec | ~3 sec |
| 1M rows | ~2 min, high memory | ~1 min | ~45 sec |
| 10M rows | Often crashes (OOM) | Often crashes | ✅ ~20 min, 2GB RAM |
| 100M rows | ❌ | ❌ | ✅ Streams from disk |

### The Problem df-diff Solves

```
You migrated a database from SQL Server to Spark.
You have two exports: sales_sql.csv and sales_spark.parquet

Questions you need answered:
├── Are all 10 million rows present in both?
├── Are any customer_ids missing?
├── Do the values match exactly?
├── If not, is it just whitespace/formatting differences?
├── Or are there real data discrepancies?
└── Can I get a report to show my manager?

df-diff answers all of these in one command:
$ python df-diff a.csv b.parquet --key customer_id
```

## Features

- **Schema comparison** - Column names, order, and types
- **Key analysis** - Uniqueness, coverage, missing/extra rows
- **Value comparison** with smart normalization:
  - Whitespace trimming (`"  John  "` = `"John"`)
  - Null equivalence (`NA`, `NULL`, `None`, `""` treated as null)
  - Numeric tolerance (`12.0` = `12`, `3.14159` ≈ `3.14159000001`)
  - Case-insensitive option
- **Detailed markdown reports** with sample mismatches
- **Three engine options** for different scale requirements

## Quick Start

```bash
# Clone and install
git clone https://github.com/YOUR_USERNAME/df-diff.git
cd df-diff
pip install -r requirements.txt

# Run comparison
python src/df_diff_v2_vectorized.py data_a.csv data_b.parquet --key customer_id
```

## Which Version Should I Use?

| Version | Engine | Best For | Memory |
|---------|--------|----------|--------|
| `v1_slow` | Pandas row-by-row | Debugging, <10K rows | O(n) |
| `v2_vectorized` | Pandas vectorized | 10K-500K rows | O(n) |
| `v3_duckdb` | DuckDB streaming | 500K+ rows, large files | O(1) |

**Rule of thumb:** Start with `v2`. If you run out of memory or it's too slow, switch to `v3`.

## Usage

### Basic

```bash
python src/df_diff_v2_vectorized.py file_a.csv file_b.parquet --key id
```

### Composite Key

```bash
python src/df_diff_v2_vectorized.py file_a.csv file_b.csv \
    --key customer_id \
    --key order_date
```

### All Options

```bash
python src/df_diff_v2_vectorized.py file_a.csv file_b.parquet \
    --key id \
    --output report.md \
    --numeric-tolerance 0.001 \
    --ignore-case \
    --no-strip \
    --max-samples 20
```

### V3 (DuckDB) Additional Options

```bash
python src/df_diff_v3_duckdb.py large_a.parquet large_b.parquet \
    --key id \
    --batch-size 100  # Columns per SQL query (default: 50)
```

## CLI Reference

| Argument | Description |
|----------|-------------|
| `file_a` | Path to first file (CSV or Parquet) |
| `file_b` | Path to second file (CSV or Parquet) |
| `--key`, `-k` | Primary key column(s). Repeat for composite keys. |
| `--output`, `-o` | Output markdown report path (default: `comparison_report.md`) |
| `--numeric-tolerance` | Tolerance for numeric comparison (default: `1e-9`) |
| `--ignore-case` | Case-insensitive string comparison |
| `--no-strip` | Disable whitespace trimming |
| `--max-samples` | Max sample mismatches per column (default: `10`) |
| `--allow-duplicate-keys` | Continue even if primary keys are not unique |

## Output

### Console Summary

```
============================================================
DATA COMPARISON SUMMARY (Pandas Vectorized)
============================================================

File A: sales_sql.csv (150,432 rows)
File B: sales_spark.parquet (150,430 rows)

Schema:       ✓ MATCH
Row Coverage: ⚠ 6 keys differ (4 only in A, 2 only in B)
Values:       ✓ MATCH (after normalization)

------------------------------------------------------------
OVERALL: EQUIVALENT (with normalization/reordering)
------------------------------------------------------------
```

When schemas differ, the tool shows which columns are different:

```
============================================================
DATA COMPARISON SUMMARY (Pandas Vectorized)
============================================================

File A: sales_v1.csv (10,000 rows)
File B: sales_v2.csv (10,000 rows)

Schema:       ✗ MISMATCH
Row Coverage: ✓ MATCH (all keys present in both)
Values:       ✗ 245 mismatches (1.63%)

------------------------------------------------------------
OVERALL: DIFFERENT
------------------------------------------------------------

⚠ Schema differences detected:
  Columns only in A (2):
    - legacy_region_code
    - old_discount_type
  Columns only in B (3):
    - new_region_id
    - discount_category
    - tax_rate

⚠ 245 cell mismatches found
  Columns with mismatches:
    - unit_price: 128
    - quantity: 89
    - sales_rep: 28
```

### Markdown Report

See [docs/sample_report.md](docs/sample_report.md) for a full example.

## Exit Codes

| Code | Meaning |
|------|---------|
| `0` | IDENTICAL or EQUIVALENT |
| `1` | DIFFERENT |
| `2` | Error (file not found, invalid key, etc.) |

Use in CI/CD:
```bash
python src/df_diff_v2_vectorized.py expected.csv actual.csv --key id || echo "Data mismatch!"
```

## Performance

Benchmarks on M1 MacBook Pro (16GB RAM):

| Dataset | V1 (slow) | V2 (vectorized) | V3 (DuckDB) |
|---------|-----------|-----------------|-------------|
| 10K × 50 cols | 45 sec | 2 sec | 3 sec |
| 100K × 100 cols | — | 15 sec | 10 sec |
| 1M × 200 cols | — | 4 min | 90 sec |
| 10M × 500 cols | — | OOM | 20 min |

Run your own benchmarks:
```bash
python benchmarks/benchmark.py
```

## Project Structure

```
df-diff/
├── src/
│   ├── df_diff_v1_slow.py        # Reference implementation
│   ├── df_diff_v2_vectorized.py  # Recommended for most cases
│   └── df_diff_v3_duckdb.py      # For large datasets
├── testdata/
│   ├── data_a_sql.csv                 # Source of truth
│   ├── data_b_identical.csv           # Exact copy → IDENTICAL
│   ├── data_c_equivalent.csv          # Normalizable diffs → EQUIVALENT
│   ├── data_d_different.csv           # Real differences → DIFFERENT
│   └── README.md
├── benchmarks/
│   └── benchmark.py                   # Performance comparison script
├── docs/
│   ├── how_it_works.md               # Technical deep-dive
│   └── sample_report.md              # Example output
├── requirements.txt
├── LICENSE
└── README.md
```

## Requirements

**V1 and V2:**
```
pandas>=2.0
numpy>=1.24
pyarrow>=12.0  # For Parquet support
```

**V3 (additional):**
```
duckdb>=0.9
```

## How It Works

See [docs/how_it_works.md](docs/how_it_works.md) for a detailed explanation of:
- The comparison algorithm
- Normalization cascade (exact → null → trim → numeric → mismatch)
- Why V3 is faster than V2 for large data
- Memory usage patterns

## Limitations

- **Headers required** — First row is always treated as column names. A warning is shown if all column names appear to be numeric (suggesting the first row might be data).
- **CSV and Parquet only** — No Excel, JSON, etc.
- **In-memory join** — V2 requires RAM for both files + merged result
- **Single-machine** — Not distributed (use Spark for TB-scale)

## License

MIT License - Use freely, fork at will.

---

## Educational Use

This repo is designed for learning. The three versions implement the **same algorithm** with different execution strategies, making it easy to study how implementation choices affect performance.

### Why Three Versions?

| Version | Strategy | Lesson |
|---------|----------|--------|
| V1 | Python loops | Baseline — how you'd write it first |
| V2 | Vectorized pandas | Why NumPy/pandas beat pure Python |
| V3 | SQL streaming | Why databases beat pandas at scale |

### The Core Algorithm

All three versions follow the same comparison cascade:

```
┌─────────────────────────────────────────────────────────────┐
│                    For each cell (A, B):                    │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
                 ┌───────────────────────┐
                 │  A == B (as strings)? │
                 └───────────┬───────────┘
                        yes │ no
                    ┌───────┴───────┐
                    ▼               ▼
             ┌──────────┐   ┌───────────────────────┐
             │  EXACT   │   │  Both null-like?      │
             │  MATCH   │   │  ("", NA, NULL, None) │
             └──────────┘   └───────────┬───────────┘
                                   yes │ no
                               ┌───────┴───────┐
                               ▼               ▼
                        ┌──────────┐   ┌───────────────────────┐
                        │   NULL   │   │  trim(A) == trim(B)?  │
                        │  MATCH   │   │  (whitespace, case)   │
                        └──────────┘   └───────────┬───────────┘
                                              yes │ no
                                          ┌───────┴───────┐
                                          ▼               ▼
                                   ┌──────────┐   ┌───────────────────────┐
                                   │  TRIM    │   │  |num(A) - num(B)|    │
                                   │  MATCH   │   │     < tolerance?      │
                                   └──────────┘   └───────────┬───────────┘
                                                         yes │ no
                                                     ┌───────┴───────┐
                                                     ▼               ▼
                                              ┌──────────┐    ┌──────────┐
                                              │ NUMERIC  │    │ MISMATCH │
                                              │  MATCH   │    │          │
                                              └──────────┘    └──────────┘
```

### V1: Row-by-Row (The Naive Approach)

```python
for col in columns:                    # For each column
    for idx, row in df.iterrows():     # For each row (SLOW!)
        if row[col_a] == row[col_b]:
            exact += 1
        elif is_null(row[col_a]) and is_null(row[col_b]):
            null_match += 1
        # ... etc
```

```
┌─────────────────────────────────────────────────────────────┐
│  250K rows × 275 columns = 68.75 MILLION Python iterations  │
└─────────────────────────────────────────────────────────────┘

         Column 1         Column 2         Column 3
        ┌───┐            ┌───┐            ┌───┐
Row 1   │ → │ compare    │ → │ compare    │ → │ compare
        ├───┤            ├───┤            ├───┤
Row 2   │ → │ compare    │ → │ compare    │ → │ compare
        ├───┤            ├───┤            ├───┤
Row 3   │ → │ compare    │ → │ compare    │ → │ compare
        ├───┤            ├───┤            ├───┤
  ...   │...│            │...│            │...│
        └───┘            └───┘            └───┘
        
Each arrow = 1 Python function call with type checking overhead
```

### V2: Vectorized (Let NumPy Do the Work)

```python
for col in columns:                    # Still loop over columns
    # But entire column compared in ONE C operation
    mask = (series_a == series_b)      # Returns 250K booleans instantly
    exact = mask.sum()                 # Sum in C, not Python
```

```
┌─────────────────────────────────────────────────────────────┐
│  275 column iterations × O(1) vectorized ops on 250K rows   │
└─────────────────────────────────────────────────────────────┘

         Column 1              Column 2              Column 3
     ┌─────────────┐       ┌─────────────┐       ┌─────────────┐
     │ ■ ■ ■ ■ ■ ■ │  ══>  │ ■ ■ ■ ■ ■ ■ │  ══>  │ ■ ■ ■ ■ ■ ■ │
     │ ■ ■ ■ ■ ■ ■ │       │ ■ ■ ■ ■ ■ ■ │       │ ■ ■ ■ ■ ■ ■ │
     │ ■ ■ ■ ■ ■ ■ │       │ ■ ■ ■ ■ ■ ■ │       │ ■ ■ ■ ■ ■ ■ │
     │  (250K vals) │       │  (250K vals) │       │  (250K vals) │
     └─────────────┘       └─────────────┘       └─────────────┘
           │                     │                     │
           ▼                     ▼                     ▼
      ONE numpy call        ONE numpy call        ONE numpy call
      processes all         processes all         processes all
```

### V3: SQL Streaming (Let the Database Do Everything)

```sql
SELECT 
    SUM(CASE WHEN a = b THEN 1 ELSE 0 END) AS exact_col1,
    SUM(CASE WHEN a = b THEN 1 ELSE 0 END) AS exact_col2,
    -- ... 50 columns computed in ONE pass
FROM (
    SELECT * FROM file_a JOIN file_b ON key
)
```

```
┌─────────────────────────────────────────────────────────────┐
│  ~6 SQL queries (50 cols each), data streams from disk      │
└─────────────────────────────────────────────────────────────┘

    ┌──────────┐     ┌──────────────────────────────────────┐
    │          │     │           DuckDB Engine              │
    │  Disk    │ ──> │  ┌─────┐  ┌─────┐  ┌─────┐          │
    │ (files)  │     │  │ CPU │  │ CPU │  │ CPU │  4 cores │
    │          │     │  └─────┘  └─────┘  └─────┘          │
    └──────────┘     │       │       │       │             │
                     │       └───────┼───────┘             │
         never       │               ▼                     │
         fully       │  ┌───────────────────────┐          │
         in RAM      │  │  Running aggregates   │          │
                     │  │  (just a few numbers) │          │
                     │  └───────────────────────┘          │
                     └──────────────────────────────────────┘
```

### V3: SQL NULL Handling

SQL has **three-valued logic**: expressions evaluate to TRUE, FALSE, or **NULL**. Any comparison involving NULL returns NULL, not TRUE or FALSE:

```sql
NULL = NULL   → NULL  (not TRUE!)
NULL != NULL  → NULL  (not TRUE!)
NULL = 'hello' → NULL
NULL != 'hello' → NULL
```

This matters because V3's comparison cascade uses SQL `CASE WHEN` expressions. A naive approach fails silently:

```sql
-- BROKEN: if either value is NULL, this returns 0 (not 1)
SUM(CASE WHEN CAST(a AS VARCHAR) = CAST(b AS VARCHAR) THEN 1 ELSE 0 END)

-- Both sides are NULL → CAST(NULL AS VARCHAR) = CAST(NULL AS VARCHAR) → NULL → ELSE 0
-- The row is not counted as a match OR a mismatch — it just vanishes.
```

V3 uses `IS NOT DISTINCT FROM` / `IS DISTINCT FROM`, which are NULL-safe comparison operators that treat NULL as equal to NULL:

```sql
NULL IS NOT DISTINCT FROM NULL    → TRUE   (NULL equals NULL)
NULL IS DISTINCT FROM NULL        → FALSE
NULL IS NOT DISTINCT FROM 'hello' → FALSE  (NULL differs from a value)
NULL IS DISTINCT FROM 'hello'     → TRUE

-- V3's actual exact-match check:
SUM(CASE WHEN CAST(a AS VARCHAR) IS NOT DISTINCT FROM CAST(b AS VARCHAR) THEN 1 ELSE 0 END)
```

**Why V1/V2 don't have this problem:** Pandas converts all values to Python strings before comparison (`series.astype(str)`), which turns `pd.NA` into the literal string `"<NA>"`. After that, `==` and `!=` always return True/False — Python doesn't have three-valued logic.

| Expression | Python | SQL |
|------------|--------|-----|
| `NULL == NULL` | `True` (both become `"<NA>"`) | `NULL` (unknown) |
| `NULL != NULL` | `False` | `NULL` (unknown) |
| `NULL == "hello"` | `False` | `NULL` (unknown) |

This is a general hazard when porting pandas logic to SQL: any column that can contain NULL values will silently break boolean conditions unless you use NULL-safe operators.

### Memory Comparison

```
For 10M rows × 500 columns:

V1/V2 (Pandas):
┌────────────────────────────────────────────────────────┐
│████████████████████████████████████████████████████████│ File A: 8GB
├────────────────────────────────────────────────────────┤
│████████████████████████████████████████████████████████│ File B: 8GB
├────────────────────────────────────────────────────────┤
│████████████████████████████████████████████████████████│ Merged: 12GB
├──────────────────────┤                                  │
│██████████████████████│ Temp arrays: 5GB+                │
└────────────────────────────────────────────────────────┘
Total: 33+ GB  (often crashes)


V3 (DuckDB):
┌──────┐
│██████│ Working memory: ~2GB (fixed, regardless of file size)
└──────┘
Total: ~2GB  (streams from disk)
```

### Speed Comparison

```
10K rows × 50 cols:

V1: ████████████████████████████████████████░░░░░░░ 45 sec
V2: ██░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  2 sec  ← 22× faster
V3: ███░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░  3 sec


1M rows × 200 cols:

V1: (would take hours, not practical)
V2: ████████████████████████████████████████░░░░░░░  4 min
V3: █████████████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░ 90 sec  ← 2.7× faster


10M rows × 500 cols:

V1: (not practical)
V2: (out of memory on 16GB machine)
V3: ████████████████████████████████░░░░░░░░░░░░░░░ 20 min  ← only option
```

### Studying the Code

The best way to learn:

1. **Read V1 first** — understand the algorithm without optimization noise
2. **Diff V1 vs V2** — see how vectorization replaces loops:
   ```bash
   diff -y src/df_diff_v1_slow.py src/df_diff_v2_vectorized.py | less
   ```
3. **Diff V2 vs V3** — see how SQL replaces pandas:
   ```bash
   diff -y src/df_diff_v2_vectorized.py src/df_diff_v3_duckdb.py | less
   ```

The files are structured identically — same sections, same order — so diffs are meaningful.

---

## Contributing

This is a "fork and modify" project. I don't plan to actively maintain it, but feel free to:
- Fork and customize for your needs
- Open issues for bugs (I may or may not respond)
- Submit PRs (I'll review when I can)

## Acknowledgments

Built during a data migration project comparing MS SQL exports against Spark outputs. The pain of manual diff-ing inspired this tool.
