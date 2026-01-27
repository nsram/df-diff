# How It Works

A technical deep-dive into the Data Compare tool.

## Overview

The comparison process has four phases:

```
┌─────────────┐     ┌─────────────┐
│   File A    │     │   File B    │
└──────┬──────┘     └──────┬──────┘
       │                   │
       ▼                   ▼
┌──────────────────────────────────┐
│     1. Schema Comparison         │
│  - Column names, order, types    │
└──────────────┬───────────────────┘
               ▼
┌──────────────────────────────────┐
│     2. Key Analysis              │
│  - Uniqueness, coverage          │
└──────────────┬───────────────────┘
               ▼
┌──────────────────────────────────┐
│     3. Value Comparison          │
│  - Join on key, compare cells    │
└──────────────┬───────────────────┘
               ▼
┌──────────────────────────────────┐
│     4. Report Generation         │
│  - Markdown + stdout summary     │
└──────────────────────────────────┘
```

## The Normalization Cascade

When comparing two values, we try multiple strategies in order:

```
Step 1: Exact String Match
        "John Smith" == "John Smith"  →  ✓ EXACT MATCH

Step 2: Null Equivalence
        "NULL" vs "NA" vs "" vs None  →  ✓ NULL MATCH

Step 3: Whitespace/Case Normalization
        "  John Smith  " → "John Smith"
        "JOHN SMITH" → "john smith" (if --ignore-case)
                                      →  ✓ NORMALIZED MATCH

Step 4: Numeric Tolerance
        "49.99" vs "49.990000001"
        |49.99 - 49.990000001| < tolerance
                                      →  ✓ NUMERIC MATCH

Step 5: Everything else
                                      →  ✗ MISMATCH
```

### Why This Order?

1. **Exact match is fastest** - simple string comparison
2. **Null check is cheap** - set membership test
3. **String normalization is cheap** - `strip()` and `lower()`
4. **Numeric parsing is expensive** - only try if nothing else worked
5. **Mismatch is the fallback** - collect samples for debugging

## Version Differences

### V1: Row-by-Row (Reference Implementation)

```python
for col in columns:
    for idx, row in merged.iterrows():  # Python loop - SLOW
        val_a, val_b = row[col_a], row[col_b]
        # ... comparison logic ...
```

**Complexity:** O(rows × columns) Python operations

**Use case:** Debugging, understanding the algorithm, <10K rows

### V2: Vectorized Pandas

```python
for col in columns:
    # Vectorized operations - FAST
    exact_mask = (series_a == series_b)           # Entire column at once
    null_mask = is_null(series_a) & is_null(series_b)
    trim_mask = (series_a.str.strip() == series_b.str.strip())
    # ... etc ...
    
    count = exact_mask.sum()  # Vectorized reduction
```

**Complexity:** O(columns) Python operations, O(rows) happens in C

**Use case:** Most common scenarios, 10K-500K rows

### V3: DuckDB Streaming

```sql
SELECT 
    SUM(CASE WHEN a = b THEN 1 ELSE 0 END) AS exact,
    SUM(CASE WHEN a != b AND is_null(a) AND is_null(b) THEN 1 ELSE 0 END) AS null_match,
    SUM(CASE WHEN TRIM(a) = TRIM(b) THEN 1 ELSE 0 END) AS trim_match,
    ...
FROM matched
```

**Complexity:** O(rows) streaming, O(1) memory per batch

**Use case:** Large datasets, 500K+ rows, memory-constrained

## Why DuckDB is Faster Than Pandas at Scale

### 1. Memory Access Pattern

**Pandas:** Multiple passes, intermediate allocations
```python
series_a = merged[col].astype(str)     # Pass 1: allocate
trimmed = series_a.str.strip()          # Pass 2: allocate
mask = (trimmed == trimmed_b)           # Pass 3: allocate
count = mask.sum()                       # Pass 4: reduce
# 4 full scans, 3 large allocations
```

**DuckDB:** Single pass, streaming aggregation
```sql
SUM(CASE WHEN TRIM(a) = TRIM(b) THEN 1 ELSE 0 END)
-- 1 scan, running counter, no intermediate storage
```

### 2. Column Batching

**Pandas:** One column at a time
```
Column 1: scan → scan → scan → scan
Column 2: scan → scan → scan → scan
...
Column 1000: scan → scan → scan → scan
```

**DuckDB:** 50 columns per scan
```
Columns 1-50:    one scan computes all 50
Columns 51-100:  one scan computes all 50
...
```

### 3. Parallelization

**Pandas:** Single-threaded

**DuckDB:** Automatic multi-threading
```
Thread 1: rows 0-2.5M      ─┐
Thread 2: rows 2.5M-5M      ├─→ merge → result
Thread 3: rows 5M-7.5M      │
Thread 4: rows 7.5M-10M    ─┘
```

### 4. Memory Efficiency

| 10M rows × 500 cols | Pandas | DuckDB |
|---------------------|--------|--------|
| File A in memory | 8 GB | 0 (streams) |
| File B in memory | 8 GB | 0 (streams) |
| Merged DataFrame | 12 GB | ~100 MB (view) |
| Intermediate arrays | 5+ GB | ~0 |
| **Peak memory** | **33+ GB** | **~2 GB** |

## Numeric Tolerance Formula

We use the same formula as `numpy.isclose`:

```
|a - b| <= atol + rtol × |b|
```

Where:
- `atol` = absolute tolerance (default: 1e-9)
- `rtol` = relative tolerance (default: 1e-9)

**Examples:**

| a | b | atol=1e-9, rtol=1e-9 | Result |
|---|---|----------------------|--------|
| 100.0 | 100.0000000001 | 1e-9 + 1e-9×100 = 1.01e-7 | Match (1e-10 < 1.01e-7) |
| 0.0 | 0.0000000001 | 1e-9 + 0 = 1e-9 | Match (1e-10 < 1e-9) |
| 100.0 | 100.001 | 1e-9 + 1e-9×100 = 1.01e-7 | Mismatch (0.001 > 1.01e-7) |

## Data Flow Diagram

```
                         ComparisonConfig
                               │
        ┌──────────────────────┼──────────────────────┐
        ▼                      ▼                      ▼
   ┌─────────┐           ┌─────────┐           ┌─────────┐
   │ load A  │           │ load B  │           │  keys   │
   └────┬────┘           └────┬────┘           └────┬────┘
        │                     │                     │
        └──────────┬──────────┘                     │
                   ▼                                │
           ┌──────────────┐                         │
           │ compare_schema│                        │
           └──────┬───────┘                         │
                  │                                 │
                  ▼                                 ▼
           ┌──────────────────────────────────────────┐
           │              analyze_keys                 │
           └──────────────────┬───────────────────────┘
                              │
                              ▼
           ┌──────────────────────────────────────────┐
           │            compare_values                 │
           │  ┌─────────────────────────────────────┐ │
           │  │  for each column:                   │ │
           │  │    1. exact match                   │ │
           │  │    2. null equivalence              │ │
           │  │    3. string normalization          │ │
           │  │    4. numeric tolerance             │ │
           │  │    5. mismatch                      │ │
           │  └─────────────────────────────────────┘ │
           └──────────────────┬───────────────────────┘
                              │
                              ▼
                    ┌──────────────────┐
                    │ ComparisonReport │
                    └────────┬─────────┘
                             │
              ┌──────────────┴──────────────┐
              ▼                             ▼
        ┌──────────┐                 ┌──────────────┐
        │  stdout  │                 │ report.md    │
        └──────────┘                 └──────────────┘
```

## Limitations and Edge Cases

### Duplicate Keys

If keys are not unique, the merge becomes a cross-product:
```
A: key=1, value=X
A: key=1, value=Y
B: key=1, value=Z

Merged:
key=1, A=X, B=Z
key=1, A=Y, B=Z  ← Both A rows match to the single B row
```

The tool warns about duplicates but still compares.

### Floating Point Edge Cases

- `NaN == NaN` → treated as match (unlike Python default)
- `Inf == Inf` → treated as match
- `-Inf == Inf` → mismatch

### Null Representations

The default null set is:
```python
["", "NA", "N/A", "NULL", "None", "NaN", "nan", "<NA>", "null", "NONE"]
```

All of these are treated as equivalent.

### Very Wide Tables

For tables with 1000+ columns:
- V2 may be slow due to per-column overhead
- V3 batches columns (default: 50 per query) for efficiency
- Adjust with `--batch-size` flag in V3
