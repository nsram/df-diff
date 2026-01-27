# Data Comparison Report

**Generated:** 2025-01-24T10:30:00.000000
**Engine:** Pandas Vectorized (v2)

## 1. File Metadata

| Property | File A | File B |
|----------|--------|--------|
| Path | `sales_sql_export.csv` | `sales_spark_export.parquet` |
| Format | csv | parquet |
| Size | 45.2 MB | 12.8 MB |
| Rows | 150,432 | 150,430 |
| Columns | 24 | 24 |

## 2. Schema Comparison

✓ Column count matches: 24

✓ Column names match

✓ Column order matches

### Type Differences

| Column | Type in A | Type in B |
|--------|-----------|-----------|
| revenue | object | double |
| quantity | object | int64 |
| discount | object | double |

## 3. Primary Key Analysis

**Key column(s):** `customer_id`, `order_date`

### Uniqueness
✓ Keys are unique in File A
✓ Keys are unique in File B

### Key Coverage

- Keys in both files: **150,428**
- Keys only in A: **4**
- Keys only in B: **2**

#### Sample keys only in A:
  - `('C-10042', '2025-01-15')`
  - `('C-10099', '2025-01-16')`
  - `('C-10123', '2025-01-17')`
  - `('C-10456', '2025-01-18')`

#### Sample keys only in B:
  - `('C-99901', '2025-01-19')`
  - `('C-99902', '2025-01-19')`

## 4. Value Comparison

Comparing **150,428** matched rows across **22** non-key columns
(**3,309,416** total cell comparisons)

### Summary by Column

| Column | Exact | Trimmed | Null Eq. | Numeric Tol. | Mismatch |
|--------|-------|---------|----------|--------------|----------|
| customer_name | 148,000 | 2,428 | 0 | 0 | 0 |
| email | 150,428 | 0 | 0 | 0 | 0 |
| phone | 150,400 | 0 | 28 | 0 | 0 |
| address | 149,500 | 928 | 0 | 0 | 0 |
| city | 150,428 | 0 | 0 | 0 | 0 |
| state | 150,428 | 0 | 0 | 0 | 0 |
| zip | 150,428 | 0 | 0 | 0 | 0 |
| country | 150,428 | 0 | 0 | 0 | 0 |
| product_id | 150,428 | 0 | 0 | 0 | 0 |
| product_name | 150,000 | 428 | 0 | 0 | 0 |
| category | 150,428 | 0 | 0 | 0 | 0 |
| quantity | 150,428 | 0 | 0 | 0 | 0 |
| unit_price | 150,000 | 0 | 0 | 428 | 0 |
| discount | 150,400 | 0 | 28 | 0 | 0 |
| revenue | 149,500 | 0 | 0 | 928 | 0 |
| cost | 150,428 | 0 | 0 | 0 | 0 |
| profit | 149,500 | 0 | 0 | 928 | 0 |
| order_status | 150,428 | 0 | 0 | 0 | 0 |
| ship_date | 150,428 | 0 | 0 | 0 | 0 |
| ship_mode | 150,428 | 0 | 0 | 0 | 0 |
| region | 150,000 | 428 | 0 | 0 | 0 |
| sales_rep | 148,000 | 2,428 | 0 | 0 | 0 |

### Aggregate Totals

- Exact matches: **3,298,904**
- Matches after normalization: **10,512**
- Mismatches: **0**

### Normalization Details

**customer_name:**
  - Whitespace/case normalization resolved: 2,428

**phone:**
  - Null representation resolved: 28

**address:**
  - Whitespace/case normalization resolved: 928

**product_name:**
  - Whitespace/case normalization resolved: 428

**unit_price:**
  - Numeric tolerance resolved: 428
    - Max delta: 1.00e-09
    - Mean delta: 4.52e-10

**discount:**
  - Null representation resolved: 28

**revenue:**
  - Numeric tolerance resolved: 928
    - Max delta: 9.99e-10
    - Mean delta: 5.12e-10

**profit:**
  - Numeric tolerance resolved: 928
    - Max delta: 9.99e-10
    - Mean delta: 5.08e-10

**region:**
  - Whitespace/case normalization resolved: 428

**sales_rep:**
  - Whitespace/case normalization resolved: 2,428

## 5. Summary Verdict

- **Schema:** ✓ MATCH
- **Row Coverage:** ⚠ 6 keys differ (4 only in A, 2 only in B)
- **Values:** ✓ MATCH (after normalization)

### Overall: **EQUIVALENT (with normalization/reordering)**
