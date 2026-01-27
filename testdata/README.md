# Test Data Files for Data Comparison Tool

## Files

| File | Description | Expected Result vs `data_a_sql.csv` |
|------|-------------|-------------------------------------|
| `data_a_sql.csv` | Source of truth (simulates SQL export) | N/A |
| `data_b_identical.csv` | Exact copy of A | **IDENTICAL** |
| `data_c_equivalent.csv` | Same data with normalization differences | **EQUIVALENT** |
| `data_d_different.csv` | Actual differences in data | **DIFFERENT** |

## Test Commands

```bash
# Test 1: Identical files → IDENTICAL
python df_diff_v2_vectorized.py data_a_sql.csv data_b_identical.csv --key customer_id

# Test 2: Equivalent after normalization → EQUIVALENT
python df_diff_v2_vectorized.py data_a_sql.csv data_c_equivalent.csv --key customer_id

# Test 3: Different data → DIFFERENT
python df_diff_v2_vectorized.py data_a_sql.csv data_d_different.csv --key customer_id
```

## Differences in `data_c_equivalent.csv` (should normalize to match)

- Whitespace in sales_rep: `"  John Smith  "` vs `"John Smith"`
- Whitespace in region: `"  Northeast  "` vs `"Northeast"`  
- Whitespace in sales_rep: `"  Bob Wilson"` vs `"Bob Wilson"`
- Tiny numeric in unit_price: `49.990000001` vs `49.99`
- Tiny numeric in unit_price: `29.990000001` vs `29.99`

## Differences in `data_d_different.csv` (actual mismatches)

### Missing/Extra Rows
- Missing from D: customer_id `1005` (Gadget Y order)
- Extra in D: customer_id `1011` (Widget D order)

### Value Mismatches
| customer_id | Field | Value in A | Value in D |
|-------------|-------|------------|------------|
| 1004 | quantity | 20 | 25 |
| 1008 | unit_price | 49.99 | 55.99 |
| 1009 | region | Northeast | Southeast |

## Primary Key

All files use `customer_id` as the primary key.
