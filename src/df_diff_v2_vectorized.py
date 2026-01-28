#!/usr/bin/env python3
"""
Data Comparison Tool - Version 2: Pandas Vectorized

Compares two rectangular datasets (CSV, Parquet) and produces
a comprehensive report of similarities and differences.

This version uses vectorized pandas/numpy operations. Fast for in-memory data.
Best for: Medium datasets (50K-1M rows) that fit in memory.

Usage:
    python data_compare_v2_vectorized.py file_a.csv file_b.parquet --key customer_id
"""

import argparse
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import numpy as np

# Regex matching zero-width Unicode characters that cause false mismatches
_ZERO_WIDTH_RE = re.compile('[\u0000\u200b\u200c\u200d\ufeff\u00ad\u2060\u180e]')


# =============================================================================
# Configuration
# =============================================================================

@dataclass
class ComparisonConfig:
    """Configuration for data comparison."""
    file_a: str
    file_b: str
    primary_key: list[str]
    
    # Null representations to normalize
    null_values: list[str] = field(default_factory=lambda: [
        "", "NA", "N/A", "NULL", "None", "NaN", "nan", "<NA>", "null", "NONE"
    ])
    
    # Numeric comparison tolerance
    numeric_atol: float = 1e-9  # Absolute tolerance
    numeric_rtol: float = 1e-9  # Relative tolerance
    
    # String normalization
    strip_whitespace: bool = True
    case_sensitive: bool = True
    
    # Output
    output_file: str | None = None
    max_sample_diffs: int = 10
    
    # Key handling
    allow_duplicate_keys: bool = False


# =============================================================================
# Data Structures for Results
# =============================================================================

@dataclass
class FileMetadata:
    path: str
    format: str
    size_bytes: int
    row_count: int
    column_count: int
    columns: list[str]


@dataclass
class SchemaComparison:
    column_count_match: bool
    column_names_match: bool
    column_order_match: bool
    columns_only_in_a: list[str]
    columns_only_in_b: list[str]
    common_columns: list[str]
    column_order_a: list[str]
    column_order_b: list[str]
    type_mismatches: list[tuple[str, str, str]]


@dataclass
class KeyAnalysis:
    key_columns: list[str]
    unique_in_a: bool
    unique_in_b: bool
    duplicate_count_a: int
    duplicate_count_b: int
    keys_in_both: int
    keys_only_in_a: int
    keys_only_in_b: int
    sample_keys_only_in_a: list[tuple]
    sample_keys_only_in_b: list[tuple]


@dataclass 
class ColumnValueComparison:
    column: str
    exact_match: int
    match_after_trim: int
    match_after_null_norm: int
    numeric_within_tolerance: int
    mismatch: int
    total_compared: int
    
    numeric_max_delta: float | None = None
    numeric_mean_delta: float | None = None
    
    sample_mismatches: list[dict] = field(default_factory=list)


@dataclass
class ValueComparison:
    total_rows_compared: int
    total_cells_compared: int
    columns: list[ColumnValueComparison]
    
    total_exact_match: int = 0
    total_match_after_norm: int = 0
    total_mismatch: int = 0


@dataclass
class ComparisonReport:
    timestamp: str
    config: ComparisonConfig
    file_a: FileMetadata
    file_b: FileMetadata
    schema: SchemaComparison
    keys: KeyAnalysis
    values: ValueComparison | None
    
    schema_verdict: str = ""
    coverage_verdict: str = ""
    value_verdict: str = ""
    overall_verdict: str = ""


# =============================================================================
# File Loading
# =============================================================================

def detect_format(filepath: str) -> str:
    """Detect file format from extension."""
    ext = Path(filepath).suffix.lower()
    format_map = {
        '.csv': 'csv',
        '.parquet': 'parquet',
        '.pq': 'parquet',
    }
    fmt = format_map.get(ext)
    if fmt is None:
        raise ValueError(f"Unsupported file format: {ext}. Only CSV and Parquet are supported.")
    return fmt


def validate_column_names(columns: list[str], filepath: str) -> None:
    """Check column names for potential issues and report warnings."""
    import re
    
    issues = {
        'starts_with_number': [],
        'has_spaces': [],
        'has_special_chars': [],
        'is_empty': [],
        'is_reserved_word': []
    }
    
    # Common SQL/pandas reserved words
    reserved_words = {
        'select', 'from', 'where', 'and', 'or', 'not', 'null', 'true', 'false',
        'in', 'is', 'as', 'on', 'join', 'left', 'right', 'inner', 'outer',
        'group', 'order', 'by', 'having', 'limit', 'offset', 'union', 'all',
        'insert', 'update', 'delete', 'create', 'drop', 'alter', 'table',
        'index', 'key', 'primary', 'foreign', 'references', 'constraint',
        'default', 'values', 'set', 'into', 'like', 'between', 'case', 'when',
        'then', 'else', 'end', 'distinct', 'count', 'sum', 'avg', 'min', 'max'
    }
    
    # Valid identifier pattern: starts with letter or underscore, followed by letters/numbers/underscores
    valid_pattern = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')
    
    for col in columns:
        col_str = str(col).strip()
        
        if not col_str:
            issues['is_empty'].append(col)
        elif col_str[0].isdigit():
            issues['starts_with_number'].append(col)
        elif ' ' in col_str:
            issues['has_spaces'].append(col)
        elif not valid_pattern.match(col_str):
            issues['has_special_chars'].append(col)
        
        if col_str.lower() in reserved_words:
            issues['is_reserved_word'].append(col)
    
    # Count total problematic columns
    all_issues = set()
    for issue_list in issues.values():
        all_issues.update(issue_list)
    
    if all_issues:
        print(f"\n  ⚠ Column name warnings for {filepath}:")
        print(f"    {len(all_issues)} of {len(columns)} columns have non-standard names")
        
        if issues['is_empty']:
            print(f"    - Empty names: {len(issues['is_empty'])}")
        if issues['starts_with_number']:
            examples = issues['starts_with_number'][:3]
            print(f"    - Start with number: {len(issues['starts_with_number'])} (e.g., {examples})")
        if issues['has_spaces']:
            examples = issues['has_spaces'][:3]
            print(f"    - Contain spaces: {len(issues['has_spaces'])} (e.g., {examples})")
        if issues['has_special_chars']:
            examples = issues['has_special_chars'][:3]
            print(f"    - Special characters: {len(issues['has_special_chars'])} (e.g., {examples})")
        if issues['is_reserved_word']:
            examples = issues['is_reserved_word'][:3]
            print(f"    - Reserved words: {len(issues['is_reserved_word'])} (e.g., {examples})")
        
        # Strong warning if ALL columns look like data
        if len(issues['starts_with_number']) == len(columns):
            print(f"\n    ❌ ALL columns start with numbers - first row is likely DATA, not headers!")
            print(f"       Columns found: {columns[:5]}{'...' if len(columns) > 5 else ''}")


def load_file(filepath: str, null_values: list[str]) -> pd.DataFrame:
    """Load a file into a DataFrame."""
    fmt = detect_format(filepath)
    
    if fmt == 'csv':
        df = pd.read_csv(filepath, dtype=str, na_values=null_values, keep_default_na=False)
    elif fmt == 'parquet':
        df = pd.read_parquet(filepath)
        # Normalize all null representations to pd.NA (matching CSV na_values behavior)
        null_str_map = {v: pd.NA for v in null_values}
        for col in df.columns:
            df[col] = df[col].astype(str).replace(null_str_map)
    else:
        raise ValueError(f"Unsupported file format: {fmt}")
    
    if len(df.columns) == 0:
        raise ValueError(f"File has no columns (headers required): {filepath}")
    
    # Validate column names
    validate_column_names(list(df.columns), filepath)

    # Strip zero-width Unicode characters from all values
    for col in df.columns:
        df[col] = df[col].str.replace(_ZERO_WIDTH_RE, '', regex=True)

    return df


def get_file_metadata(filepath: str, df: pd.DataFrame) -> FileMetadata:
    """Extract metadata from a file."""
    path = Path(filepath)
    return FileMetadata(
        path=str(path),
        format=detect_format(filepath),
        size_bytes=path.stat().st_size,
        row_count=len(df),
        column_count=len(df.columns),
        columns=list(df.columns)
    )


# =============================================================================
# Schema Comparison
# =============================================================================

def compare_schema(df_a: pd.DataFrame, df_b: pd.DataFrame) -> SchemaComparison:
    """Compare the schemas of two DataFrames."""
    cols_a = set(df_a.columns)
    cols_b = set(df_b.columns)
    
    common = cols_a & cols_b
    only_in_a = cols_a - cols_b
    only_in_b = cols_b - cols_a
    
    order_a = [c for c in df_a.columns if c in common]
    order_b = [c for c in df_b.columns if c in common]
    
    type_mismatches = []
    for col in common:
        type_a = str(df_a[col].dtype)
        type_b = str(df_b[col].dtype)
        if type_a != type_b:
            type_mismatches.append((col, type_a, type_b))
    
    return SchemaComparison(
        column_count_match=(len(df_a.columns) == len(df_b.columns)),
        column_names_match=(cols_a == cols_b),
        column_order_match=(order_a == order_b),
        columns_only_in_a=sorted(only_in_a),
        columns_only_in_b=sorted(only_in_b),
        common_columns=sorted(common),
        column_order_a=list(df_a.columns),
        column_order_b=list(df_b.columns),
        type_mismatches=type_mismatches
    )


# =============================================================================
# Primary Key Analysis
# =============================================================================

def analyze_keys(
    df_a: pd.DataFrame, 
    df_b: pd.DataFrame, 
    key_columns: list[str],
    max_samples: int
) -> KeyAnalysis:
    """Analyze primary keys in both DataFrames."""
    
    for col in key_columns:
        if col not in df_a.columns:
            raise ValueError(f"Key column '{col}' not found in file A")
        if col not in df_b.columns:
            raise ValueError(f"Key column '{col}' not found in file B")
    
    if len(key_columns) == 1:
        keys_a = set(df_a[key_columns[0]].astype(str))
        keys_b = set(df_b[key_columns[0]].astype(str))
        key_series_a = df_a[key_columns[0]].astype(str)
        key_series_b = df_b[key_columns[0]].astype(str)
    else:
        keys_a = set(df_a[key_columns].astype(str).apply(tuple, axis=1))
        keys_b = set(df_b[key_columns].astype(str).apply(tuple, axis=1))
        key_series_a = df_a[key_columns].astype(str).apply(tuple, axis=1)
        key_series_b = df_b[key_columns].astype(str).apply(tuple, axis=1)
    
    dup_a = key_series_a.duplicated().sum()
    dup_b = key_series_b.duplicated().sum()
    
    in_both = keys_a & keys_b
    only_a = keys_a - keys_b
    only_b = keys_b - keys_a
    
    sample_only_a = [k if isinstance(k, tuple) else (k,) for k in list(only_a)[:max_samples]]
    sample_only_b = [k if isinstance(k, tuple) else (k,) for k in list(only_b)[:max_samples]]
    
    return KeyAnalysis(
        key_columns=key_columns,
        unique_in_a=(dup_a == 0),
        unique_in_b=(dup_b == 0),
        duplicate_count_a=int(dup_a),
        duplicate_count_b=int(dup_b),
        keys_in_both=len(in_both),
        keys_only_in_a=len(only_a),
        keys_only_in_b=len(only_b),
        sample_keys_only_in_a=sample_only_a,
        sample_keys_only_in_b=sample_only_b
    )


# =============================================================================
# Value Comparison - VECTORIZED (FAST)
# =============================================================================

def compare_values(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    key_columns: list[str],
    columns_to_compare: list[str],
    config: ComparisonConfig
) -> ValueComparison:
    """Compare values using vectorized pandas/numpy operations."""
    
    key_col_name = '__comparison_key__'
    
    df_a = df_a.copy()
    df_b = df_b.copy()
    
    if len(key_columns) == 1:
        df_a[key_col_name] = df_a[key_columns[0]].astype(str)
        df_b[key_col_name] = df_b[key_columns[0]].astype(str)
    else:
        df_a[key_col_name] = df_a[key_columns].astype(str).apply(lambda x: '|||'.join(x), axis=1)
        df_b[key_col_name] = df_b[key_columns].astype(str).apply(lambda x: '|||'.join(x), axis=1)
    
    merged = pd.merge(
        df_a[[key_col_name] + columns_to_compare],
        df_b[[key_col_name] + columns_to_compare],
        on=key_col_name,
        suffixes=('_A', '_B')
    )
    
    total_rows = len(merged)
    column_results = []
    
    total_exact = 0
    total_norm = 0
    total_mismatch = 0
    
    null_values_set = set(config.null_values)
    value_columns = [col for col in columns_to_compare if col not in key_columns]
    num_cols = len(value_columns)
    
    for i, col in enumerate(value_columns):
        if num_cols > 10 and (i + 1) % 25 == 0:
            print(f"  Comparing column {i + 1}/{num_cols}: {col}")
        
        col_a = f"{col}_A"
        col_b = f"{col}_B"
        
        # Get series as strings
        series_a = merged[col_a].astype(str)
        series_b = merged[col_b].astype(str)
        orig_a = merged[col_a]
        orig_b = merged[col_b]
        
        # =================================================================
        # STEP 1: Exact string match (vectorized)
        # =================================================================
        exact_match_mask = (series_a == series_b)
        
        # =================================================================
        # STEP 2: Null normalization (vectorized)
        # =================================================================
        remaining = ~exact_match_mask
        
        def is_null_like(series, orig_series):
            is_pd_null = orig_series.isna()
            str_vals = series.str.strip()
            is_str_null = str_vals.isin(null_values_set)
            return is_pd_null | is_str_null
        
        null_a = is_null_like(series_a, orig_a)
        null_b = is_null_like(series_b, orig_b)
        
        both_null_mask = remaining & null_a & null_b
        
        # =================================================================
        # STEP 3: String normalization (vectorized)
        # =================================================================
        remaining2 = remaining & ~both_null_mask
        
        if config.strip_whitespace:
            trimmed_a = series_a.str.strip()
            trimmed_b = series_b.str.strip()
        else:
            trimmed_a = series_a
            trimmed_b = series_b
        
        if not config.case_sensitive:
            trimmed_a = trimmed_a.str.lower()
            trimmed_b = trimmed_b.str.lower()
        
        non_null_both = ~null_a & ~null_b
        trim_match_mask = remaining2 & non_null_both & (trimmed_a == trimmed_b)
        
        # =================================================================
        # STEP 4: Numeric tolerance (vectorized)
        # =================================================================
        remaining3 = remaining2 & ~trim_match_mask
        
        num_a = pd.to_numeric(trimmed_a, errors='coerce')
        num_b = pd.to_numeric(trimmed_b, errors='coerce')
        
        both_numeric = remaining3 & num_a.notna() & num_b.notna()
        
        if both_numeric.any():
            delta = (num_a - num_b).abs()
            tolerance_threshold = config.numeric_atol + config.numeric_rtol * num_b.abs()
            within_tol = delta <= tolerance_threshold
            numeric_match_mask = both_numeric & within_tol
            
            matched_deltas = delta[numeric_match_mask]
            numeric_max_delta = float(matched_deltas.max()) if len(matched_deltas) > 0 else None
            numeric_mean_delta = float(matched_deltas.mean()) if len(matched_deltas) > 0 else None
        else:
            numeric_match_mask = pd.Series([False] * total_rows, index=merged.index)
            numeric_max_delta = None
            numeric_mean_delta = None
        
        # =================================================================
        # STEP 4b: Special float values (Inf, NaN)
        # =================================================================
        # inf - inf = nan in IEEE 754, failing the tolerance check above.
        # NaN strings not in null_values_set are excluded by notna() above.
        # Handle both explicitly, matching V1's try_numeric_compare.
        remaining4 = remaining3 & ~numeric_match_mask

        num_a_safe = num_a.fillna(0)
        num_b_safe = num_b.fillna(0)
        both_inf_same_sign = (remaining4
                              & np.isinf(num_a_safe) & np.isinf(num_b_safe)
                              & (np.sign(num_a_safe) == np.sign(num_b_safe)))

        both_nan_str = (remaining4
                        & trimmed_a.str.lower().eq('nan')
                        & trimmed_b.str.lower().eq('nan'))

        special_float_mask = both_inf_same_sign | both_nan_str
        numeric_match_mask = numeric_match_mask | special_float_mask

        # =================================================================
        # STEP 5: Everything else is a mismatch
        # =================================================================
        mismatch_mask = remaining3 & ~numeric_match_mask
        
        # =================================================================
        # Count results
        # =================================================================
        exact_match_count = int(exact_match_mask.sum())
        null_match_count = int(both_null_mask.sum())
        trim_match_count = int(trim_match_mask.sum())
        numeric_match_count = int(numeric_match_mask.sum())
        mismatch_count = int(mismatch_mask.sum())
        
        # =================================================================
        # Collect sample mismatches
        # =================================================================
        sample_mismatches = []
        if mismatch_count > 0:
            mismatch_indices = merged.index[mismatch_mask][:config.max_sample_diffs]
            for idx in mismatch_indices:
                sample_mismatches.append({
                    'key': merged.loc[idx, key_col_name],
                    'value_a': series_a.loc[idx],
                    'value_b': series_b.loc[idx]
                })
        
        col_result = ColumnValueComparison(
            column=col,
            exact_match=exact_match_count,
            match_after_trim=trim_match_count,
            match_after_null_norm=null_match_count,
            numeric_within_tolerance=numeric_match_count,
            mismatch=mismatch_count,
            total_compared=total_rows,
            numeric_max_delta=numeric_max_delta,
            numeric_mean_delta=numeric_mean_delta,
            sample_mismatches=sample_mismatches
        )
        column_results.append(col_result)
        
        total_exact += exact_match_count
        total_norm += trim_match_count + null_match_count + numeric_match_count
        total_mismatch += mismatch_count
    
    return ValueComparison(
        total_rows_compared=total_rows,
        total_cells_compared=total_rows * len(value_columns),
        columns=column_results,
        total_exact_match=total_exact,
        total_match_after_norm=total_norm,
        total_mismatch=total_mismatch
    )


# =============================================================================
# Report Generation
# =============================================================================

def format_bytes(size: int) -> str:
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def format_number(n: int) -> str:
    return f"{n:,}"


def generate_report(report: ComparisonReport) -> str:
    """Generate a markdown report."""
    lines = []
    
    lines.append("# Data Comparison Report")
    lines.append("")
    lines.append(f"**Generated:** {report.timestamp}")
    lines.append(f"**Engine:** Pandas Vectorized (v2)")
    lines.append("")
    
    lines.append("## 1. File Metadata")
    lines.append("")
    lines.append("| Property | File A | File B |")
    lines.append("|----------|--------|--------|")
    lines.append(f"| Path | `{report.file_a.path}` | `{report.file_b.path}` |")
    lines.append(f"| Format | {report.file_a.format} | {report.file_b.format} |")
    lines.append(f"| Size | {format_bytes(report.file_a.size_bytes)} | {format_bytes(report.file_b.size_bytes)} |")
    lines.append(f"| Rows | {format_number(report.file_a.row_count)} | {format_number(report.file_b.row_count)} |")
    lines.append(f"| Columns | {report.file_a.column_count} | {report.file_b.column_count} |")
    lines.append("")
    
    schema = report.schema
    lines.append("## 2. Schema Comparison")
    lines.append("")
    
    if schema.column_count_match:
        lines.append(f"✓ Column count matches: {report.file_a.column_count}")
    else:
        lines.append(f"✗ Column count differs: A has {report.file_a.column_count}, B has {report.file_b.column_count}")
    lines.append("")
    
    if schema.column_names_match:
        lines.append("✓ Column names match")
    else:
        lines.append("✗ Column names differ")
        if schema.columns_only_in_a:
            lines.append(f"  - Only in A: {schema.columns_only_in_a}")
        if schema.columns_only_in_b:
            lines.append(f"  - Only in B: {schema.columns_only_in_b}")
    lines.append("")
    
    if schema.column_names_match:
        if schema.column_order_match:
            lines.append("✓ Column order matches")
        else:
            lines.append("⚠ Column order differs (names match but order is different)")
            lines.append(f"  - Order in A: {schema.column_order_a}")
            lines.append(f"  - Order in B: {schema.column_order_b}")
    lines.append("")
    
    if schema.type_mismatches:
        lines.append("### Type Differences")
        lines.append("")
        lines.append("| Column | Type in A | Type in B |")
        lines.append("|--------|-----------|-----------|")
        for col, type_a, type_b in schema.type_mismatches:
            lines.append(f"| {col} | {type_a} | {type_b} |")
        lines.append("")
    
    keys = report.keys
    lines.append("## 3. Primary Key Analysis")
    lines.append("")
    key_str = ", ".join(f"`{k}`" for k in keys.key_columns)
    lines.append(f"**Key column(s):** {key_str}")
    lines.append("")
    
    lines.append("### Uniqueness")
    if keys.unique_in_a:
        lines.append("✓ Keys are unique in File A")
    else:
        lines.append(f"⚠ Keys have {format_number(keys.duplicate_count_a)} duplicates in File A")
    if keys.unique_in_b:
        lines.append("✓ Keys are unique in File B")
    else:
        lines.append(f"⚠ Keys have {format_number(keys.duplicate_count_b)} duplicates in File B")
    lines.append("")
    
    lines.append("### Key Coverage")
    lines.append("")
    lines.append(f"- Keys in both files: **{format_number(keys.keys_in_both)}**")
    lines.append(f"- Keys only in A: **{format_number(keys.keys_only_in_a)}**")
    lines.append(f"- Keys only in B: **{format_number(keys.keys_only_in_b)}**")
    lines.append("")
    
    if keys.sample_keys_only_in_a:
        lines.append("#### Sample keys only in A:")
        for k in keys.sample_keys_only_in_a[:5]:
            lines.append(f"  - `{k}`")
        lines.append("")
    
    if keys.sample_keys_only_in_b:
        lines.append("#### Sample keys only in B:")
        for k in keys.sample_keys_only_in_b[:5]:
            lines.append(f"  - `{k}`")
        lines.append("")
    
    if report.values:
        values = report.values
        lines.append("## 4. Value Comparison")
        lines.append("")
        lines.append(f"Comparing **{format_number(values.total_rows_compared)}** matched rows across **{len(values.columns)}** non-key columns")
        lines.append(f"(**{format_number(values.total_cells_compared)}** total cell comparisons)")
        lines.append("")
        
        lines.append("### Summary by Column")
        lines.append("")
        lines.append("| Column | Exact | Trimmed | Null Eq. | Numeric Tol. | Mismatch |")
        lines.append("|--------|-------|---------|----------|--------------|----------|")
        for col in values.columns:
            lines.append(
                f"| {col.column} | {format_number(col.exact_match)} | {format_number(col.match_after_trim)} | "
                f"{format_number(col.match_after_null_norm)} | {format_number(col.numeric_within_tolerance)} | {format_number(col.mismatch)} |"
            )
        lines.append("")
        
        lines.append("### Aggregate Totals")
        lines.append("")
        lines.append(f"- Exact matches: **{format_number(values.total_exact_match)}**")
        lines.append(f"- Matches after normalization: **{format_number(values.total_match_after_norm)}**")
        lines.append(f"- Mismatches: **{format_number(values.total_mismatch)}**")
        lines.append("")
        
        cols_with_norm = [c for c in values.columns if c.match_after_trim > 0 or c.match_after_null_norm > 0 or c.numeric_within_tolerance > 0]
        if cols_with_norm:
            lines.append("### Normalization Details")
            lines.append("")
            for col in cols_with_norm:
                lines.append(f"**{col.column}:**")
                if col.match_after_trim > 0:
                    lines.append(f"  - Whitespace/case normalization resolved: {format_number(col.match_after_trim)}")
                if col.match_after_null_norm > 0:
                    lines.append(f"  - Null representation resolved: {format_number(col.match_after_null_norm)}")
                if col.numeric_within_tolerance > 0:
                    lines.append(f"  - Numeric tolerance resolved: {format_number(col.numeric_within_tolerance)}")
                    if col.numeric_max_delta is not None:
                        lines.append(f"    - Max delta: {col.numeric_max_delta:.2e}")
                    if col.numeric_mean_delta is not None:
                        lines.append(f"    - Mean delta: {col.numeric_mean_delta:.2e}")
                lines.append("")
        
        cols_with_mismatch = [c for c in values.columns if c.mismatch > 0]
        if cols_with_mismatch:
            lines.append("### Mismatch Details")
            lines.append("")
            for col in cols_with_mismatch:
                lines.append(f"**{col.column}:** {format_number(col.mismatch)} mismatches")
                if col.sample_mismatches:
                    lines.append("")
                    lines.append("| Key | Value A | Value B |")
                    lines.append("|-----|---------|---------|")
                    for sample in col.sample_mismatches[:10]:
                        val_a = str(sample['value_a'])[:50]
                        val_b = str(sample['value_b'])[:50]
                        lines.append(f"| `{sample['key']}` | `{val_a}` | `{val_b}` |")
                lines.append("")
    
    lines.append("## 5. Summary Verdict")
    lines.append("")
    lines.append(f"- **Schema:** {report.schema_verdict}")
    lines.append(f"- **Row Coverage:** {report.coverage_verdict}")
    lines.append(f"- **Values:** {report.value_verdict}")
    lines.append("")
    lines.append(f"### Overall: **{report.overall_verdict}**")
    lines.append("")
    
    return "\n".join(lines)


def generate_stdout_summary(report: ComparisonReport) -> str:
    lines = []
    lines.append("=" * 60)
    lines.append("DATA COMPARISON SUMMARY (Pandas Vectorized)")
    lines.append("=" * 60)
    lines.append("")
    lines.append(f"File A: {report.file_a.path} ({format_number(report.file_a.row_count)} rows)")
    lines.append(f"File B: {report.file_b.path} ({format_number(report.file_b.row_count)} rows)")
    lines.append("")
    lines.append(f"Schema:       {report.schema_verdict}")
    lines.append(f"Row Coverage: {report.coverage_verdict}")
    lines.append(f"Values:       {report.value_verdict}")
    lines.append("")
    lines.append("-" * 60)
    lines.append(f"OVERALL: {report.overall_verdict}")
    lines.append("-" * 60)

    # Show schema differences if columns don't match
    if not report.schema.column_names_match:
        lines.append("")
        lines.append(f"⚠ Schema differences detected:")
        if report.schema.columns_only_in_a:
            lines.append(f"  Columns only in A ({len(report.schema.columns_only_in_a)}):")
            for col in report.schema.columns_only_in_a[:10]:
                lines.append(f"    - {col}")
            if len(report.schema.columns_only_in_a) > 10:
                lines.append(f"    ... and {len(report.schema.columns_only_in_a) - 10} more")
        if report.schema.columns_only_in_b:
            lines.append(f"  Columns only in B ({len(report.schema.columns_only_in_b)}):")
            for col in report.schema.columns_only_in_b[:10]:
                lines.append(f"    - {col}")
            if len(report.schema.columns_only_in_b) > 10:
                lines.append(f"    ... and {len(report.schema.columns_only_in_b) - 10} more")

    if report.values and report.values.total_mismatch > 0:
        lines.append("")
        lines.append(f"⚠ {format_number(report.values.total_mismatch)} cell mismatches found")
        cols_with_issues = [c for c in report.values.columns if c.mismatch > 0]
        if cols_with_issues:
            lines.append("  Columns with mismatches:")
            for col in cols_with_issues[:5]:
                lines.append(f"    - {col.column}: {format_number(col.mismatch)}")
    
    return "\n".join(lines)


# =============================================================================
# Main Comparison Logic
# =============================================================================

def run_comparison(config: ComparisonConfig) -> ComparisonReport:
    """Run the full comparison and return a report."""
    
    print(f"Loading {config.file_a}...")
    df_a = load_file(config.file_a, config.null_values)
    meta_a = get_file_metadata(config.file_a, df_a)
    
    print(f"Loading {config.file_b}...")
    df_b = load_file(config.file_b, config.null_values)
    meta_b = get_file_metadata(config.file_b, df_b)
    
    print(f"  File A: {format_number(meta_a.row_count)} rows × {meta_a.column_count} columns")
    print(f"  File B: {format_number(meta_b.row_count)} rows × {meta_b.column_count} columns")
    
    print("Comparing schemas...")
    schema = compare_schema(df_a, df_b)
    
    print("Analyzing primary keys...")
    keys = analyze_keys(df_a, df_b, config.primary_key, config.max_sample_diffs)
    
    # Check for duplicate keys
    if not keys.unique_in_a or not keys.unique_in_b:
        if not config.allow_duplicate_keys:
            msg = "Duplicate keys found: "
            if not keys.unique_in_a:
                msg += f"File A has {keys.duplicate_count_a} duplicates. "
            if not keys.unique_in_b:
                msg += f"File B has {keys.duplicate_count_b} duplicates. "
            msg += "Use --allow-duplicate-keys to proceed anyway."
            raise ValueError(msg)
        else:
            print(f"  ⚠ Warning: Duplicate keys found (proceeding due to --allow-duplicate-keys)")
    
    print(f"  Keys in both: {format_number(keys.keys_in_both)}")
    
    values = None
    if schema.common_columns and keys.keys_in_both > 0:
        print(f"Comparing values for {format_number(keys.keys_in_both)} matched rows (vectorized)...")
        values = compare_values(df_a, df_b, config.primary_key, schema.common_columns, config)
        print(f"  Exact matches: {format_number(values.total_exact_match)}")
        print(f"  After normalization: {format_number(values.total_match_after_norm)}")
        print(f"  Mismatches: {format_number(values.total_mismatch)}")
    
    # Generate verdicts
    if schema.column_names_match and schema.column_order_match:
        schema_verdict = "✓ MATCH"
    elif schema.column_names_match:
        schema_verdict = "⚠ MATCH (columns reordered)"
    else:
        schema_verdict = "✗ MISMATCH"
    
    if keys.keys_only_in_a == 0 and keys.keys_only_in_b == 0:
        coverage_verdict = "✓ MATCH (all keys present in both)"
    else:
        total_diff = keys.keys_only_in_a + keys.keys_only_in_b
        coverage_verdict = f"⚠ {format_number(total_diff)} keys differ ({format_number(keys.keys_only_in_a)} only in A, {format_number(keys.keys_only_in_b)} only in B)"
    
    if values is None:
        value_verdict = "N/A (no comparable data)"
    elif values.total_mismatch == 0:
        if values.total_match_after_norm == 0:
            value_verdict = "✓ EXACT MATCH"
        else:
            value_verdict = "✓ MATCH (after normalization)"
    else:
        pct = (values.total_mismatch / values.total_cells_compared) * 100
        value_verdict = f"✗ {format_number(values.total_mismatch)} mismatches ({pct:.4f}%)"
    
    if (schema.column_names_match and 
        keys.keys_only_in_a == 0 and keys.keys_only_in_b == 0 and
        values and values.total_mismatch == 0):
        if values.total_match_after_norm == 0 and schema.column_order_match:
            overall_verdict = "IDENTICAL"
        else:
            overall_verdict = "EQUIVALENT (with normalization/reordering)"
    elif values and values.total_mismatch == 0:
        overall_verdict = "EQUIVALENT (row/schema differences only)"
    else:
        overall_verdict = "DIFFERENT"
    
    return ComparisonReport(
        timestamp=datetime.now().isoformat(),
        config=config,
        file_a=meta_a,
        file_b=meta_b,
        schema=schema,
        keys=keys,
        values=values,
        schema_verdict=schema_verdict,
        coverage_verdict=coverage_verdict,
        value_verdict=value_verdict,
        overall_verdict=overall_verdict
    )


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Compare two data files (CSV/Parquet) - Vectorized version.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python data_compare_v2_vectorized.py sales_sql.csv sales_spark.parquet --key customer_id
  python data_compare_v2_vectorized.py a.csv b.parquet --key id --key date --output report.md
        """
    )
    
    parser.add_argument("file_a", help="Path to first file (CSV or Parquet)")
    parser.add_argument("file_b", help="Path to second file (CSV or Parquet)")
    parser.add_argument("--key", "-k", action="append", required=True, dest="keys",
                        help="Primary key column(s). Specify multiple times for composite keys.")
    parser.add_argument("--output", "-o", help="Output file for detailed report (markdown)")
    parser.add_argument("--numeric-tolerance", type=float, default=1e-9,
                        help="Absolute tolerance for numeric comparison (default: 1e-9)")
    parser.add_argument("--no-strip", action="store_true",
                        help="Disable whitespace stripping")
    parser.add_argument("--ignore-case", action="store_true",
                        help="Case-insensitive string comparison")
    parser.add_argument("--max-samples", type=int, default=10,
                        help="Maximum sample differences to show per column (default: 10)")
    parser.add_argument("--allow-duplicate-keys", action="store_true",
                        help="Continue even if primary keys are not unique")
    
    args = parser.parse_args()
    
    config = ComparisonConfig(
        file_a=args.file_a,
        file_b=args.file_b,
        primary_key=args.keys,
        numeric_atol=args.numeric_tolerance,
        numeric_rtol=args.numeric_tolerance,
        strip_whitespace=not args.no_strip,
        case_sensitive=not args.ignore_case,
        output_file=args.output,
        max_sample_diffs=args.max_samples,
        allow_duplicate_keys=args.allow_duplicate_keys
    )
    
    try:
        report = run_comparison(config)
        
        print()
        print(generate_stdout_summary(report))
        
        output_path = config.output_file or "comparison_report.md"
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(generate_report(report))
        
        print()
        print(f"Detailed report written to: {output_path}")
        
        if report.overall_verdict in ["IDENTICAL", "EQUIVALENT (with normalization/reordering)"]:
            sys.exit(0)
        else:
            sys.exit(1)
            
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(2)


if __name__ == "__main__":
    main()
