#!/usr/bin/env python3
"""
Data Comparison Tool - Version 1: Pandas Row-by-Row

Compares two rectangular datasets (CSV, Parquet) and produces
a comprehensive report of similarities and differences.

This version uses iterative row-by-row comparison. Simple but slow.
Best for: Small datasets (<50K rows), debugging, reference implementation.

Usage:
    python data_compare_v1_slow.py file_a.csv file_b.parquet --key customer_id
"""

import argparse
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import numpy as np


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
    type_mismatches: list[tuple[str, str, str]]  # (column, type_a, type_b)


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
    
    # Numeric stats (if applicable)
    numeric_max_delta: float | None = None
    numeric_mean_delta: float | None = None
    
    # Sample mismatches
    sample_mismatches: list[dict] = field(default_factory=list)


@dataclass
class ValueComparison:
    total_rows_compared: int
    total_cells_compared: int
    columns: list[ColumnValueComparison]
    
    # Aggregate counts
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
    
    # Overall verdict
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
        # Convert to string for consistent comparison
        for col in df.columns:
            df[col] = df[col].astype(str).replace({'nan': pd.NA, 'None': pd.NA})
    else:
        raise ValueError(f"Unsupported file format: {fmt}")
    
    if len(df.columns) == 0:
        raise ValueError(f"File has no columns (headers required): {filepath}")
    
    # Validate column names
    validate_column_names(list(df.columns), filepath)
    
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
    
    # Check order for common columns
    order_a = [c for c in df_a.columns if c in common]
    order_b = [c for c in df_b.columns if c in common]
    
    # Type comparison for common columns
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
    
    # Check key columns exist
    for col in key_columns:
        if col not in df_a.columns:
            raise ValueError(f"Key column '{col}' not found in file A")
        if col not in df_b.columns:
            raise ValueError(f"Key column '{col}' not found in file B")
    
    # Create key tuples
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
    
    # Check uniqueness
    dup_a = key_series_a.duplicated().sum()
    dup_b = key_series_b.duplicated().sum()
    
    # Key overlap
    in_both = keys_a & keys_b
    only_a = keys_a - keys_b
    only_b = keys_b - keys_a
    
    # Sample keys
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
# Value Comparison - ROW BY ROW (SLOW)
# =============================================================================

def normalize_null(value: Any, null_values: list[str]) -> Any:
    """Normalize null representations."""
    if pd.isna(value):
        return None
    if isinstance(value, str) and value.strip() in null_values:
        return None
    return value


def normalize_string(value: Any, strip_whitespace: bool, case_sensitive: bool) -> Any:
    """Normalize string values."""
    if value is None or pd.isna(value):
        return None
    if isinstance(value, str):
        if strip_whitespace:
            value = value.strip()
        if not case_sensitive:
            value = value.lower()
    return value


def try_numeric_compare(val_a: Any, val_b: Any, atol: float, rtol: float) -> tuple[bool, float | None]:
    """Try to compare values as numbers. Returns (is_match, delta)."""
    try:
        num_a = float(val_a)
        num_b = float(val_b)
        
        # Handle inf/nan
        if np.isnan(num_a) and np.isnan(num_b):
            return True, 0.0
        if np.isinf(num_a) and np.isinf(num_b) and np.sign(num_a) == np.sign(num_b):
            return True, 0.0
            
        delta = abs(num_a - num_b)
        is_close = np.isclose(num_a, num_b, atol=atol, rtol=rtol)
        return is_close, delta
    except (ValueError, TypeError):
        return False, None


def compare_values(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    key_columns: list[str],
    columns_to_compare: list[str],
    config: ComparisonConfig
) -> ValueComparison:
    """Compare values between two DataFrames on matched keys. ROW-BY-ROW version."""
    
    # Create key for joining
    key_col_name = '__comparison_key__'
    
    df_a = df_a.copy()
    df_b = df_b.copy()
    
    if len(key_columns) == 1:
        df_a[key_col_name] = df_a[key_columns[0]].astype(str)
        df_b[key_col_name] = df_b[key_columns[0]].astype(str)
    else:
        df_a[key_col_name] = df_a[key_columns].astype(str).apply(lambda x: '|||'.join(x), axis=1)
        df_b[key_col_name] = df_b[key_columns].astype(str).apply(lambda x: '|||'.join(x), axis=1)
    
    # Inner join on key
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
    
    # Get non-key columns
    value_columns = [c for c in columns_to_compare if c not in key_columns]
    num_cols = len(value_columns)
    
    for i, col in enumerate(value_columns):
        if num_cols > 10 and (i + 1) % 10 == 0:
            print(f"  Comparing column {i + 1}/{num_cols}: {col}")
            
        col_a = f"{col}_A"
        col_b = f"{col}_B"
        
        exact_match = 0
        match_after_trim = 0
        match_after_null = 0
        numeric_tolerance = 0
        mismatch = 0
        
        numeric_deltas = []
        sample_mismatches = []
        
        for idx, row in merged.iterrows():
            val_a = row[col_a]
            val_b = row[col_b]
            key_val = row[key_col_name]
            
            # 1. Exact string match
            str_a = str(val_a) if not pd.isna(val_a) else None
            str_b = str(val_b) if not pd.isna(val_b) else None
            
            if str_a == str_b:
                exact_match += 1
                continue
            
            # 2. Null normalization
            norm_null_a = normalize_null(val_a, config.null_values)
            norm_null_b = normalize_null(val_b, config.null_values)
            
            if norm_null_a is None and norm_null_b is None:
                match_after_null += 1
                continue
            
            # 3. String normalization (trim whitespace, case)
            norm_str_a = normalize_string(norm_null_a, config.strip_whitespace, config.case_sensitive)
            norm_str_b = normalize_string(norm_null_b, config.strip_whitespace, config.case_sensitive)
            
            if norm_str_a is not None and norm_str_b is not None:
                if str(norm_str_a) == str(norm_str_b):
                    match_after_trim += 1
                    continue
            
            # 4. Numeric comparison (12 == 12.0)
            if norm_null_a is not None and norm_null_b is not None:
                is_numeric_match, delta = try_numeric_compare(
                    norm_null_a, norm_null_b, 
                    config.numeric_atol, config.numeric_rtol
                )
                if is_numeric_match:
                    numeric_tolerance += 1
                    if delta is not None:
                        numeric_deltas.append(delta)
                    continue
            
            # 5. Mismatch
            mismatch += 1
            if len(sample_mismatches) < config.max_sample_diffs:
                sample_mismatches.append({
                    'key': key_val,
                    'value_a': str_a,
                    'value_b': str_b
                })
        
        # Compute numeric stats
        numeric_max_delta = max(numeric_deltas) if numeric_deltas else None
        numeric_mean_delta = float(np.mean(numeric_deltas)) if numeric_deltas else None
        
        col_result = ColumnValueComparison(
            column=col,
            exact_match=exact_match,
            match_after_trim=match_after_trim,
            match_after_null_norm=match_after_null,
            numeric_within_tolerance=numeric_tolerance,
            mismatch=mismatch,
            total_compared=total_rows,
            numeric_max_delta=numeric_max_delta,
            numeric_mean_delta=numeric_mean_delta,
            sample_mismatches=sample_mismatches
        )
        column_results.append(col_result)
        
        total_exact += exact_match
        total_norm += match_after_trim + match_after_null + numeric_tolerance
        total_mismatch += mismatch
    
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
    """Format bytes to human readable."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def format_number(n: int) -> str:
    """Format number with commas."""
    return f"{n:,}"


def generate_report(report: ComparisonReport) -> str:
    """Generate a markdown report."""
    lines = []
    
    # Header
    lines.append("# Data Comparison Report")
    lines.append("")
    lines.append(f"**Generated:** {report.timestamp}")
    lines.append(f"**Engine:** Pandas Row-by-Row (v1)")
    lines.append("")
    
    # Files
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
    
    # Schema
    lines.append("## 2. Schema Comparison")
    lines.append("")
    
    schema = report.schema
    
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
    
    # Primary Key Analysis
    lines.append("## 3. Primary Key Analysis")
    lines.append("")
    keys = report.keys
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
    
    # Value Comparison
    if report.values:
        lines.append("## 4. Value Comparison")
        lines.append("")
        values = report.values
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
    
    # Summary
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
    """Generate a concise summary for stdout."""
    lines = []
    lines.append("=" * 60)
    lines.append("DATA COMPARISON SUMMARY (Pandas Row-by-Row)")
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
    
    # Load files
    print(f"Loading {config.file_a}...")
    df_a = load_file(config.file_a, config.null_values)
    meta_a = get_file_metadata(config.file_a, df_a)
    
    print(f"Loading {config.file_b}...")
    df_b = load_file(config.file_b, config.null_values)
    meta_b = get_file_metadata(config.file_b, df_b)
    
    print(f"  File A: {format_number(meta_a.row_count)} rows × {meta_a.column_count} columns")
    print(f"  File B: {format_number(meta_b.row_count)} rows × {meta_b.column_count} columns")
    
    # Schema comparison
    print("Comparing schemas...")
    schema = compare_schema(df_a, df_b)
    
    # Key analysis
    print("Analyzing primary keys...")
    keys = analyze_keys(df_a, df_b, config.primary_key, config.max_sample_diffs)
    print(f"  Keys in both: {format_number(keys.keys_in_both)}")
    
    # Value comparison
    values = None
    if schema.common_columns and keys.keys_in_both > 0:
        print(f"Comparing values for {format_number(keys.keys_in_both)} matched rows (row-by-row)...")
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
        description="Compare two data files (CSV/Parquet) - Row-by-Row version.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python data_compare_v1_slow.py sales_sql.csv sales_spark.parquet --key customer_id
  python data_compare_v1_slow.py a.csv b.parquet --key id --key date --output report.md
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
        max_sample_diffs=args.max_samples
    )
    
    try:
        report = run_comparison(config)
        
        print()
        print(generate_stdout_summary(report))
        
        output_path = config.output_file or "comparison_report.md"
        with open(output_path, 'w') as f:
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
