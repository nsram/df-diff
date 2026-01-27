#!/usr/bin/env python3
"""
Data Comparison Tool - Version 3: DuckDB Streaming

Compares two rectangular datasets (CSV, Parquet) and produces
a comprehensive report of similarities and differences.

This version uses DuckDB for out-of-core streaming comparison.
Best for: Large datasets (1M+ rows) that exceed available memory.

Usage:
    python data_compare_v3_duckdb.py file_a.csv file_b.parquet --key customer_id
"""

import argparse
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import duckdb


# =============================================================================
# Configuration
# =============================================================================

@dataclass
class ComparisonConfig:
    """Configuration for data comparison."""
    file_a: str
    file_b: str
    primary_key: list[str]
    
    null_values: list[str] = field(default_factory=lambda: [
        "", "NA", "N/A", "NULL", "None", "NaN", "nan", "<NA>", "null", "NONE"
    ])
    
    numeric_atol: float = 1e-9
    numeric_rtol: float = 1e-9
    strip_whitespace: bool = True
    case_sensitive: bool = True
    output_file: str | None = None
    max_sample_diffs: int = 10
    column_batch_size: int = 50  # Batch columns to reduce I/O passes
    allow_duplicate_keys: bool = False


# =============================================================================
# Data Structures
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
# SQL Helpers
# =============================================================================

def escape_identifier(name: str) -> str:
    """Escape a SQL identifier."""
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def escape_string_literal(value: str) -> str:
    """Escape a string literal for SQL."""
    return value.replace("'", "''")


def make_col_alias(col: str) -> str:
    """Make a safe column alias for SQL."""
    return col.replace("'", "").replace('"', "").replace(" ", "_")


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


# =============================================================================
# File Handling
# =============================================================================

def detect_format(filepath: str) -> str:
    ext = Path(filepath).suffix.lower()
    format_map = {'.csv': 'csv', '.parquet': 'parquet', '.pq': 'parquet'}
    fmt = format_map.get(ext)
    if fmt is None:
        raise ValueError(f"Unsupported file format: {ext}. Only CSV and Parquet are supported.")
    return fmt


def get_duckdb_read_expr(filepath: str) -> str:
    fmt = detect_format(filepath)
    escaped_path = escape_string_literal(filepath)
    if fmt == 'csv':
        return f"read_csv('{escaped_path}', all_varchar=true, auto_detect=true, header=true)"
    elif fmt == 'parquet':
        return f"read_parquet('{escaped_path}')"
    else:
        raise ValueError(f"Unsupported format: {fmt}")


def get_file_metadata(filepath: str, con: duckdb.DuckDBPyConnection) -> FileMetadata:
    path = Path(filepath)
    read_expr = get_duckdb_read_expr(filepath)
    row_count = con.execute(f"SELECT COUNT(*) FROM {read_expr}").fetchone()[0]
    
    # Use SELECT * LIMIT 0 to get schema, as DESCRIBE doesn't work with read_csv()
    con.execute(f"CREATE OR REPLACE TEMP VIEW __schema_check__ AS SELECT * FROM {read_expr} LIMIT 0")
    schema_info = con.execute("DESCRIBE __schema_check__").fetchall()
    con.execute("DROP VIEW IF EXISTS __schema_check__")
    
    columns = [row[0] for row in schema_info]
    if len(columns) == 0:
        raise ValueError(f"File has no columns (headers required): {filepath}")
    
    # Validate column names
    validate_column_names(columns, filepath)
    
    return FileMetadata(
        path=str(path), format=detect_format(filepath),
        size_bytes=path.stat().st_size, row_count=row_count,
        column_count=len(columns), columns=columns
    )


# =============================================================================
# Schema Comparison
# =============================================================================

def compare_schema(meta_a: FileMetadata, meta_b: FileMetadata, con: duckdb.DuckDBPyConnection) -> SchemaComparison:
    cols_a, cols_b = set(meta_a.columns), set(meta_b.columns)
    common = cols_a & cols_b
    order_a = [c for c in meta_a.columns if c in common]
    order_b = [c for c in meta_b.columns if c in common]
    
    read_a, read_b = get_duckdb_read_expr(meta_a.path), get_duckdb_read_expr(meta_b.path)
    
    # Get types via temp views since DESCRIBE doesn't work with read_csv()
    con.execute(f"CREATE OR REPLACE TEMP VIEW __types_a__ AS SELECT * FROM {read_a} LIMIT 0")
    con.execute(f"CREATE OR REPLACE TEMP VIEW __types_b__ AS SELECT * FROM {read_b} LIMIT 0")
    types_a = {row[0]: row[1] for row in con.execute("DESCRIBE __types_a__").fetchall()}
    types_b = {row[0]: row[1] for row in con.execute("DESCRIBE __types_b__").fetchall()}
    con.execute("DROP VIEW IF EXISTS __types_a__")
    con.execute("DROP VIEW IF EXISTS __types_b__")
    
    type_mismatches = [(col, types_a[col], types_b[col]) for col in common if types_a[col] != types_b[col]]
    
    return SchemaComparison(
        column_count_match=(len(meta_a.columns) == len(meta_b.columns)),
        column_names_match=(cols_a == cols_b),
        column_order_match=(order_a == order_b),
        columns_only_in_a=sorted(cols_a - cols_b),
        columns_only_in_b=sorted(cols_b - cols_a),
        common_columns=sorted(common),
        column_order_a=meta_a.columns,
        column_order_b=meta_b.columns,
        type_mismatches=type_mismatches
    )


# =============================================================================
# Key Analysis
# =============================================================================

def analyze_keys(meta_a: FileMetadata, meta_b: FileMetadata, key_columns: list[str],
                 max_samples: int, con: duckdb.DuckDBPyConnection) -> KeyAnalysis:
    for col in key_columns:
        if col not in meta_a.columns:
            raise ValueError(f"Key column '{col}' not found in file A")
        if col not in meta_b.columns:
            raise ValueError(f"Key column '{col}' not found in file B")
    
    read_a, read_b = get_duckdb_read_expr(meta_a.path), get_duckdb_read_expr(meta_b.path)
    key_cols_sql = ", ".join(escape_identifier(k) for k in key_columns)
    key_cols_cast = ", ".join(f'CAST({escape_identifier(k)} AS VARCHAR)' for k in key_columns)
    
    dup_a = con.execute(f"SELECT COUNT(*) - COUNT(DISTINCT ({key_cols_cast})) FROM {read_a}").fetchone()[0]
    dup_b = con.execute(f"SELECT COUNT(*) - COUNT(DISTINCT ({key_cols_cast})) FROM {read_b}").fetchone()[0]
    
    con.execute(f"CREATE OR REPLACE TEMP VIEW keys_a AS SELECT DISTINCT {key_cols_sql} FROM {read_a}")
    con.execute(f"CREATE OR REPLACE TEMP VIEW keys_b AS SELECT DISTINCT {key_cols_sql} FROM {read_b}")
    
    join_cond = ' AND '.join(f'a.{escape_identifier(k)} = b.{escape_identifier(k)}' for k in key_columns)
    
    keys_in_both = con.execute(f"SELECT COUNT(*) FROM keys_a a INNER JOIN keys_b b ON {join_cond}").fetchone()[0]
    keys_only_a = con.execute(f"SELECT COUNT(*) FROM keys_a a WHERE NOT EXISTS (SELECT 1 FROM keys_b b WHERE {join_cond})").fetchone()[0]
    keys_only_b = con.execute(f"SELECT COUNT(*) FROM keys_b b WHERE NOT EXISTS (SELECT 1 FROM keys_a a WHERE {join_cond})").fetchone()[0]
    
    sample_a = con.execute(f"SELECT {key_cols_sql} FROM keys_a a WHERE NOT EXISTS (SELECT 1 FROM keys_b b WHERE {join_cond}) LIMIT {max_samples}").fetchall()
    sample_b = con.execute(f"SELECT {key_cols_sql} FROM keys_b b WHERE NOT EXISTS (SELECT 1 FROM keys_a a WHERE {join_cond}) LIMIT {max_samples}").fetchall()
    
    return KeyAnalysis(
        key_columns=key_columns, unique_in_a=(dup_a == 0), unique_in_b=(dup_b == 0),
        duplicate_count_a=int(dup_a), duplicate_count_b=int(dup_b),
        keys_in_both=int(keys_in_both), keys_only_in_a=int(keys_only_a), keys_only_in_b=int(keys_only_b),
        sample_keys_only_in_a=[tuple(row) for row in sample_a],
        sample_keys_only_in_b=[tuple(row) for row in sample_b]
    )


# =============================================================================
# Value Comparison - BATCHED
# =============================================================================

def compare_values_duckdb(meta_a: FileMetadata, meta_b: FileMetadata, key_columns: list[str],
                          columns_to_compare: list[str], config: ComparisonConfig,
                          con: duckdb.DuckDBPyConnection) -> ValueComparison:
    """Compare values using DuckDB with batched column aggregation."""
    
    read_a, read_b = get_duckdb_read_expr(meta_a.path), get_duckdb_read_expr(meta_b.path)
    join_cond = ' AND '.join(f'a.{escape_identifier(k)} = b.{escape_identifier(k)}' for k in key_columns)
    key_cols_sql = ", ".join(f'a.{escape_identifier(k)} AS {escape_identifier(k)}' for k in key_columns)
    
    col_select = ", ".join(
        f'a.{escape_identifier(c)} AS {escape_identifier(c + "_A")}, b.{escape_identifier(c)} AS {escape_identifier(c + "_B")}'
        for c in columns_to_compare
    )
    
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW matched AS
        SELECT {key_cols_sql}, {col_select}
        FROM {read_a} a INNER JOIN {read_b} b ON {join_cond}
    """)
    
    total_rows = con.execute("SELECT COUNT(*) FROM matched").fetchone()[0]
    value_columns = [c for c in columns_to_compare if c not in key_columns]
    num_cols = len(value_columns)
    
    null_values_sql = ", ".join(f"'{escape_string_literal(v)}'" for v in config.null_values)
    
    if len(key_columns) == 1:
        key_expr = f'CAST({escape_identifier(key_columns[0])} AS VARCHAR)'
    else:
        key_expr = "CONCAT_WS('|||', " + ", ".join(f'CAST({escape_identifier(k)} AS VARCHAR)' for k in key_columns) + ")"
    
    column_results = []
    total_exact, total_norm, total_mismatch = 0, 0, 0
    
    # Process in batches
    batch_size = config.column_batch_size
    for batch_start in range(0, num_cols, batch_size):
        batch_end = min(batch_start + batch_size, num_cols)
        batch_cols = value_columns[batch_start:batch_end]
        
        if num_cols > batch_size:
            print(f"  Processing columns {batch_start + 1}-{batch_end} of {num_cols}...")
        
        # Build single aggregation query for batch
        agg_parts = []
        for col in batch_cols:
            col_a = escape_identifier(f"{col}_A")
            col_b = escape_identifier(f"{col}_B")
            alias = make_col_alias(col)
            
            str_a, str_b = f'CAST({col_a} AS VARCHAR)', f'CAST({col_b} AS VARCHAR)'
            exact_eq = f'{str_a} IS NOT DISTINCT FROM {str_b}'
            not_exact = f'{str_a} IS DISTINCT FROM {str_b}'

            if config.strip_whitespace:
                trim_a, trim_b = f'TRIM({str_a})', f'TRIM({str_b})'
            else:
                trim_a, trim_b = str_a, str_b
            
            if not config.case_sensitive:
                trim_a, trim_b = f'LOWER({trim_a})', f'LOWER({trim_b})'
            
            null_a = f"({col_a} IS NULL OR TRIM(CAST({col_a} AS VARCHAR)) IN ({null_values_sql}))"
            null_b = f"({col_b} IS NULL OR TRIM(CAST({col_b} AS VARCHAR)) IN ({null_values_sql}))"
            
            num_a, num_b = f'TRY_CAST({trim_a} AS DOUBLE)', f'TRY_CAST({trim_b} AS DOUBLE)'
            tol_expr = f"({num_a} IS NOT NULL AND {num_b} IS NOT NULL AND ABS({num_a} - {num_b}) <= {config.numeric_atol} + {config.numeric_rtol} * ABS({num_b}))"
            
            agg_parts.append(f"""
                SUM(CASE WHEN {exact_eq} THEN 1 ELSE 0 END) AS exact_{alias},
                SUM(CASE WHEN {not_exact} AND {null_a} AND {null_b} THEN 1 ELSE 0 END) AS null_{alias},
                SUM(CASE WHEN {not_exact} AND NOT ({null_a} AND {null_b}) AND NOT {null_a} AND NOT {null_b} AND {trim_a} = {trim_b} THEN 1 ELSE 0 END) AS trim_{alias},
                SUM(CASE WHEN {not_exact} AND NOT ({null_a} AND {null_b}) AND ({null_a} OR {null_b} OR {trim_a} != {trim_b}) AND {tol_expr} THEN 1 ELSE 0 END) AS numeric_{alias},
                MAX(CASE WHEN {not_exact} AND NOT ({null_a} AND {null_b}) AND ({null_a} OR {null_b} OR {trim_a} != {trim_b}) AND {tol_expr} THEN ABS({num_a} - {num_b}) END) AS maxdelta_{alias},
                AVG(CASE WHEN {not_exact} AND NOT ({null_a} AND {null_b}) AND ({null_a} OR {null_b} OR {trim_a} != {trim_b}) AND {tol_expr} THEN ABS({num_a} - {num_b}) END) AS meandelta_{alias}
            """)
        
        agg_sql = ", ".join(agg_parts)
        stats = con.execute(f"SELECT {agg_sql} FROM matched").fetchone()
        
        # Parse results
        for i, col in enumerate(batch_cols):
            alias = make_col_alias(col)
            base = i * 6
            
            exact = stats[base] or 0
            null_match = stats[base + 1] or 0
            trim_match = stats[base + 2] or 0
            numeric_match = stats[base + 3] or 0
            max_delta = stats[base + 4]
            mean_delta = stats[base + 5]
            
            mismatch = total_rows - exact - null_match - trim_match - numeric_match
            
            # Get sample mismatches
            sample_mismatches = []
            if mismatch > 0:
                col_a = escape_identifier(f"{col}_A")
                col_b = escape_identifier(f"{col}_B")
                str_a, str_b = f'CAST({col_a} AS VARCHAR)', f'CAST({col_b} AS VARCHAR)'
                not_exact = f'{str_a} IS DISTINCT FROM {str_b}'

                if config.strip_whitespace:
                    trim_a, trim_b = f'TRIM({str_a})', f'TRIM({str_b})'
                else:
                    trim_a, trim_b = str_a, str_b
                if not config.case_sensitive:
                    trim_a, trim_b = f'LOWER({trim_a})', f'LOWER({trim_b})'

                null_a = f"({col_a} IS NULL OR TRIM(CAST({col_a} AS VARCHAR)) IN ({null_values_sql}))"
                null_b = f"({col_b} IS NULL OR TRIM(CAST({col_b} AS VARCHAR)) IN ({null_values_sql}))"
                num_a, num_b = f'TRY_CAST({trim_a} AS DOUBLE)', f'TRY_CAST({trim_b} AS DOUBLE)'
                tol_expr = f"({num_a} IS NOT NULL AND {num_b} IS NOT NULL AND ABS({num_a} - {num_b}) <= {config.numeric_atol} + {config.numeric_rtol} * ABS({num_b}))"

                mismatch_cond = f"{not_exact} AND NOT ({null_a} AND {null_b}) AND ({null_a} OR {null_b} OR {trim_a} != {trim_b}) AND NOT {tol_expr}"
                
                samples = con.execute(f"""
                    SELECT {key_expr} AS key_val, {str_a} AS val_a, {str_b} AS val_b
                    FROM matched WHERE {mismatch_cond} LIMIT {config.max_sample_diffs}
                """).fetchall()
                
                sample_mismatches = [{'key': r[0], 'value_a': r[1], 'value_b': r[2]} for r in samples]
            
            column_results.append(ColumnValueComparison(
                column=col, exact_match=int(exact), match_after_trim=int(trim_match),
                match_after_null_norm=int(null_match), numeric_within_tolerance=int(numeric_match),
                mismatch=int(mismatch), total_compared=total_rows,
                numeric_max_delta=max_delta, numeric_mean_delta=mean_delta,
                sample_mismatches=sample_mismatches
            ))
            
            total_exact += exact
            total_norm += null_match + trim_match + numeric_match
            total_mismatch += mismatch
    
    return ValueComparison(
        total_rows_compared=total_rows,
        total_cells_compared=total_rows * len(value_columns),
        columns=column_results,
        total_exact_match=int(total_exact),
        total_match_after_norm=int(total_norm),
        total_mismatch=int(total_mismatch)
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
    lines = []
    lines.append("# Data Comparison Report")
    lines.append("")
    lines.append(f"**Generated:** {report.timestamp}")
    lines.append(f"**Engine:** DuckDB Streaming (v3)")
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
            lines.append("⚠ Column order differs")
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
    lines.append(f"**Key column(s):** {', '.join(f'`{k}`' for k in keys.key_columns)}")
    lines.append("")
    lines.append("### Uniqueness")
    lines.append(f"{'✓' if keys.unique_in_a else '⚠'} Keys {'are unique' if keys.unique_in_a else f'have {format_number(keys.duplicate_count_a)} duplicates'} in File A")
    lines.append(f"{'✓' if keys.unique_in_b else '⚠'} Keys {'are unique' if keys.unique_in_b else f'have {format_number(keys.duplicate_count_b)} duplicates'} in File B")
    lines.append("")
    lines.append("### Key Coverage")
    lines.append("")
    lines.append(f"- Keys in both: **{format_number(keys.keys_in_both)}**")
    lines.append(f"- Only in A: **{format_number(keys.keys_only_in_a)}**")
    lines.append(f"- Only in B: **{format_number(keys.keys_only_in_b)}**")
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
        lines.append(f"Comparing **{format_number(values.total_rows_compared)}** rows × **{len(values.columns)}** columns")
        lines.append(f"(**{format_number(values.total_cells_compared)}** cells)")
        lines.append("")
        
        lines.append("### Summary by Column")
        lines.append("")
        lines.append("| Column | Exact | Trimmed | Null Eq. | Numeric | Mismatch |")
        lines.append("|--------|-------|---------|----------|---------|----------|")
        for col in values.columns:
            lines.append(f"| {col.column} | {format_number(col.exact_match)} | {format_number(col.match_after_trim)} | {format_number(col.match_after_null_norm)} | {format_number(col.numeric_within_tolerance)} | {format_number(col.mismatch)} |")
        lines.append("")
        
        lines.append("### Aggregate Totals")
        lines.append("")
        lines.append(f"- Exact matches: **{format_number(values.total_exact_match)}**")
        lines.append(f"- After normalization: **{format_number(values.total_match_after_norm)}**")
        lines.append(f"- Mismatches: **{format_number(values.total_mismatch)}**")
        lines.append("")
        
        cols_with_norm = [c for c in values.columns if c.match_after_trim > 0 or c.match_after_null_norm > 0 or c.numeric_within_tolerance > 0]
        if cols_with_norm:
            lines.append("### Normalization Details")
            lines.append("")
            for col in cols_with_norm:
                lines.append(f"**{col.column}:**")
                if col.match_after_trim > 0:
                    lines.append(f"  - Whitespace/case resolved: {format_number(col.match_after_trim)}")
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
                    for s in col.sample_mismatches[:10]:
                        lines.append(f"| `{s['key']}` | `{str(s['value_a'])[:50]}` | `{str(s['value_b'])[:50]}` |")
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
    lines = ["=" * 60, "DATA COMPARISON SUMMARY (DuckDB Streaming)", "=" * 60, ""]
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
        lines.append(f"⚠ {format_number(report.values.total_mismatch)} cell mismatches")
        cols = [c for c in report.values.columns if c.mismatch > 0][:5]
        if cols:
            lines.append("  Columns with mismatches:")
            for c in cols:
                lines.append(f"    - {c.column}: {format_number(c.mismatch)}")
    
    return "\n".join(lines)


# =============================================================================
# Main
# =============================================================================

def run_comparison(config: ComparisonConfig) -> ComparisonReport:
    con = duckdb.connect(":memory:")
    con.execute("SET threads TO 4")
    con.execute("SET memory_limit = '12GB'")
    
    print(f"Loading metadata for {config.file_a}...")
    meta_a = get_file_metadata(config.file_a, con)
    print(f"Loading metadata for {config.file_b}...")
    meta_b = get_file_metadata(config.file_b, con)
    print(f"  File A: {format_number(meta_a.row_count)} rows × {meta_a.column_count} columns")
    print(f"  File B: {format_number(meta_b.row_count)} rows × {meta_b.column_count} columns")
    
    print("Comparing schemas...")
    schema = compare_schema(meta_a, meta_b, con)
    
    print("Analyzing primary keys...")
    keys = analyze_keys(meta_a, meta_b, config.primary_key, config.max_sample_diffs, con)
    
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
        print(f"Comparing values for {format_number(keys.keys_in_both)} matched rows...")
        values = compare_values_duckdb(meta_a, meta_b, config.primary_key, schema.common_columns, config, con)
        print(f"  Exact: {format_number(values.total_exact_match)}")
        print(f"  Normalized: {format_number(values.total_match_after_norm)}")
        print(f"  Mismatches: {format_number(values.total_mismatch)}")
    
    con.close()
    
    # Verdicts
    if schema.column_names_match and schema.column_order_match:
        schema_verdict = "✓ MATCH"
    elif schema.column_names_match:
        schema_verdict = "⚠ MATCH (reordered)"
    else:
        schema_verdict = "✗ MISMATCH"
    
    if keys.keys_only_in_a == 0 and keys.keys_only_in_b == 0:
        coverage_verdict = "✓ MATCH"
    else:
        coverage_verdict = f"⚠ {format_number(keys.keys_only_in_a + keys.keys_only_in_b)} keys differ"
    
    if values is None:
        value_verdict = "N/A"
    elif values.total_mismatch == 0:
        value_verdict = "✓ EXACT MATCH" if values.total_match_after_norm == 0 else "✓ MATCH (normalized)"
    else:
        pct = (values.total_mismatch / values.total_cells_compared) * 100
        value_verdict = f"✗ {format_number(values.total_mismatch)} mismatches ({pct:.4f}%)"
    
    if (schema.column_names_match and keys.keys_only_in_a == 0 and keys.keys_only_in_b == 0 and
        values and values.total_mismatch == 0):
        overall_verdict = "IDENTICAL" if values.total_match_after_norm == 0 and schema.column_order_match else "EQUIVALENT"
    elif values and values.total_mismatch == 0:
        overall_verdict = "EQUIVALENT (row/schema differ)"
    else:
        overall_verdict = "DIFFERENT"
    
    return ComparisonReport(
        timestamp=datetime.now().isoformat(), config=config,
        file_a=meta_a, file_b=meta_b, schema=schema, keys=keys, values=values,
        schema_verdict=schema_verdict, coverage_verdict=coverage_verdict,
        value_verdict=value_verdict, overall_verdict=overall_verdict
    )


def main():
    parser = argparse.ArgumentParser(
        description="Compare two data files (CSV/Parquet) - DuckDB Streaming version.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python data_compare_v3_duckdb.py sales_sql.csv sales_spark.parquet --key customer_id
  python data_compare_v3_duckdb.py a.parquet b.parquet --key id --key date -o report.md
        """
    )
    
    parser.add_argument("file_a", help="Path to first file (CSV or Parquet)")
    parser.add_argument("file_b", help="Path to second file (CSV or Parquet)")
    parser.add_argument("--key", "-k", action="append", required=True, dest="keys",
                        help="Primary key column(s)")
    parser.add_argument("--output", "-o", help="Output markdown report file")
    parser.add_argument("--numeric-tolerance", type=float, default=1e-9)
    parser.add_argument("--no-strip", action="store_true", help="Disable whitespace stripping")
    parser.add_argument("--ignore-case", action="store_true", help="Case-insensitive comparison")
    parser.add_argument("--max-samples", type=int, default=10)
    parser.add_argument("--batch-size", type=int, default=50, help="Columns per batch (default: 50)")
    parser.add_argument("--allow-duplicate-keys", action="store_true",
                        help="Continue even if primary keys are not unique")
    
    args = parser.parse_args()
    
    config = ComparisonConfig(
        file_a=args.file_a, file_b=args.file_b, primary_key=args.keys,
        numeric_atol=args.numeric_tolerance, numeric_rtol=args.numeric_tolerance,
        strip_whitespace=not args.no_strip, case_sensitive=not args.ignore_case,
        output_file=args.output, max_sample_diffs=args.max_samples,
        column_batch_size=args.batch_size,
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
        
        sys.exit(0 if report.overall_verdict in ["IDENTICAL", "EQUIVALENT"] else 1)
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(2)


if __name__ == "__main__":
    main()
