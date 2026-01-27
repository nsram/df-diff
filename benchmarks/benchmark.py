#!/usr/bin/env python3
"""
Benchmark script for Data Compare tool.

Generates synthetic datasets and compares performance across versions.

Usage:
    python benchmark.py
    python benchmark.py --rows 100000 --cols 50
    python benchmark.py --skip-v1  # Skip slow version
"""

import argparse
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import numpy as np
import pandas as pd


def generate_test_data(rows: int, cols: int, seed: int = 42) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Generate two DataFrames with controlled differences."""
    np.random.seed(seed)
    
    # Generate base data
    data = {
        'id': [f'ID-{i:08d}' for i in range(rows)]
    }
    
    # Add columns with various types
    for i in range(cols):
        col_type = i % 4
        col_name = f'col_{i:03d}'
        
        if col_type == 0:  # String
            data[col_name] = [f'value_{np.random.randint(0, 1000)}' for _ in range(rows)]
        elif col_type == 1:  # Integer
            data[col_name] = np.random.randint(0, 10000, rows).astype(str)
        elif col_type == 2:  # Float
            data[col_name] = np.random.uniform(0, 1000, rows).round(4).astype(str)
        else:  # Nullable
            vals = np.random.choice(['A', 'B', 'C', 'NA', ''], rows)
            data[col_name] = vals
    
    df_a = pd.DataFrame(data)
    df_b = df_a.copy()
    
    # Introduce controlled differences in df_b
    
    # 1. Whitespace differences (~1% of string columns)
    for i in range(0, cols, 4):
        col_name = f'col_{i:03d}'
        mask = np.random.random(rows) < 0.01
        df_b.loc[mask, col_name] = '  ' + df_b.loc[mask, col_name].astype(str) + '  '
    
    # 2. Tiny numeric differences (~1% of float columns)
    for i in range(2, cols, 4):
        col_name = f'col_{i:03d}'
        mask = np.random.random(rows) < 0.01
        df_b.loc[mask, col_name] = (
            pd.to_numeric(df_b.loc[mask, col_name], errors='coerce') + 1e-10
        ).astype(str)
    
    # 3. Remove a few rows from B, add a few different rows
    drop_indices = np.random.choice(rows, size=max(1, rows // 1000), replace=False)
    df_b = df_b.drop(drop_indices).reset_index(drop=True)
    
    # Add a few new rows
    new_rows = pd.DataFrame({
        'id': [f'ID-NEW-{i:04d}' for i in range(max(1, rows // 1000))],
        **{f'col_{i:03d}': ['new_value'] * max(1, rows // 1000) for i in range(cols)}
    })
    df_b = pd.concat([df_b, new_rows], ignore_index=True)
    
    return df_a, df_b


def save_test_files(df_a: pd.DataFrame, df_b: pd.DataFrame, tmpdir: Path) -> tuple[Path, Path]:
    """Save DataFrames to CSV files."""
    path_a = tmpdir / 'test_a.csv'
    path_b = tmpdir / 'test_b.csv'
    
    df_a.to_csv(path_a, index=False)
    df_b.to_csv(path_b, index=False)
    
    return path_a, path_b


def run_benchmark(script: str, file_a: Path, file_b: Path, output: Path) -> tuple[float, bool]:
    """Run a comparison script and measure time."""
    start = time.perf_counter()
    
    try:
        result = subprocess.run(
            [sys.executable, script, str(file_a), str(file_b), '--key', 'id', '--output', str(output)],
            capture_output=True,
            text=True,
            timeout=3600  # 1 hour timeout
        )
        elapsed = time.perf_counter() - start
        success = result.returncode in [0, 1]  # 0=identical/equivalent, 1=different
        
        if not success:
            print(f"  Error: {result.stderr[:200]}")
        
        return elapsed, success
        
    except subprocess.TimeoutExpired:
        return float('inf'), False
    except Exception as e:
        print(f"  Exception: {e}")
        return float('inf'), False


def format_time(seconds: float) -> str:
    """Format seconds to human readable."""
    if seconds == float('inf'):
        return 'TIMEOUT'
    elif seconds < 1:
        return f'{seconds*1000:.0f}ms'
    elif seconds < 60:
        return f'{seconds:.1f}s'
    else:
        return f'{seconds/60:.1f}min'


def format_size(rows: int, cols: int) -> str:
    """Format dataset size."""
    cells = rows * cols
    if cells < 1_000_000:
        return f'{cells/1000:.0f}K cells'
    else:
        return f'{cells/1_000_000:.1f}M cells'


def main():
    parser = argparse.ArgumentParser(description='Benchmark Data Compare versions')
    parser.add_argument('--rows', type=int, default=10000, help='Number of rows (default: 10000)')
    parser.add_argument('--cols', type=int, default=50, help='Number of columns (default: 50)')
    parser.add_argument('--skip-v1', action='store_true', help='Skip V1 (slow) benchmark')
    parser.add_argument('--skip-v3', action='store_true', help='Skip V3 (DuckDB) benchmark')
    args = parser.parse_args()
    
    # Find script locations
    script_dir = Path(__file__).parent.parent / 'src'
    v1_script = script_dir / 'df_diff_v1_slow.py'
    v2_script = script_dir / 'df_diff_v2_vectorized.py'
    v3_script = script_dir / 'df_diff_v3_duckdb.py'
    
    # Check scripts exist
    for script in [v2_script] + ([] if args.skip_v1 else [v1_script]) + ([] if args.skip_v3 else [v3_script]):
        if not script.exists():
            print(f"Error: Script not found: {script}")
            sys.exit(1)
    
    print(f"{'='*60}")
    print(f"DATA COMPARE BENCHMARK")
    print(f"{'='*60}")
    print(f"Dataset: {args.rows:,} rows × {args.cols} columns ({format_size(args.rows, args.cols)})")
    print()
    
    # Generate test data
    print("Generating test data...")
    start = time.perf_counter()
    df_a, df_b = generate_test_data(args.rows, args.cols)
    gen_time = time.perf_counter() - start
    print(f"  Generated in {format_time(gen_time)}")
    print(f"  File A: {len(df_a):,} rows")
    print(f"  File B: {len(df_b):,} rows")
    print()
    
    # Save to temp files
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        
        print("Saving test files...")
        start = time.perf_counter()
        path_a, path_b = save_test_files(df_a, df_b, tmpdir)
        save_time = time.perf_counter() - start
        print(f"  Saved in {format_time(save_time)}")
        print(f"  File A: {path_a.stat().st_size / 1024 / 1024:.1f} MB")
        print(f"  File B: {path_b.stat().st_size / 1024 / 1024:.1f} MB")
        print()
        
        results = {}
        
        # Run V1 benchmark
        if not args.skip_v1:
            if args.rows > 50000:
                print("V1 (Row-by-Row): SKIPPED (too slow for >50K rows)")
                results['V1'] = (float('inf'), False)
            else:
                print("V1 (Row-by-Row): Running...")
                output = tmpdir / 'report_v1.md'
                elapsed, success = run_benchmark(str(v1_script), path_a, path_b, output)
                results['V1'] = (elapsed, success)
                print(f"  {'✓' if success else '✗'} Completed in {format_time(elapsed)}")
        
        # Run V2 benchmark
        print("V2 (Vectorized): Running...")
        output = tmpdir / 'report_v2.md'
        elapsed, success = run_benchmark(str(v2_script), path_a, path_b, output)
        results['V2'] = (elapsed, success)
        print(f"  {'✓' if success else '✗'} Completed in {format_time(elapsed)}")
        
        # Run V3 benchmark
        if not args.skip_v3:
            print("V3 (DuckDB): Running...")
            output = tmpdir / 'report_v3.md'
            elapsed, success = run_benchmark(str(v3_script), path_a, path_b, output)
            results['V3'] = (elapsed, success)
            print(f"  {'✓' if success else '✗'} Completed in {format_time(elapsed)}")
        
        # Summary
        print()
        print(f"{'='*60}")
        print("RESULTS")
        print(f"{'='*60}")
        print()
        print(f"{'Version':<20} {'Time':<15} {'Status':<10}")
        print(f"{'-'*20} {'-'*15} {'-'*10}")
        
        for version, (elapsed, success) in results.items():
            status = '✓ OK' if success else '✗ FAILED'
            print(f"{version:<20} {format_time(elapsed):<15} {status:<10}")
        
        # Winner
        print()
        valid_results = {k: v[0] for k, v in results.items() if v[1] and v[0] != float('inf')}
        if valid_results:
            winner = min(valid_results, key=valid_results.get)
            print(f"Fastest: {winner} ({format_time(valid_results[winner])})")
        
        print()


if __name__ == '__main__':
    main()
