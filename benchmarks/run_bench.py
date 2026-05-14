#!/usr/bin/env python3
"""
Full benchmark suite: baseline / mitigation / mitigation+combiner
across 5 corpus sizes × 2 distributions.

Writes: benchmarks/results.json, benchmarks/summary.md
"""

import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parent.parent
BIN = ROOT / "bin"
DATA = ROOT / "data"
BENCH_RUNS = ROOT / "benchmarks" / "runs"
RESULTS_JSON = ROOT / "benchmarks" / "results.json"
SUMMARY_MD = ROOT / "benchmarks" / "summary.md"

BENCH_RUNS.mkdir(parents=True, exist_ok=True)

# Corpus sizes (number of records)
SIZES = [100, 1_000, 10_000, 100_000, 1_000_000]

COMMON = dict(M=8, R=8, fixed_splits=8, seed=42)


# ---------------------------------------------------------------------------
# Corpus generation
# ---------------------------------------------------------------------------

def corpus_path(dist: str, n: int) -> Path:
    if dist == "zipf":
        return DATA / f"zipf{_suffix(n)}.jsonl"
    else:
        return DATA / f"catastrophe{_suffix(n)}.jsonl"


def _suffix(n: int) -> str:
    if n >= 1_000_000:
        return f"{n // 1_000_000}m"
    if n >= 1_000:
        return f"{n // 1_000}k"
    return str(n)


def generate_corpora():
    for n in SIZES:
        # Zipf
        p = corpus_path("zipf", n)
        if not p.exists():
            print(f"  Generating {p.name} ({n:,} records) …", flush=True)
            cmd = [
                str(BIN / "make_zipf"),
                f"-N-records={n}",
                f"-out={p}",
                "-vocab-size=5000",
                "-words-per-record=20",
                "-seed=42",
            ]
            subprocess.run(cmd, check=True)
        else:
            print(f"  {p.name} already exists, skipping.", flush=True)

        # Catastrophe
        p = corpus_path("catastrophe", n)
        if not p.exists():
            print(f"  Generating {p.name} ({n:,} records) …", flush=True)
            cmd = [
                str(BIN / "make_catastrophe"),
                f"-N-records={n}",
                f"-out={p}",
                "-hot-frac=0.7",
                "-vocab-size=5000",
                "-words-per-record=20",
                "-seed=42",
            ]
            subprocess.run(cmd, check=True)
        else:
            print(f"  {p.name} already exists, skipping.", flush=True)


# ---------------------------------------------------------------------------
# Single run
# ---------------------------------------------------------------------------

def run_once(dist: str, n: int, mode: str, combiner: bool) -> dict:
    """Execute one master run and return extracted metrics."""
    run_dir_base = BENCH_RUNS / f"{dist}_{_suffix(n)}"
    run_dir_base.mkdir(parents=True, exist_ok=True)

    input_path = corpus_path(dist, n)
    label = mode + ("+combiner" if combiner else "")
    print(f"    Running {dist}/{_suffix(n)}/{label} …", end=" ", flush=True)
    t0 = time.time()

    cmd = [
        str(BIN / "master"), "run",
        f"-input={input_path}",
        f"-run-dir={run_dir_base}",
        f"-M={COMMON['M']}",
        f"-R={COMMON['R']}",
        f"-mode={mode}",
        f"-fixed-splits={COMMON['fixed_splits']}",
        f"-seed={COMMON['seed']}",
        "-sample-rate=0.01",
        "-heavy-top-pct=0.01",
        "-max-parallel-maps=8",
    ]
    if combiner:
        cmd.append("-combiner")

    result = subprocess.run(
        cmd, capture_output=True, text=True, timeout=600
    )
    elapsed = time.time() - t0

    if result.returncode != 0:
        print(f"FAILED (rc={result.returncode})", flush=True)
        print("  STDERR:", result.stderr[-500:], flush=True)
        return {"error": result.stderr[-500:], "dist": dist, "n": n, "mode": mode, "combiner": combiner}

    # Find the most recent run directory created under run_dir_base
    run_dirs = sorted(run_dir_base.iterdir(), key=lambda p: p.stat().st_mtime)
    # Filter to only directories whose name starts with run_
    run_dirs = [d for d in run_dirs if d.is_dir() and d.name.startswith("run_")]
    if not run_dirs:
        print("ERROR: no run dir found", flush=True)
        return {"error": "no run dir", "dist": dist, "n": n, "mode": mode, "combiner": combiner}

    run_path = run_dirs[-1]
    summary_path = run_path / "metrics" / "run_summary.json"
    if not summary_path.exists():
        print("ERROR: no run_summary.json", flush=True)
        return {"error": "no run_summary.json", "dist": dist, "n": n, "mode": mode, "combiner": combiner}

    with open(summary_path) as f:
        summary = json.load(f)

    # Count total words from final.jsonl
    final_path = run_path / "output" / "final.jsonl"
    total_words = count_words(final_path)

    times = summary.get("times_ms", {})
    cov = summary.get("cov", {})

    print(f"done ({elapsed:.1f}s wall, {times.get('total','?')}ms reported)", flush=True)

    return {
        "dist": dist,
        "n": n,
        "mode": mode,
        "combiner": combiner,
        "T_total": times.get("total"),
        "T_map": times.get("map"),
        "T_sample": times.get("sample"),
        "T_plan": times.get("plan"),
        "T_reduce": times.get("reduce"),
        "T_merge": times.get("merge"),
        "CoV_records": cov.get("records"),
        "CoV_bytes": cov.get("bytes"),
        "max_median_records": summary.get("max_median_ratio", {}).get("records"),
        "max_median_bytes": summary.get("max_median_ratio", {}).get("bytes"),
        "reducer_records": summary.get("reducer_load", {}).get("records"),
        "reducer_bytes": summary.get("reducer_load", {}).get("bytes"),
        "total_words": total_words,
        "run_path": str(run_path),
    }


def count_words(final_path: Path) -> Optional[int]:
    """Sum the v field across all lines in final.jsonl."""
    if not final_path.exists():
        return None
    total = 0
    with open(final_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                v = rec.get("v", 0)
                if isinstance(v, (int, float)):
                    total += int(v)
            except json.JSONDecodeError:
                pass
    return total


# ---------------------------------------------------------------------------
# Run matrix
# ---------------------------------------------------------------------------

def run_all():
    results = []
    configs = [
        ("baseline", False),
        ("mitigation", False),
        ("mitigation", True),   # +combiner
    ]

    for dist in ["zipf", "catastrophe"]:
        for n in SIZES:
            print(f"\n[{dist} / {_suffix(n)}]", flush=True)
            for mode, combiner in configs:
                r = run_once(dist, n, mode, combiner)
                results.append(r)

    return results


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def write_results_json(results):
    with open(RESULTS_JSON, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nWrote {RESULTS_JSON}", flush=True)


def label(mode: str, combiner: bool) -> str:
    if mode == "baseline":
        return "Baseline"
    if not combiner:
        return "Mitigation"
    return "Mitig+Combiner"


def fmt(v, fmt_str="{:.0f}"):
    if v is None:
        return "—"
    try:
        return fmt_str.format(v)
    except Exception:
        return str(v)


def write_summary_md(results):
    lines = []
    lines.append("# Benchmark Results\n")
    lines.append("M=8, R=8, fixed-splits=8, vocab=5000, hot-frac=0.7 (catastrophe), seed=42\n")

    for dist in ["zipf", "catastrophe"]:
        lines.append(f"\n## Distribution: {dist}\n")
        lines.append(
            "| Size | Mode | T_total | T_map | T_sample | T_plan | T_reduce | T_merge "
            "| CoV_rec | CoV_bytes | Total Words | Flag |\n"
        )
        lines.append(
            "|------|------|--------:|------:|---------:|-------:|---------:|--------:"
            "|--------:|----------:|------------:|------|\n"
        )

        # Group by size, then check word count consistency
        for n in SIZES:
            rows = [r for r in results if r["dist"] == dist and r["n"] == n]
            word_counts = {r["total_words"] for r in rows if r.get("total_words") is not None and "error" not in r}
            consistent = len(word_counts) <= 1

            for r in rows:
                if "error" in r:
                    flag = "ERROR"
                    lines.append(
                        f"| {_suffix(n)} | {label(r['mode'], r['combiner'])} | ERROR | — | — | — | — | — | — | — | — | {flag} |\n"
                    )
                    continue

                mismatch = "" if consistent else "WORD_MISMATCH"
                lines.append(
                    f"| {_suffix(n)} "
                    f"| {label(r['mode'], r['combiner'])} "
                    f"| {fmt(r['T_total'])} "
                    f"| {fmt(r['T_map'])} "
                    f"| {fmt(r.get('T_sample'))} "
                    f"| {fmt(r.get('T_plan'))} "
                    f"| {fmt(r['T_reduce'])} "
                    f"| {fmt(r['T_merge'])} "
                    f"| {fmt(r['CoV_records'], '{:.4f}')} "
                    f"| {fmt(r['CoV_bytes'], '{:.4f}')} "
                    f"| {fmt(r['total_words'])} "
                    f"| {mismatch} |\n"
                )

    lines.append("\n## Notes\n")
    lines.append("- WORD_MISMATCH: total word count differs across modes for this corpus — indicates a correctness bug.\n")
    lines.append("- ERROR: run failed or timed out.\n")
    lines.append("- T_sample and T_plan are only present for mitigation modes.\n")

    with open(SUMMARY_MD, "w") as f:
        f.writelines(lines)
    print(f"Wrote {SUMMARY_MD}", flush=True)


def print_mismatches(results):
    any_mismatch = False
    for dist in ["zipf", "catastrophe"]:
        for n in SIZES:
            rows = [r for r in results if r["dist"] == dist and r["n"] == n and "error" not in r]
            counts = {r["total_words"] for r in rows if r.get("total_words") is not None}
            if len(counts) > 1:
                any_mismatch = True
                print(f"\n!!! WORD COUNT MISMATCH: {dist}/{_suffix(n)}")
                for r in rows:
                    print(f"    {label(r['mode'], r['combiner'])}: {r.get('total_words')}")
    if not any_mismatch:
        print("\nAll word counts consistent across modes. ✓")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=== Generating corpora ===", flush=True)
    generate_corpora()

    print("\n=== Running benchmark matrix ===", flush=True)
    print("30 runs total (5 sizes × 2 distributions × 3 modes)", flush=True)
    results = run_all()

    write_results_json(results)
    write_summary_md(results)
    print_mismatches(results)

    errors = [r for r in results if "error" in r]
    if errors:
        print(f"\n!!! {len(errors)} run(s) FAILED:")
        for r in errors:
            print(f"    {r['dist']}/{_suffix(r['n'])}/{label(r['mode'], r['combiner'])}: {r['error'][:200]}")
    else:
        print(f"\nAll {len(results)} runs completed successfully.")
