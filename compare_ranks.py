#!/usr/bin/env python3
import json
import sys
import os

baseline_dir = sys.argv[1] if len(sys.argv) > 1 else None
mitigation_dir = sys.argv[2] if len(sys.argv) > 2 else None

if not baseline_dir or not mitigation_dir:
    # Find latest runs
    runs_dir = "runs"
    baseline_runs = sorted([d for d in os.listdir(runs_dir) if d.startswith("run_pagerank_baseline_")], reverse=True)
    mitigation_runs = sorted([d for d in os.listdir(runs_dir) if d.startswith("run_pagerank_mitigation_")], reverse=True)
    
    if baseline_runs:
        baseline_dir = os.path.join(runs_dir, baseline_runs[0])
    if mitigation_runs:
        mitigation_dir = os.path.join(runs_dir, mitigation_runs[0])

if not baseline_dir or not mitigation_dir:
    print("Error: Could not find baseline and/or mitigation runs")
    sys.exit(1)

baseline_file = os.path.join(baseline_dir, "final", "ranks_sorted.jsonl")
mitigation_file = os.path.join(mitigation_dir, "final", "ranks_sorted.jsonl")

print(f"Baseline: {baseline_dir}")
print(f"Mitigation: {mitigation_dir}")
print()

# Read baseline ranks
baseline_ranks = {}
if os.path.exists(baseline_file):
    with open(baseline_file) as f:
        for line in f:
            if line.strip():
                record = json.loads(line)
                baseline_ranks[record['node']] = record['rank']
else:
    # Try unsorted file
    baseline_file = os.path.join(baseline_dir, "final", "ranks.jsonl")
    if os.path.exists(baseline_file):
        with open(baseline_file) as f:
            for line in f:
                if line.strip():
                    record = json.loads(line)
                    baseline_ranks[record['node']] = record['rank']

# Read mitigation ranks
mitigation_ranks = {}
if os.path.exists(mitigation_file):
    with open(mitigation_file) as f:
        for line in f:
            if line.strip():
                record = json.loads(line)
                mitigation_ranks[record['node']] = record['rank']
else:
    # Try unsorted file
    mitigation_file = os.path.join(mitigation_dir, "final", "ranks.jsonl")
    if os.path.exists(mitigation_file):
        with open(mitigation_file) as f:
            for line in f:
                if line.strip():
                    record = json.loads(line)
                    mitigation_ranks[record['node']] = record['rank']

# Compare
print("=== Detailed Comparison ===")
print(f"Baseline nodes: {len(baseline_ranks)}")
print(f"Mitigation nodes: {len(mitigation_ranks)}")
print()

all_nodes = set(baseline_ranks.keys()) | set(mitigation_ranks.keys())
if len(all_nodes) != len(baseline_ranks) or len(all_nodes) != len(mitigation_ranks):
    print("WARNING: Node count mismatch!")
    baseline_only = set(baseline_ranks.keys()) - set(mitigation_ranks.keys())
    mitigation_only = set(mitigation_ranks.keys()) - set(baseline_ranks.keys())
    if baseline_only:
        print(f"  Baseline only: {baseline_only}")
    if mitigation_only:
        print(f"  Mitigation only: {mitigation_only}")
    print()

max_diff = 0.0
max_diff_node = None
differences = []

for node in sorted(all_nodes):
    baseline_rank = baseline_ranks.get(node, 0.0)
    mitigation_rank = mitigation_ranks.get(node, 0.0)
    diff = abs(baseline_rank - mitigation_rank)
    if diff > max_diff:
        max_diff = diff
        max_diff_node = node
    if diff > 1e-6:  # Significant difference
        differences.append((node, baseline_rank, mitigation_rank, diff))

if differences:
    print(f"Found {len(differences)} nodes with differences > 1e-6:")
    for node, b_rank, m_rank, diff in differences[:10]:
        print(f"  {node}: baseline={b_rank:.10f}, mitigation={m_rank:.10f}, diff={diff:.10f}")
    if len(differences) > 10:
        print(f"  ... and {len(differences) - 10} more")
else:
    print("✓ No significant differences found (all differences < 1e-6)")

print()
print(f"Maximum difference: {max_diff:.10f} at node {max_diff_node}")
if max_diff_node:
    print(f"  Baseline: {baseline_ranks.get(max_diff_node, 0.0):.10f}")
    print(f"  Mitigation: {mitigation_ranks.get(max_diff_node, 0.0):.10f}")

# Check if ranks are essentially identical
tolerance = 1e-5
all_match = True
for node in sorted(all_nodes):
    baseline_rank = baseline_ranks.get(node, 0.0)
    mitigation_rank = mitigation_ranks.get(node, 0.0)
    if abs(baseline_rank - mitigation_rank) > tolerance:
        all_match = False
        break

print()
if all_match:
    print(f"✓ SUCCESS: All ranks match within tolerance ({tolerance})")
else:
    print(f"✗ FAILURE: Ranks differ beyond tolerance ({tolerance})")

