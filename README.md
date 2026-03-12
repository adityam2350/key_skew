# MapReduce MVP - Reducer Skew Mitigation

A clean, modular Go implementation of a local MapReduce MVP that simulates a distributed system using OS subprocesses. The system demonstrates reducer skew under naive hash partitioning and mitigation via heavy-hitter detection and key salting.

## Features

- **WordCount Job**: Classic MapReduce word counting with skew mitigation
  - **Baseline Mode**: Naive hash partitioning without skew mitigation
  - **Mitigation Mode**: Two-pass map pipeline with sampling, heavy-hitter detection, key salting, and merge_unsalt
- **PageRank Job**: Iterative MapReduce for computing PageRank on directed graphs
  - **Baseline Mode**: Single-stage PageRank computation without skew mitigation
  - **Mitigation Mode**: Two-stage aggregation with selective salting for heavy destination nodes
  - Multiple iterations with graph structure preservation
  - Configurable damping factor
  - Final ranks output with optional sorting
- **Synthetic Datasets**: 
  - WordCount: Zipf distribution and catastrophe (single-key hotspot) generators
  - PageRank: Zipf-like and heavily skewed graph generators
- **Comprehensive Metrics**: Per-reducer load, coefficient of variation, max/median ratios, and timing for both jobs

## Architecture

The codebase is organized into three main directories:

- **`common/`**: Shared code used by both WordCount and PageRank jobs
  - `cmd/master/`: Master dispatcher and shared utilities
  - `cmd/mapper/`: Mapper worker (job-agnostic)
  - `cmd/reducer/`: Reducer worker (job-agnostic)
  - `internal/common/`: Shared utilities (types, hashing, I/O, logging, metrics)
  - `internal/jobs/`: Job interface and registry
  - `internal/master/`: Shared master utilities

- **`wordcount/`**: WordCount-specific code
  - `cmd/master/`: WordCount driver
  - `cmd/merge_unsalt/`: Utility to combine salted keys
  - `cmd/make_zipf/`: Zipf distribution generator
  - `cmd/make_catastrophe/`: Catastrophe dataset generator
  - `internal/jobs/`: WordCount job implementation

- **`pagerank/`**: PageRank-specific code
  - `cmd/master/`: PageRank driver
  - `cmd/init_pagerank/`: Graph initialization utility
  - `cmd/make_zipf_graph/`: Zipf-like graph generator
  - `cmd/make_skewed_graph/`: Heavily skewed graph generator
  - `cmd/analyze_heavy/`: Heavy-hitter analysis tool for mitigation mode
  - `internal/jobs/`: PageRank job implementation (baseline, stage1, stage2)
  - `internal/heavy/`: Heavy-hitter infrastructure (config, salted keys)

The system consists of:

1. **Master**: Coordinator that orchestrates the MapReduce pipeline (supports both WordCount and PageRank)
2. **Mapper**: Worker subprocess that processes input shards (sample or execute mode)
3. **Reducer**: Worker subprocess that aggregates partitioned data
4. **Merge Unsalt**: Utility to combine salted keys back to original keys (WordCount only)
5. **Dataset Generators**: 
   - WordCount: `make_zipf`, `make_catastrophe`
   - PageRank: `make_zipf_graph`, `make_skewed_graph`
6. **Graph Initialization**: `init_pagerank` - Converts edge lists to graph state for PageRank
7. **Heavy-Hitter Analysis**: `analyze_heavy` - Analyzes graph and generates heavy-hitter config for PageRank mitigation

### Data Flow

```
Input JSONL → Sharding → Mappers → Partition Files → Shuffle → Reducers → Merge → Final Output
```

In mitigation mode:
```
Input → Sharding → Sample Mappers → Heavy Hitter Detection → Plan → Execute Mappers → Shuffle → Reducers → Merge Unsalt → Final Output
```

## Building

The easiest way to build all binaries is using `make`:

```bash
make
```

Or build individual binaries:

```bash
make master
make mapper
make reducer
make merge_unsalt
make make_zipf
make make_catastrophe
make init_pagerank
make make_zipf_graph
make make_skewed_graph
make analyze_heavy
```

Alternatively, you can build manually:

```bash
go build -o bin/master ./common/cmd/master
go build -o bin/mapper ./common/cmd/mapper
go build -o bin/reducer ./common/cmd/reducer
go build -o bin/merge_unsalt ./wordcount/cmd/merge_unsalt
go build -o bin/make_zipf ./wordcount/cmd/make_zipf
go build -o bin/make_catastrophe ./wordcount/cmd/make_catastrophe
go build -o bin/init_pagerank ./pagerank/cmd/init_pagerank
go build -o bin/make_zipf_graph ./pagerank/cmd/make_zipf_graph
go build -o bin/make_skewed_graph ./pagerank/cmd/make_skewed_graph
go build -o bin/analyze_heavy ./pagerank/cmd/analyze_heavy
```

## Usage

### WordCount Job

#### 1. Generate WordCount Datasets

**Zipf Distribution** (power-law distribution):
```bash
./bin/make_zipf --N-records 20000 --vocab-size 5000 --zipf-s 1.1 --words-per-record 20 --seed 0 --out data/zipf.jsonl
```

**Catastrophe Dataset** (single hot key):
```bash
./bin/make_catastrophe --N-records 20000 --hot-frac 0.5 --vocab-size 1000 --words-per-record 20 --seed 0 --out data/catastrophe.jsonl
```

#### 2. Run WordCount - Baseline Mode

Naive hash partitioning without skew mitigation:

```bash
./bin/master run \
  --input data/zipf.jsonl \
  --run-dir runs \
  --M 8 \
  --R 8 \
  --max-parallel-maps 8 \
  --mode baseline
```

#### 3. Run WordCount - Mitigation Mode

Two-pass pipeline with heavy-hitter detection and salting:

```bash
./bin/master run \
  --input data/zipf.jsonl \
  --run-dir runs \
  --M 8 \
  --R 8 \
  --max-parallel-maps 8 \
  --mode mitigation \
  --sample-rate 0.01 \
  --heavy-top-pct 0.01 \
  --fixed-splits 8 \
  --seed 0
```

### PageRank Job

#### 1. Generate Graph Datasets

**Zipf-like Graph** (power-law in-degree distribution):
```bash
./bin/make_zipf_graph \
  --num-nodes 1000 \
  --num-edges 10000 \
  --zipf-s 1.1 \
  --zipf-v 1.0 \
  --seed 0 \
  --out data/graph_zipf.txt
```

**Heavily Skewed Graph** (extreme skew with hot nodes):
```bash
./bin/make_skewed_graph \
  --num-nodes 1000 \
  --num-edges 10000 \
  --num-hot-nodes 10 \
  --hot-edge-frac 0.5 \
  --seed 0 \
  --out data/graph_skewed.txt
```

#### 2. Initialize Graph State

Convert edge list to initial graph state for PageRank:

```bash
./bin/init_pagerank \
  --input data/graph_zipf.txt \
  --out data/graph_init.jsonl
```

This creates:
- `data/graph_init.jsonl` - Initial graph state with ranks initialized to 1/N
- `data/graph_init.jsonl.meta.json` - Metadata (num_nodes, damping, iterations, etc.)

#### 3. Run PageRank

**Baseline Mode (no salting, backward compatible default):**

```bash
./bin/master run-pagerank \
  --input data/graph_init.jsonl \
  --mode baseline \
  --run-dir runs \
  --M 8 \
  --R 8 \
  --max-parallel-maps 8 \
  --iterations 10 \
  --damping 0.85
```

**Mitigation Mode (with selective salting for heavy destination nodes):**

First, analyze the graph to identify heavy-hitter destination nodes:

```bash
./bin/analyze_heavy \
  --input data/graph_zipf.txt \
  --top-k 10 \
  --default-splits 8 \
  --out heavy_config.json
```

Then run PageRank with mitigation:

```bash
./bin/master run-pagerank \
  --input data/graph_init.jsonl \
  --mode mitigation \
  --heavy-config heavy_config.json \
  --run-dir runs \
  --M 8 \
  --R 8 \
  --max-parallel-maps 8 \
  --iterations 10 \
  --damping 0.85
```

**Parameters:**
- `--input`: Path to initialized graph state JSONL file
- `--mode`: Execution mode: `baseline` (default) or `mitigation`
- `--heavy-config`: Path to heavy-hitter config file (required for mitigation mode)
- `--M`: Number of mappers
- `--R`: Number of reducers
- `--iterations`: Number of PageRank iterations (default: 10)
- `--damping`: PageRank damping factor (default: 0.85)
- `--max-parallel-maps`: Maximum parallel mappers (default: 8)

**analyze_heavy Parameters:**
- `--input`: Input edge list file path (src dst format)
- `--top-k`: Number of top heavy-hitter nodes to select (default: 10)
- `--default-splits`: Default number of splits for heavy nodes (default: 8)
- `--out`: Output heavy config JSON file path

## How Salting Works

### Problem: Reducer Skew

In naive hash partitioning, keys with high frequency (heavy hitters) all hash to the same reducer, causing:
- Uneven load distribution
- Some reducers overloaded while others idle
- Poor resource utilization

### Solution: Key Salting

#### WordCount Salting

1. **Sampling Pass**: Mappers sample keys and count frequencies
2. **Heavy Hitter Detection**: Master identifies top `heavy-top-pct` keys by count
3. **Salting**: Heavy keys are split into multiple salted variants:
   - Original key: `"the"`
   - Salted keys: `["the", 0]`, `["the", 1]`, ..., `["the", splits-1]`
4. **Partitioning**: Each salted key hashes to a different reducer
5. **Merge Unsalt**: After reduction, salted keys are combined back to original keys

**Example:** If `"the"` is a heavy hitter with 8 splits:
- `["the", 0]` → Reducer 3
- `["the", 1]` → Reducer 7
- `["the", 2]` → Reducer 2
- ... (distributed across reducers)

After reduction, merge_unsalt combines all salted variants back to `{"k": "the", "v": total_count}`.

#### PageRank Selective Salting

PageRank uses a two-stage aggregation pipeline for heavy destination nodes:

1. **Heavy-Hitter Analysis**: Preprocess graph to identify top-K destination nodes by in-degree
2. **Stage 1 (Salted Aggregation)**:
   - Contributions to heavy destination nodes are salted: `(nodeID, salt)` where `salt = hash(sourceNodeID) % splitCount`
   - Contributions to normal nodes remain unsalted
   - Adjacency lists are never salted (preserve graph structure)
   - Stage 1 reducers produce partial sums for each salted/unsalted key
3. **Stage 2 (Merge and Update)**:
   - Stage 2 mappers re-key all partial sums to original destination node IDs
   - Stage 2 reducers merge partial sums, recover adjacency lists, and compute new ranks

**Key Differences from WordCount:**
- Only heavy destination nodes are salted (selective salting)
- Graph structure (adjacency lists) is never salted
- Deterministic salting based on source node ID (same source always maps to same salt bucket)
- Two-stage pipeline instead of merge_unsalt utility

**Example:** If node `"n000001"` is heavy with 8 splits:
- Contribution from `"n000010"` → salted as `("n000001", hash("n000010") % 8)`
- Contribution from `"n000020"` → salted as `("n000001", hash("n000020") % 8)`
- All salted contributions are distributed across reducers in Stage 1
- Stage 2 merges them back to `"n000001"` for rank update

## Output Structure

### WordCount Output

Each WordCount run creates a timestamped directory:

```
runs/run_baseline_20240301_120000/  (or run_mitigation_...)
├── shards/                    # Input shards
│   ├── shard_000.jsonl
│   └── ...
├── intermediate/              # Mapper outputs
│   ├── map_0/
│   │   ├── part_000.jsonl
│   │   ├── part_001.jsonl
│   │   ├── map_stats.json
│   │   └── sample_counts.json (mitigation only)
│   └── ...
├── shuffle/                   # Reducer input lists
│   ├── reduce_0_inputs.txt
│   └── ...
├── output/                    # Final results
│   ├── reduce_0.jsonl
│   ├── reduce_0.stats.json
│   └── final.jsonl
├── metrics/
│   └── run_summary.json       # Comprehensive metrics
└── logs/                      # Component logs
```

### PageRank Output

Each PageRank run creates a timestamped directory with iteration subdirectories:

**Baseline mode:**
```
runs/run_pagerank_baseline_20240301_120000/
```

**Mitigation mode:**
```
runs/run_pagerank_mitigation_20240301_120000/
├── heavy_config.json          # Copied heavy-hitter config
├── iter_000/                  # Initial state
│   └── input.jsonl
├── iter_001/                  # First iteration
│   ├── shards/
│   ├── intermediate/
│   ├── shuffle/
│   ├── output/
│   └── next_input.jsonl       # Input for next iteration
├── iter_002/                  # Second iteration
│   └── ...
├── final/                     # Final results
│   ├── ranks.jsonl            # Final ranks (unsorted)
│   └── ranks_sorted.jsonl     # Final ranks (sorted by rank)
├── metrics/
│   └── run_summary.json       # Comprehensive metrics
└── logs/                      # Component logs
```

## Metrics

### WordCount Metrics

The `metrics/run_summary.json` file for WordCount contains:

```json
{
  "mode": "baseline|mitigation",
  "M": 8,
  "R": 8,
  "times_ms": {
    "total": 1234,
    "map_pass1": 100,    // mitigation only
    "map_pass2": 500,
    "reduce": 600,
    "merge": 34          // mitigation only
  },
  "reducer_load": {
    "bytes": [1000, 950, 1200, ...],
    "records": [500, 480, 600, ...]
  },
  "cov": {
    "bytes": 0.15,       // Coefficient of variation
    "records": 0.18
  },
  "max_median_ratio": {
    "bytes": 1.5,
    "records": 1.6
  },
  "heavy_hitters": {
    "count": 10,
    "top_keys": ["the", "and", ...],
    ...
  }
}
```

### PageRank Metrics

The `metrics/run_summary.json` file for PageRank contains:

```json
{
  "mode": "baseline|mitigation",
  "M": 8,
  "R": 8,
  "iterations": 10,
  "damping": 0.85,
  "num_nodes": 1000,
  "total_time_ms": 5000,
  "iteration_times_ms": {
    "iter_001": 500,
    "iter_002": 480,
    ...
  },
  "iteration_metrics": {
    "iter_001": {
      "map_time_ms": 200,
      "shuffle_time_ms": 10,
      "reduce_time_ms": 250,
      "merge_time_ms": 40,
      "total_time_ms": 500,
      "reducer_load": {
        "bytes": [1000, 950, 1200, ...],
        "records": [500, 480, 600, ...]
      },
      "cov": {
        "bytes": 0.15,
        "records": 0.18
      },
      "max_median_ratio": {
        "bytes": 1.5,
        "records": 1.6
      }
    },
    ...
  }
}
```

### Interpreting Metrics

- **CoV (Coefficient of Variation)**: Lower is better. Measures relative variability of reducer loads.
- **Max/Median Ratio**: Lower is better. Ratio of maximum to median reducer load.
- **Reducer Load**: Distribution of bytes and records per reducer.

**For WordCount (mitigation mode)**, you should see:
- Lower CoV (more balanced load)
- Lower max/median ratio (less skew)
- More even distribution in `reducer_load`

**For PageRank**, metrics are tracked per iteration, allowing you to:
- Analyze performance trends across iterations
- Identify load balancing issues in specific iterations
- Compare iteration times to detect convergence patterns
- Compare baseline vs mitigation mode performance and load distribution

## Implementation Details

### Stable Hashing

Uses CRC32 (IEEE polynomial) for deterministic partitioning:
- Unsalted key: `crc32([]byte("word")) % R`
- Salted key: `crc32([]byte("word#3")) % R`

### Round-Robin Salting

Each mapper maintains per-key salt counters for even distribution:
- First occurrence of heavy key → salt 0
- Second occurrence → salt 1
- ... (wraps around modulo splits)

### File-Based Communication

All communication between components is file-based:
- Mappers write partition files
- Master creates reducer input lists
- Reducers read from file lists
- No IPC/sockets required

## Troubleshooting

### Binaries Not Found

If `bin/mapper`, `bin/reducer`, etc. don't exist, the master will fallback to `go run ./cmd/mapper`. For faster execution, build binaries first.

### Permission Errors

Ensure the run directory is writable:
```bash
chmod -R 755 runs/
```

### Out of Memory

For large datasets, reduce `--M` and `--R` or increase system memory.

### Invalid JSON

**WordCount**: Check input file format. Each line must be valid JSON:
```json
{"text": "word1 word2 word3"}
```

**PageRank**: Graph initialization input must be edge list format:
```
n000001 n000002
n000001 n000003
n000002 n000001
```

After initialization, PageRank uses JSONL node records:
```json
{"node": "n000001", "rank": 0.01, "neighbors": ["n000002", "n000003"]}
```

## Job Interface

The framework uses a pluggable job interface, making it easy to add new MapReduce jobs:

- **WordCount**: Integer values, supports skew mitigation, outputs KV records
- **PageRank**: String values (JSON), no skew mitigation, outputs raw JSON node records

To add a new job, implement the `Job` interface in `internal/jobs/` and register it in the job registry.

## Project Structure

```
key_skew/
├── go.mod
├── README.md
├── Makefile
├── cmd/
│   ├── master/
│   │   ├── main.go              # Dispatcher
│   │   ├── common.go            # Shared utilities
│   │   ├── wordcount_driver.go  # WordCount execution
│   │   └── pagerank_driver.go   # PageRank execution
│   ├── mapper/main.go           # Mapper worker
│   ├── reducer/main.go          # Reducer worker
│   ├── merge_unsalt/main.go     # Merge utility (WordCount)
│   ├── make_zipf/main.go        # Zipf word generator
│   ├── make_catastrophe/main.go # Catastrophe generator
│   ├── make_zipf_graph/main.go  # Zipf graph generator
│   ├── make_skewed_graph/main.go # Skewed graph generator
│   └── init_pagerank/main.go    # Graph initialization
└── internal/
    ├── common/
    │   ├── types.go             # Shared types
    │   ├── hashing.go           # Partitioning
    │   ├── io.go                # JSONL I/O
    │   ├── metrics.go           # Metrics computation
    │   └── logging.go           # Logging
    └── jobs/
        ├── interface.go         # Job interface
        ├── wordcount.go         # WordCount implementation
        ├── wordcount_job.go     # WordCount job wrapper
        ├── pagerank.go          # PageRank implementation
        └── pagerank_job.go      # PageRank job wrapper
```

## License

Standard library only - no external dependencies.

