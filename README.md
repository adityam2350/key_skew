# MapReduce MVP - Reducer Skew Mitigation

A clean, modular Go implementation of a local MapReduce MVP that simulates a distributed system using OS subprocesses. The system demonstrates reducer skew under naive hash partitioning and mitigation via heavy-hitter detection and key salting.

## Features

- **WordCount Job**: Classic MapReduce word counting with skew mitigation
  - **Baseline Mode**: Naive hash partitioning without skew mitigation
  - **Mitigation Mode**: Two-pass map pipeline with sampling, heavy-hitter detection, key salting, and merge_unsalt
- **PageRank Job**: Iterative MapReduce for computing PageRank on directed graphs
  - Multiple iterations with graph structure preservation
  - Configurable damping factor
  - Final ranks output with optional sorting
- **Synthetic Datasets**: 
  - WordCount: Zipf distribution and catastrophe (single-key hotspot) generators
  - PageRank: Zipf-like and heavily skewed graph generators
- **Comprehensive Metrics**: Per-reducer load, coefficient of variation, max/median ratios, and timing for both jobs

## Architecture

The system consists of:

1. **Master**: Coordinator that orchestrates the MapReduce pipeline (supports both WordCount and PageRank)
2. **Mapper**: Worker subprocess that processes input shards (sample or execute mode)
3. **Reducer**: Worker subprocess that aggregates partitioned data
4. **Merge Unsalt**: Utility to combine salted keys back to original keys (WordCount only)
5. **Dataset Generators**: 
   - WordCount: `make_zipf`, `make_catastrophe`
   - PageRank: `make_zipf_graph`, `make_skewed_graph`
6. **Graph Initialization**: `init_pagerank` - Converts edge lists to graph state for PageRank

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
```

Alternatively, you can build manually:

```bash
go build -o bin/master ./cmd/master
go build -o bin/mapper ./cmd/mapper
go build -o bin/reducer ./cmd/reducer
go build -o bin/merge_unsalt ./cmd/merge_unsalt
go build -o bin/make_zipf ./cmd/make_zipf
go build -o bin/make_catastrophe ./cmd/make_catastrophe
```

Or build all at once:

```bash
for dir in cmd/*/; do
    name=$(basename "$dir")
    go build -o "bin/$name" "./cmd/$name"
done
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

Execute iterative PageRank computation:

```bash
./bin/master run-pagerank \
  --input data/graph_init.jsonl \
  --run-dir runs \
  --M 8 \
  --R 8 \
  --max-parallel-maps 8 \
  --iterations 10 \
  --damping 0.85
```

**Parameters:**
- `--input`: Path to initialized graph state JSONL file
- `--M`: Number of mappers
- `--R`: Number of reducers
- `--iterations`: Number of PageRank iterations (default: 10)
- `--damping`: PageRank damping factor (default: 0.85)
- `--max-parallel-maps`: Maximum parallel mappers (default: 8)

## How Salting Works

### Problem: Reducer Skew

In naive hash partitioning, keys with high frequency (heavy hitters) all hash to the same reducer, causing:
- Uneven load distribution
- Some reducers overloaded while others idle
- Poor resource utilization

### Solution: Key Salting

1. **Sampling Pass**: Mappers sample keys and count frequencies
2. **Heavy Hitter Detection**: Master identifies top `heavy-top-pct` keys by count
3. **Salting**: Heavy keys are split into multiple salted variants:
   - Original key: `"the"`
   - Salted keys: `["the", 0]`, `["the", 1]`, ..., `["the", splits-1]`
4. **Partitioning**: Each salted key hashes to a different reducer
5. **Merge Unsalt**: After reduction, salted keys are combined back to original keys

### Example

If `"the"` is a heavy hitter with 8 splits:
- `["the", 0]` → Reducer 3
- `["the", 1]` → Reducer 7
- `["the", 2]` → Reducer 2
- ... (distributed across reducers)

After reduction, merge_unsalt combines all salted variants back to `{"k": "the", "v": total_count}`.

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

```
runs/run_pagerank_20240301_120000/
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

