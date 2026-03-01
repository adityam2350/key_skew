# MapReduce MVP - Reducer Skew Mitigation

A clean, modular Go implementation of a local MapReduce MVP that simulates a distributed system using OS subprocesses. The system demonstrates reducer skew under naive hash partitioning and mitigation via heavy-hitter detection and key salting.

## Features

- **Baseline Mode**: Naive hash partitioning without skew mitigation
- **Mitigation Mode**: Two-pass map pipeline with sampling, heavy-hitter detection, key salting, and merge_unsalt
- **WordCount Job**: Default MapReduce job implementation
- **Synthetic Datasets**: Zipf distribution and catastrophe (single-key hotspot) generators
- **Comprehensive Metrics**: Per-reducer load, coefficient of variation, max/median ratios, and timing

## Architecture

The system consists of:

1. **Master**: Coordinator that orchestrates the MapReduce pipeline
2. **Mapper**: Worker subprocess that processes input shards (sample or execute mode)
3. **Reducer**: Worker subprocess that aggregates partitioned data
4. **Merge Unsalt**: Utility to combine salted keys back to original keys
5. **Dataset Generators**: Tools to create synthetic skewed datasets

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

### 1. Generate Datasets

**Zipf Distribution** (power-law distribution):
```bash
./bin/make_zipf --N-records 20000 --vocab-size 5000 --zipf-s 1.1 --words-per-record 20 --seed 0 --out data/zipf.jsonl
```

**Catastrophe Dataset** (single hot key):
```bash
./bin/make_catastrophe --N-records 20000 --hot-frac 0.5 --vocab-size 1000 --words-per-record 20 --seed 0 --out data/catastrophe.jsonl
```

### 2. Run Baseline Mode

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

### 3. Run Mitigation Mode

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

Each run creates a timestamped directory:

```
runs/run_20240301_120000/
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

## Metrics

The `metrics/run_summary.json` file contains:

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

### Interpreting Metrics

- **CoV (Coefficient of Variation)**: Lower is better. Measures relative variability of reducer loads.
- **Max/Median Ratio**: Lower is better. Ratio of maximum to median reducer load.
- **Reducer Load**: Distribution of bytes and records per reducer.

In mitigation mode, you should see:
- Lower CoV (more balanced load)
- Lower max/median ratio (less skew)
- More even distribution in `reducer_load`

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

Check input file format. Each line must be valid JSON:
```json
{"text": "word1 word2 word3"}
```

## Project Structure

```
key_skew/
├── go.mod
├── README.md
├── cmd/
│   ├── master/main.go       # Master coordinator
│   ├── mapper/main.go       # Mapper worker
│   ├── reducer/main.go      # Reducer worker
│   ├── merge_unsalt/main.go # Merge utility
│   ├── make_zipf/main.go    # Zipf generator
│   └── make_catastrophe/main.go
└── internal/
    ├── common/
    │   ├── types.go         # Shared types
    │   ├── hashing.go       # Partitioning
    │   ├── io.go            # JSONL I/O
    │   ├── metrics.go       # Metrics computation
    │   └── logging.go       # Logging
    └── jobs/
        └── wordcount.go     # WordCount job
```

## License

Standard library only - no external dependencies.

