package master

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"

	"key_skew/common/common"
)

// CreateRunDirectories creates the standard directory structure for a MapReduce run
func CreateRunDirectories(runPath string) error {
	dirs := []string{
		filepath.Join(runPath, "shards"),
		filepath.Join(runPath, "intermediate"),
		filepath.Join(runPath, "shuffle"),
		filepath.Join(runPath, "output"),
		filepath.Join(runPath, "metrics"),
		filepath.Join(runPath, "logs"),
	}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create %s: %w", dir, err)
		}
	}

	return nil
}

// ShardInput shards an input file into M shards using round-robin distribution
func ShardInput(inputPath string, runPath string, M int) error {
	file, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("failed to open input file: %w", err)
	}
	defer file.Close()

	// Create shard files
	shardFiles := make([]*os.File, M)
	shardWriters := make([]*bufio.Writer, M)
	for i := 0; i < M; i++ {
		shardPath := filepath.Join(runPath, "shards", fmt.Sprintf("shard_%03d.jsonl", i))
		shardFile, err := os.Create(shardPath)
		if err != nil {
			return fmt.Errorf("failed to create shard file: %w", err)
		}
		shardFiles[i] = shardFile
		shardWriters[i] = bufio.NewWriter(shardFile)
	}
	defer func() {
		for i, w := range shardWriters {
			w.Flush()
			shardFiles[i].Close()
		}
	}()

	// Distribute lines round-robin
	scanner := bufio.NewScanner(file)
	shardIdx := 0
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) > 0 {
			if _, err := shardWriters[shardIdx].WriteString(line + "\n"); err != nil {
				return fmt.Errorf("failed to write to shard: %w", err)
			}
			shardIdx = (shardIdx + 1) % M
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading input: %w", err)
	}

	return nil
}

// Shuffle collects partition files from mappers and groups them by reducer
func Shuffle(M, R int, runPath string) error {
	common.LogInfo("MASTER", "Shuffling partition files")

	// For each reducer, collect all partition files that belong to it
	for r := 0; r < R; r++ {
		var inputFiles []string
		for m := 0; m < M; m++ {
			partPath := filepath.Join(runPath, "intermediate", fmt.Sprintf("map_%d", m), fmt.Sprintf("part_%03d.jsonl", r))
			if _, err := os.Stat(partPath); err == nil {
				inputFiles = append(inputFiles, partPath)
			}
		}

		// Write list of input files for this reducer
		inputsPath := filepath.Join(runPath, "shuffle", fmt.Sprintf("reduce_%d_inputs.txt", r))
		if err := common.WriteReducerInputs(inputsPath, inputFiles); err != nil {
			return fmt.Errorf("failed to write reducer inputs: %w", err)
		}
	}

	return nil
}

// BuildMapperCommand creates a command to run a mapper
func BuildMapperCommand(mode string, mapID int, shardPath, outDir string, sampleRate float64, R int, planPath string, seed int64, jobName string) *exec.Cmd {
	// Try binary first, fallback to go run
	var cmd *exec.Cmd
	if _, err := os.Stat("bin/mapper"); err == nil {
		cmd = exec.Command("bin/mapper",
			"--mode", mode,
			"--map-id", strconv.Itoa(mapID),
			"--shard", shardPath,
			"--outdir", outDir,
			"--job", jobName,
		)
	} else {
		cmd = exec.Command("go", "run", "./common/cmd/mapper",
			"--mode", mode,
			"--map-id", strconv.Itoa(mapID),
			"--shard", shardPath,
			"--outdir", outDir,
			"--job", jobName,
		)
	}

	if mode == "sample" {
		cmd.Args = append(cmd.Args, "--sample-rate", fmt.Sprintf("%.4f", sampleRate))
		cmd.Args = append(cmd.Args, "--seed", strconv.FormatInt(seed, 10))
	} else {
		cmd.Args = append(cmd.Args, "--R", strconv.Itoa(R))
		if planPath != "" {
			cmd.Args = append(cmd.Args, "--plan", planPath)
		}
	}

	return cmd
}

// BuildReducerCommand creates a command to run a reducer
func BuildReducerCommand(reduceID int, inputsPath, outPath string, jobName string, damping float64, numNodes int) *exec.Cmd {
	var cmd *exec.Cmd
	if _, err := os.Stat("bin/reducer"); err == nil {
		cmd = exec.Command("bin/reducer",
			"--reduce-id", strconv.Itoa(reduceID),
			"--inputs", inputsPath,
			"--out", outPath,
			"--job", jobName,
		)
	} else {
		cmd = exec.Command("go", "run", "./common/cmd/reducer",
			"--reduce-id", strconv.Itoa(reduceID),
			"--inputs", inputsPath,
			"--out", outPath,
			"--job", jobName,
		)
	}

	// Add PageRank-specific parameters
	if jobName == "pagerank" {
		cmd.Args = append(cmd.Args, "--damping", fmt.Sprintf("%.2f", damping))
		cmd.Args = append(cmd.Args, "--num-nodes", strconv.Itoa(numNodes))
	}

	return cmd
}

// RunCommand executes a command with timeout and logging
func RunCommand(cmd *exec.Cmd, logPath string) error {
	// Create log directory if needed
	if err := os.MkdirAll(filepath.Dir(logPath), 0755); err != nil {
		return fmt.Errorf("failed to create log directory: %w", err)
	}

	logFile, err := os.Create(logPath)
	if err != nil {
		return fmt.Errorf("failed to create log file: %w", err)
	}
	defer logFile.Close()

	cmd.Stdout = logFile
	cmd.Stderr = logFile

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start command: %w", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("command failed: %w", err)
		}
		return nil
	case <-ctx.Done():
		cmd.Process.Kill()
		return fmt.Errorf("command timed out")
	}
}

// CopyFile copies a file from src to dst
func CopyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("failed to open source: %w", err)
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("failed to create destination: %w", err)
	}
	defer dstFile.Close()

	if _, err := dstFile.ReadFrom(srcFile); err != nil {
		return fmt.Errorf("failed to copy: %w", err)
	}

	return nil
}

