package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"

	"key_skew/common/common"
)

func main() {
	var nRecords int
	var hotFrac float64
	var vocabSize int
	var wordsPerRecord int
	var seed int64
	var outPath string

	flag.IntVar(&nRecords, "N-records", 10000, "Number of records to generate")
	flag.Float64Var(&hotFrac, "hot-frac", 0.5, "Fraction of tokens that are the hot key")
	flag.IntVar(&vocabSize, "vocab-size", 1000, "Vocabulary size (excluding HOT key)")
	flag.IntVar(&wordsPerRecord, "words-per-record", 20, "Number of words per record")
	flag.Int64Var(&seed, "seed", 0, "Random seed")
	flag.StringVar(&outPath, "out", "data/catastrophe.jsonl", "Output file path")
	flag.Parse()

	if err := common.InitLogging(".", "make_catastrophe"); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logging: %v\n", err)
		os.Exit(1)
	}
	defer common.CloseLogging()

	common.LogInfo("MAKE_CATASTROPHE", "Generating catastrophe dataset: N=%d, hot_frac=%.2f, vocab=%d, words/record=%d, seed=%d",
		nRecords, hotFrac, vocabSize, wordsPerRecord, seed)

	if err := os.MkdirAll(filepath.Dir(outPath), 0755); err != nil {
		common.LogError("MAKE_CATASTROPHE", "Failed to create output directory: %v", err)
		os.Exit(1)
	}

	file, err := os.Create(outPath)
	if err != nil {
		common.LogError("MAKE_CATASTROPHE", "Failed to create output file: %v", err)
		os.Exit(1)
	}
	defer file.Close()

	rng := rand.New(rand.NewSource(seed))

	vocab := make([]string, vocabSize)
	for i := 0; i < vocabSize; i++ {
		vocab[i] = fmt.Sprintf("w%06d", i+1)
	}

	hotKey := "HOT"

	encoder := json.NewEncoder(file)
	for i := 0; i < nRecords; i++ {
		words := make([]string, wordsPerRecord)
		for j := 0; j < wordsPerRecord; j++ {
			if rng.Float64() < hotFrac {
				words[j] = hotKey
			} else {
				idx := rng.Intn(vocabSize)
				words[j] = vocab[idx]
			}
		}

		record := common.InputRecord{
			Text: words[0],
		}
		for j := 1; j < len(words); j++ {
			record.Text += " " + words[j]
		}

		if err := encoder.Encode(record); err != nil {
			common.LogError("MAKE_CATASTROPHE", "Failed to encode record %d: %v", i, err)
			os.Exit(1)
		}
	}

	common.LogInfo("MAKE_CATASTROPHE", "Generated %d records to %s", nRecords, outPath)
}
