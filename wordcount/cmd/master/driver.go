package main

import (
	"fmt"
	"os"

	wordcountmaster "key_skew/wordcount/master"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <run> [flags]\n", os.Args[0])
		os.Exit(1)
	}

	subcommand := os.Args[1]
	if subcommand != "run" {
		fmt.Fprintf(os.Stderr, "Usage: %s <run> [flags]\n", os.Args[0])
		os.Exit(1)
	}

	wordcountmaster.RunWordCount()
}
