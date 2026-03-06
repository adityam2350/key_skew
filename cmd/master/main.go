package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <run|run-pagerank> [flags]\n", os.Args[0])
		os.Exit(1)
	}

	subcommand := os.Args[1]
	switch subcommand {
	case "run":
		RunWordCount()
	case "run-pagerank":
		RunPageRank()
	default:
		fmt.Fprintf(os.Stderr, "Usage: %s <run|run-pagerank> [flags]\n", os.Args[0])
		os.Exit(1)
	}
}
