package main

import (
	"fmt"
	"os"
)

func main() {
	// Help message
	if len(os.Args) < 2 {
		fmt.Println("Usage: pglitedb-benchmarks <command> [options]")
		fmt.Println()
		fmt.Println("Commands:")
		fmt.Println("  extended-benchmark   Run extended benchmark tests (1 hour duration)")
		fmt.Println("  profiling-benchmark  Run profiling benchmark tests with detailed profiling")
		fmt.Println("  concurrent-validation Run concurrent access validation tests")
		fmt.Println()
		fmt.Println("Use 'pglitedb-benchmarks <command> -h' for more information about a command.")
		os.Exit(1)
	}

	// Parse command
	switch os.Args[1] {
	case "extended-benchmark":
		extendedMain()
	case "profiling-benchmark":
		profilingMain()
	case "concurrent-validation":
		concurrentMain()
	default:
		fmt.Printf("Unknown command: %s\n", os.Args[1])
		os.Exit(1)
	}
}