package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/murakmii/retsu/internal"
	"os"
)

func main() {
	inspectCmd := flag.NewFlagSet("inspect", flag.ExitOnError)
	inspectPathArg := inspectCmd.String("path", "", "file path of parquet file to inspect")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s (inspect|TODO)\n\n", os.Args[0])
		inspectCmd.Usage()
	}

	if len(os.Args) < 2 {
		flag.Usage()
		os.Exit(2)
	}

	switch os.Args[1] {
	case "inspect":
		inspectCmd.Parse(os.Args[2:])
		if err := inspect(*inspectPathArg); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}

	default:
		flag.Usage()
		os.Exit(2)
	}
}

func inspect(path string) error {
	if len(path) == 0 {
		flag.Usage()
		os.Exit(2)
	}

	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open parquet file: %w", err)
	}
	defer f.Close()

	par := internal.NewParquet(f)
	inspected, err := par.Inspect(context.Background())
	if err != nil {
		return fmt.Errorf("failed to inspect parquet file: %w", err)
	}

	j, err := json.MarshalIndent(inspected, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal inspection result: %w", err)
	}

	fmt.Println(string(j))
	return nil
}
