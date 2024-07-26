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

	sumInt64Cmd := flag.NewFlagSet("sum-int64", flag.ExitOnError)
	sumInt64PathArg := sumInt64Cmd.String("path", "", "file path of parquet file to sum of int64 column")
	sumInt64FieldArg := sumInt64Cmd.String("field", "", "field path of parquet file to sum of int64 column")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s <sub-command>\n\n", os.Args[0])
		inspectCmd.Usage()
		sumInt64Cmd.Usage()
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

	case "sum-int64":
		sumInt64Cmd.Parse(os.Args[2:])
		if err := sumInt64(*sumInt64PathArg, *sumInt64FieldArg); err != nil {
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

func sumInt64(path string, field string) error {
	if len(path) == 0 || len(field) == 0 {
		flag.Usage()
		os.Exit(2)
	}

	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open parquet file: %w", err)
	}
	defer f.Close()

	par := internal.NewParquet(f)
	reader, err := internal.NewReader(context.Background(), par)
	if err != nil {
		return fmt.Errorf("failed to create reader: %w", err)
	}

	sum, err := reader.SumInt64(context.Background(), field)
	if err != nil {
		return fmt.Errorf("failed to aggregate field '%s': %w", field, err)
	}

	fmt.Printf("Sum: %d\n", sum)
	return nil
}
