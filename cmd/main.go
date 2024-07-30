package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/murakmii/retsu/internal"
	"os"
)

func main() {
	switch os.Args[1] {
	case "inspect":
		if err := inspect(os.Args[2]); err != nil {
			panic(err)
		}
	case "sum-int64":
		if err := sumInt64(os.Args[2], os.Args[3]); err != nil {
			panic(err)
		}
	default:
		panic("unknown command")
	}
}

func inspect(path string) error {
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
