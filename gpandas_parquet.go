package gpandas

import (
	"fmt"
	"os"

	"github.com/apoplexi24/gpandas/dataframe"
	"github.com/parquet-go/parquet-go"
)

// Read_parquet reads a Parquet file into a DataFrame.
//
// Column types are inferred from the Parquet schema: INT64 -> int64,
// DOUBLE/FLOAT -> float64, BOOLEAN -> bool, and BYTE_ARRAY -> string. Columns are
// ordered as stored in the Parquet schema (alphabetically).
//
// Parameters:
//
//	filepath: path to the .parquet file.
//
// Returns:
//
//	A pointer to a DataFrame, or an error if the file cannot be read.
//
// Example:
//
//	df, err := gp.Read_parquet("data.parquet")
func (GoPandas) Read_parquet(filepath string) (*dataframe.DataFrame, error) {
	f, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %w", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("error stating file: %w", err)
	}

	pf, err := parquet.OpenFile(f, info.Size())
	if err != nil {
		return nil, fmt.Errorf("error opening parquet file: %w", err)
	}
	schema := pf.Schema()

	// Column order as stored in the schema.
	fields := schema.Fields()
	order := make([]string, len(fields))
	for i, field := range fields {
		order[i] = field.Name()
	}

	reader := parquet.NewGenericReader[map[string]any](f, schema)
	defer reader.Close()

	numRows := int(reader.NumRows())
	rows := make([]map[string]any, numRows)
	for i := range rows {
		rows[i] = map[string]any{}
	}
	if numRows > 0 {
		if _, err := reader.Read(rows); err != nil && err.Error() != "EOF" {
			return nil, fmt.Errorf("error reading parquet rows: %w", err)
		}
	}

	// Collect per-column values and let the constructor infer typed Series.
	cols := make(map[string]dataframe.Column, len(order))
	for _, name := range order {
		values := make(dataframe.Column, numRows)
		for i := 0; i < numRows; i++ {
			values[i] = rows[i][name]
		}
		cols[name] = values
	}

	if len(order) == 0 {
		return nil, fmt.Errorf("parquet file has no columns")
	}

	return dataframe.NewDataFrameFromColumns(order, cols)
}
