package gpandas

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"

	"github.com/apoplexi24/gpandas/dataframe"
)

// Read_json reads a JSON file in records orientation (a top-level array of
// objects) and converts it into a DataFrame.
//
// Each object becomes a row; the union of all keys becomes the columns, sorted
// alphabetically for deterministic ordering. Missing keys in a record produce
// null values. JSON numbers are decoded as float64.
//
// Parameters:
//
//	filepath: path to the JSON file.
//
// Returns:
//
//	A pointer to a DataFrame, or an error if the file cannot be read or parsed.
//
// Example:
//
//	df, err := gp.Read_json("data.json")
func (GoPandas) Read_json(filepath string) (*dataframe.DataFrame, error) {
	raw, err := os.ReadFile(filepath)
	if err != nil {
		return nil, fmt.Errorf("error reading file: %w", err)
	}

	var records []map[string]any
	if err := json.Unmarshal(raw, &records); err != nil {
		return nil, fmt.Errorf("error parsing JSON (expected an array of objects): %w", err)
	}

	if len(records) == 0 {
		return nil, fmt.Errorf("no records found in JSON")
	}

	// Collect the union of keys across all records.
	keySet := make(map[string]bool)
	for _, rec := range records {
		for k := range rec {
			keySet[k] = true
		}
	}
	columns := make([]string, 0, len(keySet))
	for k := range keySet {
		columns = append(columns, k)
	}
	sort.Strings(columns)

	// Build per-column value slices, then construct typed Series from them.
	columnsMap := make(map[string]dataframe.Column, len(columns))
	for _, col := range columns {
		values := make(dataframe.Column, len(records))
		for i, rec := range records {
			if v, ok := rec[col]; ok {
				values[i] = v
			} else {
				values[i] = nil
			}
		}
		columnsMap[col] = values
	}

	return dataframe.NewDataFrameFromColumns(columns, columnsMap)
}
