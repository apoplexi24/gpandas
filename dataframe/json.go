package dataframe

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

// ToJSON serializes the DataFrame to JSON in records orientation (an array of
// objects, one per row). Column order is preserved in each object. Null values
// are emitted as JSON null.
//
// If filepath is non-empty, the JSON is written to that file and ("", nil) is
// returned on success. Otherwise the JSON string is returned.
//
// This is analogous to df.to_json(orient="records") in pandas.
//
// Example:
//
//	s, err := df.ToJSON("")               // return as string
//	_, err := df.ToJSON("out.json")        // write to file
func (df *DataFrame) ToJSON(filepath string) (string, error) {
	if df == nil {
		return "", errors.New("ToJSON: DataFrame is nil")
	}

	df.RLock()
	defer df.RUnlock()

	rowCount := 0
	if len(df.ColumnOrder) > 0 {
		rowCount = df.Columns[df.ColumnOrder[0]].Len()
	}

	var buf bytes.Buffer
	buf.WriteByte('[')

	for r := 0; r < rowCount; r++ {
		if r > 0 {
			buf.WriteByte(',')
		}
		buf.WriteByte('{')
		for c, colName := range df.ColumnOrder {
			if c > 0 {
				buf.WriteByte(',')
			}
			// Marshal the key.
			keyBytes, err := json.Marshal(colName)
			if err != nil {
				return "", fmt.Errorf("ToJSON: marshaling key '%s': %w", colName, err)
			}
			buf.Write(keyBytes)
			buf.WriteByte(':')

			// Marshal the value (null-aware).
			series := df.Columns[colName]
			var valBytes []byte
			if series.IsNull(r) {
				valBytes = []byte("null")
			} else {
				v, _ := series.At(r)
				valBytes, err = json.Marshal(v)
				if err != nil {
					return "", fmt.Errorf("ToJSON: marshaling column '%s' row %d: %w", colName, r, err)
				}
			}
			buf.Write(valBytes)
		}
		buf.WriteByte('}')
	}

	buf.WriteByte(']')

	if filepath != "" {
		if err := os.WriteFile(filepath, buf.Bytes(), 0644); err != nil {
			return "", fmt.Errorf("ToJSON: failed to write file: %w", err)
		}
		return "", nil
	}

	return buf.String(), nil
}
