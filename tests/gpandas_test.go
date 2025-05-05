package gpandas_test

import (
	"gpandas"
	"gpandas/dataframe"
	"os"
	"path/filepath"
	"testing"
)

func TestRead_csv(t *testing.T) {
	// Create temporary directory for test files
	tmpDir, err := os.MkdirTemp("", "gpandas_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	tests := []struct {
		name        string
		csvContent  string
		expectError bool
	}{
		{
			name: "valid csv",
			csvContent: `name,age,city
John,30,New York
Alice,25,London
Bob,35,Paris`,
			expectError: false,
		},
		{
			name: "empty csv",
			csvContent: `name,age,city
`,
			expectError: true,
		},
		{
			name: "inconsistent columns",
			csvContent: `name,age,city
John,30
Alice,25,London,Extra
Bob,35,Paris`,
			expectError: true,
		},
		{
			name:        "empty file",
			csvContent:  "",
			expectError: true,
		},
		{
			name:        "only headers",
			csvContent:  `name,age,city`,
			expectError: true,
		},
		{
			name: "valid csv with quoted fields",
			csvContent: `name,description,city
John,"Software Engineer, Senior",New York
Alice,"Product Manager, Lead",London
Bob,"Data Scientist, ML",Paris`,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create test file
			testFile := filepath.Join(tmpDir, tt.name+".csv")
			err := os.WriteFile(testFile, []byte(tt.csvContent), 0644)
			if err != nil {
				t.Fatalf("failed to create test file: %v", err)
			}

			// Test Read_csv
			pd := gpandas.GoPandas{}
			df, err := pd.Read_csv(testFile)

			// Check error expectations
			if tt.expectError && err == nil {
				t.Error("expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			// Additional checks for successful cases
			if !tt.expectError && err == nil {
				// Check if DataFrame is not nil
				if df == nil {
					t.Error("expected non-nil DataFrame")
					return
				}

				// Check if columns are present
				if len(df.Columns) == 0 {
					t.Error("expected non-empty columns")
				}

				// Check if data is present
				if df.Rows() == 0 {
					t.Error("expected non-empty data")
				}

				// Check if all columns have the same length
				numRows := df.Rows()
				for _, col := range df.Columns {
					series, ok := df.Series[col]
					if !ok {
						t.Errorf("column %s not found in Series map", col)
						continue
					}

					if series.Len() != numRows {
						t.Errorf("column %s has inconsistent length: expected %d, got %d",
							col, numRows, series.Len())
					}
				}
			}
		})
	}

	// Test non-existent file
	t.Run("non-existent file", func(t *testing.T) {
		pd := gpandas.GoPandas{}
		_, err := pd.Read_csv("non_existent_file.csv")
		if err == nil {
			t.Error("expected error for non-existent file but got none")
		}
	})
}

func TestRead_csvDataTypes(t *testing.T) {
	// Create temporary directory for test files
	tmpDir, err := os.MkdirTemp("", "gpandas_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Test file with different data types
	csvContent := `name,age,active,score
John,30,true,95.5
Alice,25,false,87.3
Bob,35,true,92.8`

	testFile := filepath.Join(tmpDir, "types_test.csv")
	err = os.WriteFile(testFile, []byte(csvContent), 0644)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	pd := gpandas.GoPandas{}
	df, err := pd.Read_csv(testFile)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify all values are StringCol (correct behavior)
	for _, colName := range df.Columns {
		series, ok := df.Series[colName]
		if !ok {
			t.Errorf("column %s not found in Series map", colName)
			continue
		}

		for i := 0; i < series.Len(); i++ {
			val := series.GetValue(i)
			// Nullable values are now wrapped in their respective types, not directly StringCol
			if val == nil {
				continue
			}
			switch series.(type) {
			case *dataframe.StringSeries:
				// This is the expected type
			default:
				t.Errorf("expected StringSeries type for column %s, got %T", colName, series)
			}
		}
	}
}
