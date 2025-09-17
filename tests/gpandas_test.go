package gpandas_test

import (
	"gpandas"
	"os"
	"path/filepath"
	"reflect"
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
		name         string
		csvContent   string
		expectError  bool
		expectedRows int
	}{
		{
			name: "valid csv",
			csvContent: `name,age,city
John,30,New York
Alice,25,London
Bob,35,Paris`,
			expectError:  false,
			expectedRows: 3,
		},
		{
			name: "empty csv",
			csvContent: `name,age,city
`,
			expectError:  false,
			expectedRows: 0,
		},
		{
			name: "inconsistent columns",
			csvContent: `name,age,city
John,30
Alice,25,London,Extra
Bob,35,Paris`,
			expectError:  false,
			expectedRows: 1,
		},
		{
			name:         "empty file",
			csvContent:   "",
			expectError:  true,
			expectedRows: 0,
		},
		{
			name:         "only headers",
			csvContent:   `name,age,city`,
			expectError:  false,
			expectedRows: 0,
		},
		{
			name: "valid csv with quoted fields",
			csvContent: `name,description,city
John,"Software Engineer, Senior",New York
Alice,"Product Manager, Lead",London
Bob,"Data Scientist, ML",Paris`,
			expectError:  false,
			expectedRows: 3,
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
				if df == nil {
					t.Error("expected non-nil DataFrame")
					return
				}
				if len(df.ColumnOrder) == 0 {
					t.Error("expected non-empty ColumnOrder")
				}
				if len(df.Columns) != len(df.ColumnOrder) {
					t.Errorf("columns map size and ColumnOrder mismatch: %d vs %d", len(df.Columns), len(df.ColumnOrder))
				}
				// compute row count as min length across columns
				rows := 0
				if len(df.ColumnOrder) > 0 {
					rows = df.Columns[df.ColumnOrder[0]].Len()
					for _, c := range df.ColumnOrder[1:] {
						if s := df.Columns[c]; s.Len() < rows {
							rows = s.Len()
						}
					}
				}
				if rows != tt.expectedRows {
					t.Errorf("unexpected row count: expected %d, got %d", tt.expectedRows, rows)
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

	// Verify all series have dtype string and contain string values
	for _, colName := range df.ColumnOrder {
		series := df.Columns[colName]
		if series.DType() != reflect.TypeOf("") {
			t.Errorf("expected dtype string for column %s, got %v", colName, series.DType())
		}
		for i := 0; i < series.Len(); i++ {
			val, _ := series.At(i)
			if _, ok := val.(string); !ok {
				t.Errorf("expected string value at index %d in column %s, got %T", i, colName, val)
			}
		}
	}
}
