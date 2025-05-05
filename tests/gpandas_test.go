package gpandas_test

import (
	"gpandas"
	"gpandas/dataframe"
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
		name          string
		csvContent    string
		expectError   bool
		expectEmptyDF bool
		options       []gpandas.Option
	}{
		{
			name: "valid csv",
			csvContent: `name,age,city
John,30,New York
Alice,25,London
Bob,35,Paris`,
			expectError:   false,
			expectEmptyDF: false,
			options:       nil,
		},
		{
			name: "empty csv",
			csvContent: `name,age,city
`,
			expectError:   false,
			expectEmptyDF: false,
			options:       nil,
		},
		{
			name: "inconsistent columns",
			csvContent: `name,age,city
John,30
Alice,25,London,Extra
Bob,35,Paris`,
			expectError:   true,
			expectEmptyDF: false,
			options:       nil,
		},
		{
			name:          "empty file",
			csvContent:    "",
			expectError:   true,
			expectEmptyDF: false,
			options:       nil,
		},
		{
			name:          "only headers",
			csvContent:    `name,age,city`,
			expectError:   false,
			expectEmptyDF: false,
			options:       nil,
		},
		{
			name: "valid csv with quoted fields",
			csvContent: `name,description,city
John,"Software Engineer, Senior",New York
Alice,"Product Manager, Lead",London
Bob,"Data Scientist, ML",Paris`,
			expectError:   false,
			expectEmptyDF: false,
			options:       nil,
		},
		{
			name: "csv with semicolon separator",
			csvContent: `name;age;city
John;30;New York
Alice;25;London
Bob;35;Paris`,
			expectError:   false,
			expectEmptyDF: false,
			options:       []gpandas.Option{gpandas.WithCSVSeparator(';')},
		},
		{
			name: "csv with automatic type detection",
			csvContent: `name,age,active
John,30,true
Alice,25,false
Bob,35,true`,
			expectError:   false,
			expectEmptyDF: false,
			options:       []gpandas.Option{gpandas.WithCSVAutoType(true)},
		},
		{
			name: "csv with custom worker count",
			csvContent: `name,age,city
John,30,New York
Alice,25,London
Bob,35,Paris`,
			expectError:   false,
			expectEmptyDF: false,
			options:       []gpandas.Option{gpandas.WithWorkerCount(2)},
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

			// Test Read_csv with options
			pd := gpandas.GoPandas{}
			var df *dataframe.DataFrame
			var readErr error

			if tt.options != nil {
				df, readErr = pd.Read_csv(testFile, tt.options...)
			} else {
				df, readErr = pd.Read_csv(testFile)
			}

			// Check error expectations
			if tt.expectError && readErr == nil {
				t.Error("expected error but got none")
			}
			if !tt.expectError && readErr != nil {
				t.Errorf("unexpected error: %v", readErr)
			}

			// Additional checks for successful cases
			if !tt.expectError && readErr == nil {
				// Check if DataFrame is not nil
				if df == nil {
					t.Error("expected non-nil DataFrame")
					return
				}

				// Check if columns are present
				if len(df.Columns) == 0 {
					t.Error("expected non-empty columns")
				}

				// Only check for non-empty data if we're not expecting an empty DataFrame
				if !tt.expectEmptyDF && df.Rows() == 0 {
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

				// For auto type test, check that numeric columns are properly typed
				if tt.name == "csv with automatic type detection" {
					ageSeries, ok := df.Series["age"]
					if !ok {
						t.Error("age column not found")
					} else {
						_, isIntSeries := ageSeries.(*dataframe.IntSeries)
						if !isIntSeries {
							t.Errorf("expected IntSeries for age column, got %T", ageSeries)
						}
					}

					activeSeries, ok := df.Series["active"]
					if !ok {
						t.Error("active column not found")
					} else {
						_, isBoolSeries := activeSeries.(*dataframe.BoolSeries)
						if !isBoolSeries {
							t.Errorf("expected BoolSeries for active column, got %T", activeSeries)
						}
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

	// Test without auto type detection (default)
	t.Run("without auto type detection", func(t *testing.T) {
		pd := gpandas.GoPandas{}
		df, err := pd.Read_csv(testFile)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Verify all columns are StringSeries
		for _, colName := range df.Columns {
			series, ok := df.Series[colName]
			if !ok {
				t.Errorf("column %s not found in Series map", colName)
				continue
			}

			_, isStringSeries := series.(*dataframe.StringSeries)
			if !isStringSeries {
				t.Errorf("expected StringSeries for column %s, got %T", colName, series)
			}
		}
	})

	// Test with auto type detection
	t.Run("with auto type detection", func(t *testing.T) {
		pd := gpandas.GoPandas{}
		df, err := pd.Read_csv(testFile, gpandas.WithCSVAutoType(true))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Verify columns have appropriate types
		typeMap := map[string]reflect.Type{
			"name":   reflect.TypeOf(""),
			"age":    reflect.TypeOf(int64(0)),
			"active": reflect.TypeOf(bool(false)),
			"score":  reflect.TypeOf(float64(0)),
		}

		for colName, expectedType := range typeMap {
			series, ok := df.Series[colName]
			if !ok {
				t.Errorf("column %s not found in Series map", colName)
				continue
			}

			if series.GetType() != expectedType {
				t.Errorf("wrong type for column %s: expected %v, got %v",
					colName, expectedType, series.GetType())
			}
		}
	})
}
