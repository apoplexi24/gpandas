package dataframe_test

import (
	"os"
	"testing"

	"github.com/apoplexi24/gpandas/dataframe"
	"github.com/apoplexi24/gpandas/utils/collection"
)

// Helper function to compare slices
func sliceEqual(a, b []any) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Helper function to compare string slices
func strSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestDataFrameRename tests the DataFrame.Rename method which allows renaming columns in a DataFrame.
//
// The test covers several scenarios:
//
// 1. Successful rename:
//   - Tests renaming multiple existing columns ("A" to "X" and "B" to "Y")
//   - Verifies no error is returned when renaming valid columns
//
// 2. Renaming non-existent column:
//   - Attempts to rename column "D" which doesn't exist
//   - Verifies an error is returned for invalid column name
//
// 3. Nil DataFrame:
//   - Tests behavior when DataFrame is nil
//   - Verifies appropriate error handling for nil DataFrame
//
// 4. Empty columns map:
//   - Tests behavior when an empty rename map is provided
//   - Verifies error is returned for empty rename request
//
// Each test case validates:
//   - Error behavior matches expectations (error/no error)
//   - Error conditions are properly handled
//   - Method behaves correctly for valid and invalid inputs
func TestDataFrameRename(t *testing.T) {
	tests := []struct {
		name        string
		df          *dataframe.DataFrame
		columns     map[string]string
		expectError bool
	}{
		{
			name:        "successful rename",
			df:          &dataframe.DataFrame{Columns: map[string]collection.Series{"A": mustSeries(1, 4), "B": mustSeries(2, 5), "C": mustSeries(3, 6)}, ColumnOrder: []string{"A", "B", "C"}},
			columns:     map[string]string{"A": "X", "B": "Y"},
			expectError: false,
		},
		{
			name:        "rename non-existent column",
			df:          &dataframe.DataFrame{Columns: map[string]collection.Series{"A": mustSeries(1, 4), "B": mustSeries(2, 5), "C": mustSeries(3, 6)}, ColumnOrder: []string{"A", "B", "C"}},
			columns:     map[string]string{"D": "X"},
			expectError: true,
		},
		{
			name:        "nil dataframe",
			df:          nil,
			columns:     map[string]string{"A": "X"},
			expectError: true,
		},
		{
			name:        "empty columns map",
			df:          &dataframe.DataFrame{Columns: map[string]collection.Series{"A": mustSeries(1, 4), "B": mustSeries(2, 5), "C": mustSeries(3, 6)}, ColumnOrder: []string{"A", "B", "C"}},
			columns:     map[string]string{},
			expectError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.df.Rename(test.columns)
			if (err != nil) != test.expectError {
				t.Errorf("expected error: %v, got: %v", test.expectError, err)
			}
		})
	}
}

// TestDataFrameString tests the String() method of the DataFrame struct, which converts
// a DataFrame into a formatted string representation.
//
// The test suite covers three main scenarios:
//
// 1. Basic DataFrame ("basic dataframe"):
//   - Tests a simple numeric DataFrame with:
//   - 3 columns (A, B, C)
//   - 2 rows of integer data
//   - Verifies correct table formatting with headers, borders, and row count
//
// 2. Empty DataFrame ("empty dataframe"):
//   - Tests DataFrame with:
//   - 2 columns (A, B)
//   - No data rows
//   - Verifies proper handling of empty data while maintaining structure
//   - Confirms correct row count display ([0 rows x 2 columns])
//
// 3. Mixed Data Types ("mixed data types"):
//   - Tests DataFrame with different data types:
//   - String column (Name)
//   - Integer column (Age)
//   - Boolean column (Active)
//   - Verifies proper string conversion of different data types
//   - Confirms alignment and spacing with varying content lengths
//
// Test Structure:
//   - Uses table-driven tests for multiple scenarios
//   - Each test case includes:
//   - name: descriptive test case name
//   - df: input DataFrame
//   - expected: exact expected string output
//
// Verification:
//   - Compares exact string output including:
//   - Table borders and separators
//   - Column headers
//   - Data alignment
//   - Row count summary
//   - Uses exact string matching to ensure precise formatting
//
// Example test case:
//
//	{
//	    name: "basic dataframe",
//	    df: &dataframe.DataFrame{
//	        Columns: []string{"A", "B", "C"},
//	        Data:    [][]any{{1, 2, 3}, {4, 5, 6}},
//	    },
//	    expected: `+---+---+---+
//	               | A | B | C |
//	               +---+---+---+
//	               | 1 | 2 | 3 |
//	               | 4 | 5 | 6 |
//	               +---+---+---+
//	               [2 rows x 3 columns]
//	               `,
//	}
func TestDataFrameString(t *testing.T) {
	tests := []struct {
		name     string
		df       *dataframe.DataFrame
		expected string
	}{
		{
			name: "basic dataframe",
			df:   &dataframe.DataFrame{Columns: map[string]collection.Series{"A": mustSeries(1, 4), "B": mustSeries(2, 5), "C": mustSeries(3, 6)}, ColumnOrder: []string{"A", "B", "C"}},
			expected: `+---+---+---+
| A | B | C |
+---+---+---+
| 1 | 2 | 3 |
| 4 | 5 | 6 |
+---+---+---+
[2 rows x 3 columns]
`,
		},
		{
			name: "empty dataframe",
			df:   &dataframe.DataFrame{Columns: map[string]collection.Series{"A": mustSeries(), "B": mustSeries()}, ColumnOrder: []string{"A", "B"}},
			expected: `+---+---+
| A | B |
+---+---+
+---+---+
[0 rows x 2 columns]
`,
		},
		{
			name: "mixed data types",
			df:   &dataframe.DataFrame{Columns: map[string]collection.Series{"Name": mustSeries("John", "Jane"), "Age": mustSeries(30, 25), "Active": mustSeries(true, false)}, ColumnOrder: []string{"Name", "Age", "Active"}},
			expected: `+------+-----+--------+
| Name | Age | Active |
+------+-----+--------+
| John | 30  | true   |
| Jane | 25  | false  |
+------+-----+--------+
[2 rows x 3 columns]
`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := test.df.String()
			if result != test.expected {
				t.Errorf("expected:\n%s\ngot:\n%s", test.expected, result)
			}
		})
	}
}

// TestDataFrameMerge tests the DataFrame.Merge method which combines two DataFrames
// based on a common column and specified merge strategy.
//
// The test suite covers seven main scenarios:
//
// 1. Inner Merge ("inner merge - basic case"):
//   - Tests basic inner join functionality
//   - Input:
//   - df1: ID-Name pairs (3 rows)
//   - df2: ID-Age pairs (3 rows)
//   - Verifies only matching rows are kept (2 rows)
//   - Checks correct column combination
//
// 2. Left Merge ("left merge - keep all left rows"):
//   - Tests left join functionality
//   - Input:
//   - df1: ID-Name pairs (3 rows)
//   - df2: ID-Age pairs (2 rows)
//   - Verifies all left rows are kept
//   - Confirms nil values for non-matching right rows
//
// 3. Right Merge ("right merge - keep all right rows"):
//   - Tests right join functionality
//   - Input:
//   - df1: ID-Name pairs (2 rows)
//   - df2: ID-Age pairs (3 rows)
//   - Verifies all right rows are kept
//   - Confirms nil values for non-matching left rows
//
// 4. Full Merge ("full merge - keep all rows"):
//
//   - Tests full outer join functionality
//
//   - Input:
//
//   - df1: ID-Name pairs (3 rows)
//
//   - df2: ID-Age pairs (3 rows)
//
//   - Verifies all rows from both DataFrames are kept
//
//   - Confirms nil values for non-matching rows
//
//     5. Error Cases:
//     a. Nil DataFrame ("nil dataframe error"):
//
//   - Tests behavior with nil DataFrame input
//
//   - Verifies appropriate error handling
//
//     b. Missing Column ("column not found error"):
//
//   - Tests behavior when merge column doesn't exist
//
//   - Verifies appropriate error detection
//
//     c. Invalid Merge Type ("invalid merge type error"):
//
//   - Tests behavior with invalid merge strategy
//
//   - Verifies appropriate error handling
//
// Test Structure:
//   - Uses table-driven tests for multiple scenarios
//   - Each test case includes:
//   - name: descriptive test case name
//   - df1: first input DataFrame
//   - df2: second input DataFrame
//   - on: column to merge on
//   - how: merge strategy
//   - expected: expected result DataFrame
//   - expectError: whether an error is expected
//
// Verification Steps:
// 1. Error handling:
//   - Checks if errors occur as expected
//   - Verifies error cases return appropriate errors
//
// 2. Success cases:
//   - Verifies column names match expected output
//   - Checks number of rows matches expected output
//   - Validates each row's data matches expected values
//
// Example test case:
//
//	{
//	    name: "inner merge - basic case",
//	    df1: &dataframe.DataFrame{
//	        Columns: []string{"ID", "Name"},
//	        Data:    [][]any{{1, "Alice"}, {2, "Bob"}, {3, "Charlie"}},
//	    },
//	    df2: &dataframe.DataFrame{
//	        Columns: []string{"ID", "Age"},
//	        Data:    [][]any{{1, 25}, {2, 30}, {4, 35}},
//	    },
//	    on:  "ID",
//	    how: dataframe.InnerMerge,
//	    expected: &dataframe.DataFrame{
//	        Columns: []string{"ID", "Name", "Age"},
//	        Data:    [][]any{{1, "Alice", 25}, {2, "Bob", 30}},
//	    },
//	    expectError: false,
//	}
func TestDataFrameMerge(t *testing.T) {
	tests := []struct {
		name        string
		df1         *dataframe.DataFrame
		df2         *dataframe.DataFrame
		on          string
		how         dataframe.MergeHow
		expected    *dataframe.DataFrame
		expectError bool
	}{
		{
			name:        "inner merge - basic case",
			df1:         &dataframe.DataFrame{Columns: map[string]collection.Series{"ID": mustSeries(1, 2, 3), "Name": mustSeries("Alice", "Bob", "Charlie")}, ColumnOrder: []string{"ID", "Name"}},
			df2:         &dataframe.DataFrame{Columns: map[string]collection.Series{"ID": mustSeries(1, 2, 4), "Age": mustSeries(25, 30, 35)}, ColumnOrder: []string{"ID", "Age"}},
			on:          "ID",
			how:         dataframe.InnerMerge,
			expected:    &dataframe.DataFrame{Columns: map[string]collection.Series{"ID": mustSeries(1, 2), "Name": mustSeries("Alice", "Bob"), "Age": mustSeries(25, 30)}, ColumnOrder: []string{"ID", "Name", "Age"}},
			expectError: false,
		},
		{
			name:        "left merge - keep all left rows",
			df1:         &dataframe.DataFrame{Columns: map[string]collection.Series{"ID": mustSeries(1, 2, 3), "Name": mustSeries("Alice", "Bob", "Charlie")}, ColumnOrder: []string{"ID", "Name"}},
			df2:         &dataframe.DataFrame{Columns: map[string]collection.Series{"ID": mustSeries(1, 2), "Age": mustSeries(25, 30)}, ColumnOrder: []string{"ID", "Age"}},
			on:          "ID",
			how:         dataframe.LeftMerge,
			expected:    &dataframe.DataFrame{Columns: map[string]collection.Series{"ID": mustSeries(1, 2, 3), "Name": mustSeries("Alice", "Bob", "Charlie"), "Age": mustSeries(25, 30, nil)}, ColumnOrder: []string{"ID", "Name", "Age"}},
			expectError: false,
		},
		{
			name:        "right merge - keep all right rows",
			df1:         &dataframe.DataFrame{Columns: map[string]collection.Series{"ID": mustSeries(1, 2), "Name": mustSeries("Alice", "Bob")}, ColumnOrder: []string{"ID", "Name"}},
			df2:         &dataframe.DataFrame{Columns: map[string]collection.Series{"ID": mustSeries(1, 2, 3), "Age": mustSeries(25, 30, 35)}, ColumnOrder: []string{"ID", "Age"}},
			on:          "ID",
			how:         dataframe.RightMerge,
			expected:    &dataframe.DataFrame{Columns: map[string]collection.Series{"ID": mustSeries(1, 2, 3), "Name": mustSeries("Alice", "Bob", nil), "Age": mustSeries(25, 30, 35)}, ColumnOrder: []string{"ID", "Name", "Age"}},
			expectError: false,
		},
		{
			name:        "full merge - keep all rows",
			df1:         &dataframe.DataFrame{Columns: map[string]collection.Series{"ID": mustSeries(1, 2, 3), "Name": mustSeries("Alice", "Bob", "Charlie")}, ColumnOrder: []string{"ID", "Name"}},
			df2:         &dataframe.DataFrame{Columns: map[string]collection.Series{"ID": mustSeries(1, 2, 4), "Age": mustSeries(25, 30, 35)}, ColumnOrder: []string{"ID", "Age"}},
			on:          "ID",
			how:         dataframe.FullMerge,
			expected:    &dataframe.DataFrame{Columns: map[string]collection.Series{"ID": mustSeries(1, 2, 3, 4), "Name": mustSeries("Alice", "Bob", "Charlie", nil), "Age": mustSeries(25, 30, nil, 35)}, ColumnOrder: []string{"ID", "Name", "Age"}},
			expectError: false,
		},
		{
			name:        "nil dataframe error",
			df1:         nil,
			df2:         &dataframe.DataFrame{},
			on:          "ID",
			how:         dataframe.InnerMerge,
			expectError: true,
		},
		{
			name:        "column not found error",
			df1:         &dataframe.DataFrame{Columns: map[string]collection.Series{"ID": mustSeries(1), "Name": mustSeries("Alice")}, ColumnOrder: []string{"ID", "Name"}},
			df2:         &dataframe.DataFrame{Columns: map[string]collection.Series{"UserID": mustSeries(1), "Age": mustSeries(25)}, ColumnOrder: []string{"UserID", "Age"}},
			on:          "ID",
			how:         dataframe.InnerMerge,
			expectError: true,
		},
		{
			name:        "invalid merge type error",
			df1:         &dataframe.DataFrame{Columns: map[string]collection.Series{"ID": mustSeries(1), "Name": mustSeries("Alice")}, ColumnOrder: []string{"ID", "Name"}},
			df2:         &dataframe.DataFrame{Columns: map[string]collection.Series{"ID": mustSeries(1), "Age": mustSeries(25)}, ColumnOrder: []string{"ID", "Age"}},
			on:          "ID",
			how:         "invalid",
			expectError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := test.df1.Merge(test.df2, test.on, test.how)

			// Check error cases
			if test.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			// Check columns match
			if !strSliceEqual(result.ColumnOrder, test.expected.ColumnOrder) {
				t.Errorf("columns mismatch\nexpected: %v\ngot: %v", test.expected.ColumnOrder, result.ColumnOrder)
			}

			// Check data matches by rows
			rows := 0
			if len(result.ColumnOrder) > 0 {
				rows = result.Columns[result.ColumnOrder[0]].Len()
			}
			expRows := 0
			if len(test.expected.ColumnOrder) > 0 {
				expRows = test.expected.Columns[test.expected.ColumnOrder[0]].Len()
			}
			if rows != expRows {
				t.Fatalf("row count mismatch expected %d got %d", expRows, rows)
			}
			for r := 0; r < rows; r++ {
				got := make([]any, 0, len(result.ColumnOrder))
				exp := make([]any, 0, len(test.expected.ColumnOrder))
				for _, c := range result.ColumnOrder {
					v, _ := result.Columns[c].At(r)
					got = append(got, v)
				}
				for _, c := range test.expected.ColumnOrder {
					v, _ := test.expected.Columns[c].At(r)
					exp = append(exp, v)
				}
				if !sliceEqual(got, exp) {
					t.Errorf("row %d mismatch\nexpected: %v\ngot: %v", r, exp, got)
				}
			}
		})
	}
}

// TestDataFrameToCSV tests the DataFrame.ToCSV method which converts a DataFrame to CSV format
// or writes it to a file.
//
// The test suite covers the following scenarios:
//
// 1. Basic CSV String Output:
//   - Tests conversion of simple DataFrame to CSV string
//   - Verifies correct comma separation and line endings
//
// 2. Custom Separator:
//   - Tests CSV generation with custom separator (semicolon)
//   - Verifies correct formatting with non-default separator
//
// 3. File Output:
//   - Tests writing CSV to a temporary file
//   - Verifies file contents match expected CSV format
//
// 4. Mixed Data Types:
//   - Tests CSV conversion with various data types (string, int, bool)
//   - Verifies correct string representation of different types
//
// 5. Error Cases:
//   - Tests nil DataFrame handling
//   - Tests invalid file path handling
func TestDataFrameToCSV(t *testing.T) {
	tests := []struct {
		name        string
		df          *dataframe.DataFrame
		filepath    string
		separator   string
		expected    string
		expectError bool
	}{
		{
			name:        "basic csv string output",
			df:          &dataframe.DataFrame{Columns: map[string]collection.Series{"A": mustSeries(1, 4), "B": mustSeries(2, 5), "C": mustSeries(3, 6)}, ColumnOrder: []string{"A", "B", "C"}},
			filepath:    "",
			expected:    "A,B,C\n1,2,3\n4,5,6\n",
			expectError: false,
		},
		{
			name:        "custom separator",
			df:          &dataframe.DataFrame{Columns: map[string]collection.Series{"A": mustSeries(1, 4), "B": mustSeries(2, 5), "C": mustSeries(3, 6)}, ColumnOrder: []string{"A", "B", "C"}},
			filepath:    "",
			separator:   ";",
			expected:    "A;B;C\n1;2;3\n4;5;6\n",
			expectError: false,
		},
		{
			name:        "mixed data types",
			df:          &dataframe.DataFrame{Columns: map[string]collection.Series{"Name": mustSeries("John", "Jane"), "Age": mustSeries(30, 25), "Active": mustSeries(true, false)}, ColumnOrder: []string{"Name", "Age", "Active"}},
			filepath:    "",
			expected:    "Name,Age,Active\nJohn,30,true\nJane,25,false\n",
			expectError: false,
		},
		{
			name:        "nil dataframe",
			df:          nil,
			filepath:    "",
			expectError: true,
		},
		{
			name:        "invalid file path",
			df:          &dataframe.DataFrame{Columns: map[string]collection.Series{"A": mustSeries(1), "B": mustSeries(2)}, ColumnOrder: []string{"A", "B"}},
			filepath:    "/nonexistent/directory/file.csv",
			expectError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var result string
			var err error

			if test.separator != "" {
				result, err = test.df.ToCSV(test.filepath, test.separator)
			} else {
				result, err = test.df.ToCSV(test.filepath)
			}

			// Check error cases
			if test.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			// For file output tests, read the file and compare contents
			if test.filepath != "" {
				content, err := os.ReadFile(test.filepath)
				if err != nil {
					t.Errorf("failed to read output file: %v", err)
					return
				}
				result = string(content)
				// Clean up the test file
				os.Remove(test.filepath)
			}

			// Compare results
			if result != test.expected {
				t.Errorf("CSV output mismatch\nexpected:\n%s\ngot:\n%s", test.expected, result)
			}
		})
	}

	// Test successful file writing with temporary file
	t.Run("successful file writing", func(t *testing.T) {
		df := &dataframe.DataFrame{Columns: map[string]collection.Series{"A": mustSeries(1, 3), "B": mustSeries(2, 4)}, ColumnOrder: []string{"A", "B"}}
		tempFile := t.TempDir() + "/test.csv"
		expected := "A,B\n1,2\n3,4\n"

		result, err := df.ToCSV(tempFile)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			return
		}

		// Result should be empty string when writing to file
		if result != "" {
			t.Errorf("expected empty string result when writing to file, got: %s", result)
		}

		// Read the file and verify contents
		content, err := os.ReadFile(tempFile)
		if err != nil {
			t.Errorf("failed to read output file: %v", err)
			return
		}

		if string(content) != expected {
			t.Errorf("file content mismatch\nexpected:\n%s\ngot:\n%s", expected, string(content))
		}
	})
}

// TestDataFrameNullHandling tests the null handling with boolean mask approach
func TestDataFrameNullHandling(t *testing.T) {
	t.Run("null values in series", func(t *testing.T) {
		// Create a series with null values
		series := mustSeries(1, nil, 3)

		// Check null detection
		if !series.IsNull(1) {
			t.Error("expected index 1 to be null")
		}
		if series.IsNull(0) {
			t.Error("expected index 0 to not be null")
		}
		if series.IsNull(2) {
			t.Error("expected index 2 to not be null")
		}

		// Check NullCount
		if series.NullCount() != 1 {
			t.Errorf("expected null count 1, got %d", series.NullCount())
		}
	})

	t.Run("null in dataframe display", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"A": mustSeries(1, nil, 3),
				"B": mustSeries("x", "y", nil),
			},
			ColumnOrder: []string{"A", "B"},
		}

		result := df.String()
		// Verify "null" appears in the output
		if !containsString(result, "null") {
			t.Errorf("expected 'null' in output, got:\n%s", result)
		}
	})
}

func TestDataFrameHead(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"A": mustSeries(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
			"B": mustSeries(10, 20, 30, 40, 50, 60, 70, 80, 90, 100),
		},
		ColumnOrder: []string{"A", "B"},
	}

	t.Run("default head (5 rows)", func(t *testing.T) {
		head := df.Head()
		if head.Columns["A"].Len() != 5 {
			t.Errorf("expected 5 rows, got %d", head.Columns["A"].Len())
		}
		val, _ := head.Columns["A"].At(4)
		if val != float64(5) && val != int64(5) { // Handle type inference
			t.Errorf("expected 5 at index 4, got %v", val)
		}
	})

	t.Run("head with n=3", func(t *testing.T) {
		head := df.Head(3)
		if head.Columns["A"].Len() != 3 {
			t.Errorf("expected 3 rows, got %d", head.Columns["A"].Len())
		}
		val, _ := head.Columns["A"].At(2)
		if val != float64(3) && val != int64(3) {
			t.Errorf("expected 3 at index 2, got %v", val)
		}
	})

	t.Run("head with n > len", func(t *testing.T) {
		head := df.Head(20)
		if head.Columns["A"].Len() != 10 {
			t.Errorf("expected 10 rows, got %d", head.Columns["A"].Len())
		}
	})
}

func TestDataFrameTail(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"A": mustSeries(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
			"B": mustSeries(10, 20, 30, 40, 50, 60, 70, 80, 90, 100),
		},
		ColumnOrder: []string{"A", "B"},
	}

	t.Run("default tail (5 rows)", func(t *testing.T) {
		tail := df.Tail()
		if tail.Columns["A"].Len() != 5 {
			t.Errorf("expected 5 rows, got %d", tail.Columns["A"].Len())
		}
		val, _ := tail.Columns["A"].At(0)
		if val != float64(6) && val != int64(6) {
			t.Errorf("expected 6 at index 0, got %v", val)
		}
	})

	t.Run("tail with n=3", func(t *testing.T) {
		tail := df.Tail(3)
		if tail.Columns["A"].Len() != 3 {
			t.Errorf("expected 3 rows, got %d", tail.Columns["A"].Len())
		}
		val, _ := tail.Columns["A"].At(0)
		if val != float64(8) && val != int64(8) {
			t.Errorf("expected 8 at index 0, got %v", val)
		}
	})

	t.Run("tail with n > len", func(t *testing.T) {
		tail := df.Tail(20)
		if tail.Columns["A"].Len() != 10 {
			t.Errorf("expected 10 rows, got %d", tail.Columns["A"].Len())
		}
	})
}

func containsString(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsString(s[1:], substr) || s[:len(substr)] == substr)
}

// helpers
func mustSeries(vals ...any) collection.Series {
	s, err := collection.NewSeriesWithData(nil, vals)
	if err != nil {
		panic(err)
	}
	return s
}
