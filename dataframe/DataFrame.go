package dataframe

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/apoplexi24/gpandas/utils/collection"

	"github.com/olekukonko/tablewriter"
)

type GoPandas struct{}

// FloatColumn represents a column slice of float64 values.
type FloatCol []float64

// StringColumn represents a column slice of string values.
type StringCol []string

// IntColumn represents a column slice of int64 values.
type IntCol []int64

// BoolColumn represents a column slice of bool values.
type BoolCol []bool

// Column represents a column slice of any type.
type Column []any

// TypeColumn represents a column slice of a comparable type T.
type TypeColumn[T comparable] []T

func GetMapKeys[K comparable, V any](input_map map[K]V) (collection.Set[K], error) {
	keys, err := collection.NewSet[K]()
	if err != nil {
		return nil, err
	}
	for k := range input_map {
		keys.Add(k)
	}
	return keys, nil
}

type DataFrame struct {
	sync.RWMutex
	Columns     map[string]collection.Series
	ColumnOrder []string
	Index       []string // Row labels, defaults to string representations of row numbers
}

// Rename changes the names of specified columns in the DataFrame.
//
// The method allows renaming multiple columns at once by providing a map where:
//   - Keys are the current/original column names
//   - Values are the new column names to replace them with
//
// The operation is thread-safe as it uses mutex locking to prevent concurrent modifications.
//
// Parameters:
//   - columns: map[string]string where keys are original column names and values are new names
//
// Returns:
//   - error: nil if successful, otherwise an error describing what went wrong
//
// Possible errors:
//   - If the columns map is empty
//   - If the DataFrame is nil
//   - If any specified original column name doesn't exist in the DataFrame
//   - If there are any issues with internal set operations
//
// Example:
//
//	df := &DataFrame{
//	    Columns: []string{"A", "B", "C"},
//	    Data:    [][]any{{1, 2, 3}, {4, 5, 6}},
//	}
//
//	// Rename columns "A" to "X" and "B" to "Y"
//	err := df.Rename(map[string]string{
//	    "A": "X",
//	    "B": "Y",
//	})
//
//	// Result:
//	// Columns will be ["X", "Y", "C"]
//
// Thread Safety:
//
// The method uses sync.Mutex to ensure thread-safe operation when modifying column names.
// The lock is automatically released using defer when the function returns.
//
// Note:
//   - All specified original column names must exist in the DataFrame
//   - The operation modifies the DataFrame in place
//   - Column order remains unchanged
func (df *DataFrame) Rename(columns map[string]string) error {
	if df == nil {
		return errors.New("DataFrame is nil")
	}
	if len(columns) == 0 {
		return errors.New("'columns' slice is empty. Slice of Maps to declare columns to rename is required")
	}

	keys, err := GetMapKeys[string, string](columns)
	if err != nil {
		return err
	}

	// locking df and unlocking if facing error or after finished processing
	df.Lock()
	defer df.Unlock()

	dfcols, err2 := collection.ToSet(df.ColumnOrder)
	if err2 != nil {
		return err2
	}

	keys_dfcols_set_intersect, err3 := keys.Intersect(dfcols)
	if err3 != nil {
		return err3
	}

	is_equal_cols, false_val := keys.Compare(keys_dfcols_set_intersect)
	if !is_equal_cols && false_val != nil {
		return errors.New("the column '" + false_val.(string) + "' is not present in DataFrame. Specify correct values as keys in columns map")
	} else if !is_equal_cols && false_val == nil {
		return errors.New("the columns specified in 'columns' parameter is not present in the the DataFrame")
	}

	// all conditions met till this point
	for original_column_name, new_column_name := range columns {
		// move series in map
		if series, ok := df.Columns[original_column_name]; ok {
			delete(df.Columns, original_column_name)
			df.Columns[new_column_name] = series
		}
		// update order slice
		for i := range df.ColumnOrder {
			if df.ColumnOrder[i] == original_column_name {
				df.ColumnOrder[i] = new_column_name
			}
		}
	}
	return nil
}

// String returns a string representation of the DataFrame in a formatted table.
//
// The method creates a visually appealing ASCII table representation of the DataFrame
// with the following features:
//   - Column headers are displayed in the first row
//   - Data is aligned to the left within columns
//   - Table borders and separators use ASCII characters
//   - Each cell's content is automatically converted to string representation
//   - Null values are displayed as "null"
//   - A summary line showing dimensions ([rows x columns]) is appended
//
// The table format follows this pattern:
//
//	+-----+-----+-----+
//	| Col1| Col2| Col3|
//	+-----+-----+-----+
//	| val1| val2| null|
//	| val4| val5| val6|
//	+-----+-----+-----+
//	[2 rows x 3 columns]
//
// Parameters:
//   - None (receiver method on DataFrame)
//
// Returns:
//   - string: The formatted table representation of the DataFrame
//
// Example:
//
//	df := &DataFrame{
//	    Columns: []string{"A", "B"},
//	    Data:    [][]any{{1, 2}, {3, 4}},
//	}
//	fmt.Println(df.String())
//
// Note:
//   - All values are converted to strings using fmt.Sprintf("%v", val)
//   - Null values are displayed as "null"
//   - The table is rendered using the github.com/olekukonko/tablewriter package
func (df *DataFrame) String() string {
	if df == nil {
		return "DataFrame is nil"
	}

	var buf bytes.Buffer
	table := tablewriter.NewWriter(&buf)

	// Set table properties
	table.SetAutoWrapText(false)
	table.SetAutoFormatHeaders(false)
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetCenterSeparator("+")
	table.SetColumnSeparator("|")
	table.SetRowSeparator("-")
	table.SetHeaderLine(true)
	table.SetBorder(true)

	// Set headers using the DataFrame's ColumnOrder
	table.SetHeader(df.ColumnOrder)

	// Determine number of rows using the first column's length (min length across columns)
	rowCount := 0
	if len(df.ColumnOrder) > 0 {
		first := df.Columns[df.ColumnOrder[0]]
		rowCount = first.Len()
		for _, colName := range df.ColumnOrder[1:] {
			if s := df.Columns[colName]; s != nil {
				if s.Len() < rowCount {
					rowCount = s.Len()
				}
			}
		}
	}
	displayRows := rowCount
	if rowCount > 10 {
		displayRows = 10
	}

	// Append only the first displayRows rows to the table
	for i := 0; i < displayRows; i++ {
		stringRow := make([]string, len(df.ColumnOrder))
		for j, colName := range df.ColumnOrder {
			series := df.Columns[colName]
			if series.IsNull(i) {
				stringRow[j] = "null"
			} else if val, err := series.At(i); err == nil {
				stringRow[j] = fmt.Sprintf("%v", val)
			} else {
				stringRow[j] = ""
			}
		}
		table.Append(stringRow)
	}

	// Add row count information.
	// If there are more than 10 rows, mention that only the first 10 are displayed.
	shape := fmt.Sprintf("[%d rows x %d columns]", rowCount, len(df.ColumnOrder))
	if rowCount > 10 {
		shape = fmt.Sprintf("Showing first 10 rows of %d rows x %d columns", rowCount, len(df.ColumnOrder))
	}

	// Render the table and return the string representation
	table.Render()
	return buf.String() + shape + "\n"
}

// ToCSV converts the DataFrame to a CSV string representation or writes it to a file.
//
// Parameters:
//   - filepath: file path to write the CSV to (empty string to return as string)
//   - separator: optional separator for the CSV (defaults to comma)
//
// Returns:
//   - string: CSV representation of the DataFrame if filepath is empty
//   - error: nil if successful, otherwise an error describing what went wrong
//
// Note: If filepath is provided, the method returns ("", nil) on success
// Null values are represented as empty strings in the CSV output.
//
// Example:
//
//	// Get CSV as string with default comma separator
//	csv, err := df.ToCSV("")
//
//	// Get CSV as string with custom separator
//	csv, err := df.ToCSV("", ";")
//
//	// Write to file with default comma separator
//	_, err := df.ToCSV("path/to/output.csv")
//
//	// Write to file with custom separator
//	_, err := df.ToCSV("path/to/output.csv", ";")
func (df *DataFrame) ToCSV(filepath string, separator ...string) (string, error) {
	if df == nil {
		return "", errors.New("DataFrame is nil")
	}

	// Default separator is comma
	sep := ","
	if len(separator) > 0 {
		sep = separator[0]
	}

	var buf bytes.Buffer

	// Write headers
	for i, colName := range df.ColumnOrder {
		if i > 0 {
			buf.WriteString(sep)
		}
		buf.WriteString(colName)
	}
	buf.WriteString("\n")

	// Determine row count (use shortest column to avoid out-of-range)
	rowCount := 0
	if len(df.ColumnOrder) > 0 {
		rowCount = df.Columns[df.ColumnOrder[0]].Len()
		for _, colName := range df.ColumnOrder[1:] {
			if s := df.Columns[colName]; s != nil && s.Len() < rowCount {
				rowCount = s.Len()
			}
		}
	}

	// Write data rows
	for r := 0; r < rowCount; r++ {
		for i, colName := range df.ColumnOrder {
			if i > 0 {
				buf.WriteString(sep)
			}
			series := df.Columns[colName]
			if series.IsNull(r) {
				// Null values are represented as empty strings
				buf.WriteString("")
			} else {
				val, _ := series.At(r)
				buf.WriteString(fmt.Sprintf("%v", val))
			}
		}
		buf.WriteString("\n")
	}

	// If filepath is provided, write to file and return nil
	if filepath != "" {
		err := os.WriteFile(filepath, buf.Bytes(), 0644)
		if err != nil {
			return "", fmt.Errorf("failed to write CSV to file: %w", err)
		}
		return "", nil
	}

	// If no filepath, return the CSV string
	return buf.String(), nil
}

// Loc returns a label-based indexer for the DataFrame
func (df *DataFrame) Loc() *LocIndexer {
	return &LocIndexer{df: df}
}

// ILoc returns an integer position-based indexer for the DataFrame
func (df *DataFrame) ILoc() *iLocIndexer {
	return &iLocIndexer{df: df}
}

// Select returns a new DataFrame with only the specified columns.
// If a single column is requested, still returns a DataFrame (not a Series).
func (df *DataFrame) Select(columns ...string) (*DataFrame, error) {
	if df == nil {
		return nil, errors.New("DataFrame is nil")
	}
	if len(columns) == 0 {
		return nil, errors.New("at least one column name is required")
	}

	df.RLock()
	defer df.RUnlock()

	// Validate all columns exist
	for _, colName := range columns {
		if _, ok := df.Columns[colName]; !ok {
			return nil, fmt.Errorf("column '%s' not found", colName)
		}
	}

	// Create new DataFrame with selected columns (zero-copy - just reference same Series)
	newCols := make(map[string]collection.Series, len(columns))
	for _, colName := range columns {
		newCols[colName] = df.Columns[colName]
	}

	return &DataFrame{
		Columns:     newCols,
		ColumnOrder: append([]string(nil), columns...),
		Index:       append([]string(nil), df.Index...),
	}, nil
}

// SelectCol returns a single column as a Series reference.
// This provides direct access to the underlying Series.
func (df *DataFrame) SelectCol(column string) (collection.Series, error) {
	if df == nil {
		return nil, errors.New("DataFrame is nil")
	}

	df.RLock()
	defer df.RUnlock()

	series, ok := df.Columns[column]
	if !ok {
		return nil, fmt.Errorf("column '%s' not found", column)
	}

	return series, nil
}

// SetIndex sets custom row labels for the DataFrame.
// The length of the index must match the number of rows in the DataFrame.
func (df *DataFrame) SetIndex(index []string) error {
	if df == nil {
		return errors.New("DataFrame is nil")
	}

	df.Lock()
	defer df.Unlock()

	// Determine row count
	rowCount := 0
	if len(df.ColumnOrder) > 0 {
		rowCount = df.Columns[df.ColumnOrder[0]].Len()
	}

	if len(index) != rowCount {
		return fmt.Errorf("index length (%d) must match number of rows (%d)", len(index), rowCount)
	}

	df.Index = append([]string(nil), index...)
	return nil
}

// ResetIndex resets the index to default integer sequence ("0", "1", "2", ...).
func (df *DataFrame) ResetIndex() {
	if df == nil {
		return
	}

	df.Lock()
	defer df.Unlock()

	// Determine row count
	rowCount := 0
	if len(df.ColumnOrder) > 0 {
		rowCount = df.Columns[df.ColumnOrder[0]].Len()
	}

	// Create default index
	df.Index = make([]string, rowCount)
	for i := 0; i < rowCount; i++ {
		df.Index[i] = fmt.Sprintf("%d", i)
	}
}
