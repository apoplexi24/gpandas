package dataframe

import (
	"bytes"
	"errors"
	"fmt"
	"gpandas/utils/collection"
	"os"
	"sync"

	"github.com/olekukonko/tablewriter"
)

type GoPandas struct{}

// DataFrame represents a table of data with named columns.
type DataFrame struct {
	sync.Mutex
	Columns []string
	Series  map[string]Series
}

// NewDataFrame creates a new empty DataFrame with the specified column names.
func NewDataFrame(columns []string) *DataFrame {
	df := &DataFrame{
		Columns: columns,
		Series:  make(map[string]Series),
	}
	return df
}

// AddSeries adds a Series to the DataFrame with the given name.
func (df *DataFrame) AddSeries(name string, series Series) error {
	df.Lock()
	defer df.Unlock()

	// Validation
	if _, exists := df.Series[name]; exists {
		return fmt.Errorf("column '%s' already exists in DataFrame", name)
	}

	series.SetName(name)
	df.Series[name] = series

	// Add to columns if not already present
	found := false
	for _, col := range df.Columns {
		if col == name {
			found = true
			break
		}
	}
	if !found {
		df.Columns = append(df.Columns, name)
	}

	return nil
}

// Rows returns the number of rows in the DataFrame.
func (df *DataFrame) Rows() int {
	if len(df.Columns) == 0 {
		return 0
	}

	if len(df.Series) == 0 {
		return 0
	}

	// Return length of first series
	firstCol := df.Columns[0]
	if series, ok := df.Series[firstCol]; ok {
		return series.Len()
	}

	return 0
}

// GetMapKeys returns a set of keys from a map
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

	dfcols, err2 := collection.ToSet(df.Columns)
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

	// Update columns and series map
	for original_column_name, new_column_name := range columns {
		// Update column name in Columns slice
		for df_column_idx := range df.Columns {
			if df.Columns[df_column_idx] == original_column_name {
				df.Columns[df_column_idx] = new_column_name
			}
		}

		// Update series map
		if series, ok := df.Series[original_column_name]; ok {
			series.SetName(new_column_name)
			df.Series[new_column_name] = series
			delete(df.Series, original_column_name)
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
//   - A summary line showing dimensions ([rows x columns]) is appended
//
// The table format follows this pattern:
//
//	+-----+-----+-----+
//	| Col1| Col2| Col3|
//	+-----+-----+-----+
//	| val1| val2| val3|
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

	// Set headers using the DataFrame's Columns
	table.SetHeader(df.Columns)

	// Determine number of rows and max rows to display
	numRows := df.Rows()
	displayRows := numRows
	if numRows > 10 {
		displayRows = 10
	}

	// For each row, collect data from all series
	for i := 0; i < displayRows; i++ {
		row := make([]string, len(df.Columns))
		for j, colName := range df.Columns {
			series, ok := df.Series[colName]
			if !ok {
				row[j] = "N/A"
				continue
			}

			if series.IsNull(i) {
				row[j] = "NULL"
			} else {
				row[j] = fmt.Sprintf("%v", series.GetValue(i))
			}
		}
		table.Append(row)
	}

	// Add row count information.
	shape := fmt.Sprintf("[%d rows x %d columns]", numRows, len(df.Columns))
	if numRows > 10 {
		shape = fmt.Sprintf("Showing first 10 rows of %d rows x %d columns", numRows, len(df.Columns))
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

	sep := ","
	if len(separator) > 0 && separator[0] != "" {
		sep = separator[0]
	}

	var buf bytes.Buffer

	// Write headers
	for i, col := range df.Columns {
		if i > 0 {
			buf.WriteString(sep)
		}
		buf.WriteString(col)
	}
	buf.WriteString("\n")

	// Write data
	rowCount := df.Rows()
	for i := 0; i < rowCount; i++ {
		for j, colName := range df.Columns {
			if j > 0 {
				buf.WriteString(sep)
			}

			series, ok := df.Series[colName]
			if !ok || series.IsNull(i) {
				// Write empty value for null
				buf.WriteString("")
			} else {
				buf.WriteString(fmt.Sprintf("%v", series.GetValue(i)))
			}
		}
		buf.WriteString("\n")
	}

	// If filepath provided, write to file, otherwise return as string
	if filepath != "" {
		err := os.WriteFile(filepath, buf.Bytes(), 0644)
		if err != nil {
			return "", fmt.Errorf("error writing CSV to file: %w", err)
		}
		return "", nil
	}

	return buf.String(), nil
}

// get returns the value at a specific row and column.
func (df *DataFrame) Get(row int, col string) (any, error) {
	if df == nil {
		return nil, errors.New("DataFrame is nil")
	}

	series, ok := df.Series[col]
	if !ok {
		return nil, fmt.Errorf("column '%s' not found", col)
	}

	if row < 0 || row >= series.Len() {
		return nil, fmt.Errorf("row index %d out of bounds", row)
	}

	if series.IsNull(row) {
		return nil, nil
	}

	return series.GetValue(row), nil
}

// set sets the value at a specific row and column.
func (df *DataFrame) Set(row int, col string, value any) error {
	if df == nil {
		return errors.New("DataFrame is nil")
	}

	series, ok := df.Series[col]
	if !ok {
		return fmt.Errorf("column '%s' not found", col)
	}

	if row < 0 || row >= series.Len() {
		return fmt.Errorf("row index %d out of bounds", row)
	}

	return series.SetValue(row, value)
}

// Copy creates a deep copy of the DataFrame.
func (df *DataFrame) Copy() *DataFrame {
	if df == nil {
		return nil
	}

	newDf := NewDataFrame(make([]string, len(df.Columns)))
	copy(newDf.Columns, df.Columns)

	for _, colName := range df.Columns {
		if series, ok := df.Series[colName]; ok {
			newDf.Series[colName] = series.Copy()
		}
	}

	return newDf
}

// Head returns a new DataFrame containing the first n rows.
func (df *DataFrame) Head(n int) *DataFrame {
	if df == nil {
		return nil
	}

	if n <= 0 {
		return NewDataFrame(df.Columns)
	}

	rows := df.Rows()
	if n > rows {
		n = rows
	}

	newDf := NewDataFrame(make([]string, len(df.Columns)))
	copy(newDf.Columns, df.Columns)

	for _, colName := range df.Columns {
		if series, ok := df.Series[colName]; ok {
			newSeries := series.EmptyCopy(n)
			for i := 0; i < n; i++ {
				newSeries.SetValue(i, series.GetValue(i))
			}
			newDf.Series[colName] = newSeries
		}
	}

	return newDf
}

// IsNA returns true if the value at the specified row and column is null.
func (df *DataFrame) IsNA(row int, col string) (bool, error) {
	if df == nil {
		return false, errors.New("DataFrame is nil")
	}

	series, ok := df.Series[col]
	if !ok {
		return false, fmt.Errorf("column '%s' not found", col)
	}

	if row < 0 || row >= series.Len() {
		return false, fmt.Errorf("row index %d out of bounds", row)
	}

	return series.IsNull(row), nil
}

// FillNA fills null values in the DataFrame with the specified value.
func (df *DataFrame) FillNA(value any) *DataFrame {
	if df == nil {
		return nil
	}

	newDf := df.Copy()

	for _, colName := range newDf.Columns {
		series := newDf.Series[colName]
		for i := 0; i < series.Len(); i++ {
			if series.IsNull(i) {
				series.SetValue(i, value)
			}
		}
	}

	return newDf
}

// DropNA returns a new DataFrame with rows containing null values removed.
func (df *DataFrame) DropNA() *DataFrame {
	if df == nil {
		return nil
	}

	rows := df.Rows()
	if rows == 0 {
		return df.Copy()
	}

	// Find rows with no nulls
	validRows := make([]int, 0, rows)
	for i := 0; i < rows; i++ {
		hasNull := false
		for _, colName := range df.Columns {
			if series, ok := df.Series[colName]; ok {
				if series.IsNull(i) {
					hasNull = true
					break
				}
			}
		}
		if !hasNull {
			validRows = append(validRows, i)
		}
	}

	// Create new DataFrame with only valid rows
	newDf := NewDataFrame(make([]string, len(df.Columns)))
	copy(newDf.Columns, df.Columns)

	for _, colName := range df.Columns {
		if series, ok := df.Series[colName]; ok {
			newSeries := series.EmptyCopy(len(validRows))
			for i, rowIdx := range validRows {
				newSeries.SetValue(i, series.GetValue(rowIdx))
			}
			newDf.Series[colName] = newSeries
		}
	}

	return newDf
}
