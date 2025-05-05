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

// DataFrameOption is a function type that modifies DataFrame operation settings.
type DataFrameOption func(*dataFrameOptions)

// dataFrameOptions holds all configurable settings for DataFrame operations.
type dataFrameOptions struct {
	// CSV options
	csvSeparator rune
	csvQuote     rune
	csvEscape    rune
	csvHeader    bool

	// Merge options
	mergeLeftOn  string
	mergeRightOn string
	mergeSuffix  map[string]string

	// Display options
	maxRows     int
	maxColumns  int
	nullDisplay string

	// Filtering and sorting
	filterFunc    func(row int) bool
	sortColumns   []string
	sortAscending []bool
}

// defaultDataFrameOptions returns the default options for DataFrame operations.
func defaultDataFrameOptions() *dataFrameOptions {
	return &dataFrameOptions{
		csvSeparator:  ',',
		csvQuote:      '"',
		csvEscape:     '\\',
		csvHeader:     true,
		maxRows:       10,
		maxColumns:    0, // 0 means no limit
		nullDisplay:   "NULL",
		mergeLeftOn:   "",
		mergeRightOn:  "",
		mergeSuffix:   map[string]string{"_x": "_y"},
		filterFunc:    nil,
		sortColumns:   nil,
		sortAscending: nil,
	}
}

// WithCSVSeparator sets the separator character for CSV output.
func WithCSVSeparator(sep rune) DataFrameOption {
	return func(o *dataFrameOptions) {
		o.csvSeparator = sep
	}
}

// WithCSVQuote sets the quote character for CSV output.
func WithCSVQuote(quote rune) DataFrameOption {
	return func(o *dataFrameOptions) {
		o.csvQuote = quote
	}
}

// WithCSVEscape sets the escape character for CSV output.
func WithCSVEscape(escape rune) DataFrameOption {
	return func(o *dataFrameOptions) {
		o.csvEscape = escape
	}
}

// WithCSVHeader specifies whether to include headers in CSV output.
func WithCSVHeader(includeHeader bool) DataFrameOption {
	return func(o *dataFrameOptions) {
		o.csvHeader = includeHeader
	}
}

// WithMaxRows sets the maximum rows to display.
func WithMaxRows(maxRows int) DataFrameOption {
	return func(o *dataFrameOptions) {
		if maxRows >= 0 {
			o.maxRows = maxRows
		}
	}
}

// WithMaxColumns sets the maximum columns to display.
func WithMaxColumns(maxColumns int) DataFrameOption {
	return func(o *dataFrameOptions) {
		if maxColumns >= 0 {
			o.maxColumns = maxColumns
		}
	}
}

// WithNullDisplay sets how NULL values are displayed.
func WithNullDisplay(display string) DataFrameOption {
	return func(o *dataFrameOptions) {
		o.nullDisplay = display
	}
}

// WithMergeOn sets the columns to merge on when the columns have the same name in both DataFrames.
func WithMergeOn(on string) DataFrameOption {
	return func(o *dataFrameOptions) {
		o.mergeLeftOn = on
		o.mergeRightOn = on
	}
}

// WithMergeColumns sets different column names to merge on for left and right DataFrames.
func WithMergeColumns(leftOn, rightOn string) DataFrameOption {
	return func(o *dataFrameOptions) {
		o.mergeLeftOn = leftOn
		o.mergeRightOn = rightOn
	}
}

// WithMergeSuffix sets the suffixes to use for duplicate column names in a merge.
func WithMergeSuffix(leftSuffix, rightSuffix string) DataFrameOption {
	return func(o *dataFrameOptions) {
		o.mergeSuffix = map[string]string{leftSuffix: rightSuffix}
	}
}

// WithFilter sets a filter function that determines which rows to include.
func WithFilter(filterFunc func(row int) bool) DataFrameOption {
	return func(o *dataFrameOptions) {
		o.filterFunc = filterFunc
	}
}

// WithSort sets columns to sort by and whether to sort ascending or descending.
func WithSort(columns []string, ascending []bool) DataFrameOption {
	return func(o *dataFrameOptions) {
		o.sortColumns = columns
		o.sortAscending = ascending
	}
}

// ToCSV converts the DataFrame to a CSV string representation or writes it to a file.
//
// Parameters:
//   - filepath: file path to write the CSV to (empty string to return as string)
//   - options: Optional functional parameters to customize the CSV output
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
//	// Get CSV with custom separator
//	csv, err := df.ToCSV("", WithCSVSeparator(';'))
//
//	// Write to file with custom settings
//	_, err := df.ToCSV("path/to/output.csv", WithCSVSeparator(';'), WithCSVHeader(false))
func (df *DataFrame) ToCSV(filepath string, opts ...DataFrameOption) (string, error) {
	if df == nil {
		return "", errors.New("DataFrame is nil")
	}

	// Apply options
	options := defaultDataFrameOptions()
	for _, opt := range opts {
		opt(options)
	}

	var buf bytes.Buffer

	// Write headers if enabled
	if options.csvHeader {
		for i, col := range df.Columns {
			if i > 0 {
				buf.WriteRune(options.csvSeparator)
			}
			buf.WriteString(col)
		}
		buf.WriteString("\n")
	}

	// Write data
	rowCount := df.Rows()
	for i := 0; i < rowCount; i++ {
		// Apply filter if set
		if options.filterFunc != nil && !options.filterFunc(i) {
			continue
		}

		for j, colName := range df.Columns {
			if j > 0 {
				buf.WriteRune(options.csvSeparator)
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
// Parameters:
//   - opts: Optional functional parameters to customize the output
//
// Returns:
//   - string: The formatted table representation of the DataFrame
//
// Example:
//
//	// Basic string representation
//	fmt.Println(df.String())
//
//	// Custom string representation
//	fmt.Println(df.String(WithMaxRows(5), WithNullDisplay("NA")))
func (df *DataFrame) String(opts ...DataFrameOption) string {
	if df == nil {
		return "DataFrame is nil"
	}

	// Apply options
	options := defaultDataFrameOptions()
	for _, opt := range opts {
		opt(options)
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

	// Apply column limits if necessary
	displayColumns := df.Columns
	if options.maxColumns > 0 && len(df.Columns) > options.maxColumns {
		displayColumns = df.Columns[:options.maxColumns]
	}

	// Set headers using the DataFrame's Columns
	table.SetHeader(displayColumns)

	// Determine number of rows and max rows to display
	numRows := df.Rows()
	displayRows := numRows
	if options.maxRows > 0 && numRows > options.maxRows {
		displayRows = options.maxRows
	}

	// For each row, collect data from all series
	for i := 0; i < displayRows; i++ {
		// Apply filter if set
		if options.filterFunc != nil && !options.filterFunc(i) {
			continue
		}

		row := make([]string, len(displayColumns))
		for j, colName := range displayColumns {
			series, ok := df.Series[colName]
			if !ok {
				row[j] = "N/A"
				continue
			}

			if series.IsNull(i) {
				row[j] = options.nullDisplay
			} else {
				row[j] = fmt.Sprintf("%v", series.GetValue(i))
			}
		}
		table.Append(row)
	}

	// Add row count information.
	shape := fmt.Sprintf("[%d rows x %d columns]", numRows, len(df.Columns))
	if numRows > displayRows {
		shape = fmt.Sprintf("Showing first %d rows of %d rows x %d columns", displayRows, numRows, len(df.Columns))
	}

	// Render the table and return the string representation
	table.Render()
	return buf.String() + shape + "\n"
}

// Get returns the value at a specific row and column.
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

// Set sets the value at a specific row and column.
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

// MergeWithOptions combines two DataFrames based on a common column and specified merge strategy.
// This is a new function that uses the functional options pattern and wraps the original Merge method.
//
// Parameters:
//   - right: The right DataFrame to merge with this DataFrame
//   - how: The merge strategy to use (inner, left, right, full)
//   - opts: Optional functional parameters to customize the merge
//
// Returns:
//   - *DataFrame: A new DataFrame containing the merged data
//   - error: An error if the merge fails
//
// Example:
//
//	// Basic merge on "ID" column
//	result, err := df1.MergeWithOptions(df2, dataframe.InnerMerge, WithMergeOn("ID"))
//
//	// Merge with different column names
//	result, err := df1.MergeWithOptions(df2, dataframe.LeftMerge, WithMergeColumns("LeftID", "RightID"))
//
//	// Merge with custom suffixes for duplicate columns
//	result, err := df1.MergeWithOptions(df2, dataframe.FullMerge, WithMergeOn("ID"), WithMergeSuffix("_left", "_right"))
func (df *DataFrame) MergeWithOptions(right *DataFrame, how MergeHow, opts ...DataFrameOption) (*DataFrame, error) {
	if df == nil {
		return nil, errors.New("left DataFrame is nil")
	}
	if right == nil {
		return nil, errors.New("right DataFrame is nil")
	}

	// Apply options
	options := defaultDataFrameOptions()
	for _, opt := range opts {
		opt(options)
	}

	// If options specify merge columns, use them
	if options.mergeLeftOn != "" && options.mergeRightOn != "" {
		// In case different column names are specified, we need to create a temporary right DataFrame
		// with the right column renamed to match the left column name
		if options.mergeLeftOn != options.mergeRightOn {
			// Create a copy of the right DataFrame
			tempRight := right.Copy()

			// Rename the right column to match the left column
			renameMap := map[string]string{options.mergeRightOn: options.mergeLeftOn}
			if err := tempRight.Rename(renameMap); err != nil {
				return nil, fmt.Errorf("failed to rename merge column: %w", err)
			}

			// Call the original implementation with the renamed column
			// The signature from merge.go is: Merge(other *DataFrame, on string, how MergeHow)
			return df.Merge(tempRight, options.mergeLeftOn, how)
		}

		// If the column names are the same, just use the standard implementation
		return df.Merge(right, options.mergeLeftOn, how)
	}

	// No merge columns specified in options, this is an error
	return nil, errors.New("merge columns must be specified using WithMergeOn or WithMergeColumns")
}
