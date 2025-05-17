package gpandas

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"gpandas/dataframe"
	"gpandas/utils/nullable"
	"io"
	"os"
	"runtime"
	"sync"
)

// GoPandas is the main entry point for data manipulation functions.
type GoPandas struct{}

// Option is a function type that modifies GPandas operation settings.
type Option func(*options)

// options holds all configurable settings for GPandas operations.
type options struct {
	// CSV Reader options
	csvSeparator   rune
	csvComment     rune
	csvHeaderRow   bool
	csvAutoType    bool
	csvWorkerCount int

	// DataFrame options
	nullValue      any
	defaultContext context.Context
}

// defaultOptions returns the default options.
func defaultOptions() *options {
	return &options{
		csvSeparator:   ',',
		csvComment:     '#',
		csvHeaderRow:   true,
		csvAutoType:    false,
		csvWorkerCount: runtime.NumCPU(),
		nullValue:      nil,
		defaultContext: context.Background(),
	}
}

// WithCSVSeparator sets the separator character for CSV operations.
func WithCSVSeparator(sep rune) Option {
	return func(o *options) {
		o.csvSeparator = sep
	}
}

// WithCSVComment sets the comment character for CSV operations.
func WithCSVComment(comment rune) Option {
	return func(o *options) {
		o.csvComment = comment
	}
}

// WithCSVHeaderRow specifies whether the CSV file has a header row.
func WithCSVHeaderRow(hasHeader bool) Option {
	return func(o *options) {
		o.csvHeaderRow = hasHeader
	}
}

// WithCSVAutoType enables automatic type detection for CSV data.
func WithCSVAutoType(autoType bool) Option {
	return func(o *options) {
		o.csvAutoType = autoType
	}
}

// WithWorkerCount sets the number of worker goroutines for parallel operations.
func WithWorkerCount(count int) Option {
	return func(o *options) {
		if count > 0 {
			o.csvWorkerCount = count
		}
	}
}

// WithNullValue sets the default value used to represent nulls.
func WithNullValue(value any) Option {
	return func(o *options) {
		o.nullValue = value
	}
}

// WithContext sets a custom context for operations.
func WithContext(ctx context.Context) Option {
	return func(o *options) {
		if ctx != nil {
			o.defaultContext = ctx
		}
	}
}

// Apply applies a function to each row or column of the DataFrame.
//
// This is a convenience method that forwards the call to the DataFrame's Apply method.
//
// Parameters:
//   - df: The DataFrame to apply the function to
//   - fn: Function to apply to each row or column
//   - opts: Optional functional parameters to customize the operation
//
// Returns:
//   - A new Series containing the result of the apply operation
//   - An error if the operation fails
//
// Example:
//
//	// Define a function to calculate mean of a column
//	func calculateMean(s dataframe.Series) any {
//	    sum := 0.0
//	    count := 0
//	    for i := 0; i < s.Len(); i++ {
//	        if !s.IsNull(i) {
//	            switch v := s.GetValue(i).(type) {
//	            case int:
//	                sum += float64(v)
//	                count++
//	            case float64:
//	                sum += v
//	                count++
//	            }
//	        }
//	    }
//	    if count > 0 {
//	        return sum / float64(count)
//	    }
//	    return 0.0
//	}
//
//	// Calculate mean of each column
//	result, err := gp.Apply(df, calculateMean, dataframe.WithResultType(dataframe.FloatType))
func (GoPandas) Apply(df *dataframe.DataFrame, fn interface{}, opts ...dataframe.ApplyOption) (dataframe.Series, error) {
	if df == nil {
		return nil, errors.New("DataFrame is nil")
	}
	return df.Apply(fn, opts...)
}

// DataFrame creates a new DataFrame from the provided columns, data, and column types.
//
// It validates the input parameters to ensure data consistency and proper type definitions.
//
// The function performs several validation checks:
// - Ensures column_types map is provided
// - Verifies at least one column name is present
// - Checks that data is not empty
// - Confirms the number of columns matches the data columns
// - Validates all columns have the same length
// - Ensures type definitions exist for all columns
//
// The data is then converted to the internal DataFrame format, performing type assertions
// based on the specified column types.
//
// Parameters:
//
//	columns: A slice of strings representing column names
//	data: A slice of []any containing the actual data
//	columns_types: A map defining the expected type for each column
//	opts: Optional functional parameters to customize the operation
//
// Returns:
//
//	A pointer to a DataFrame containing the processed data, or an error if validation fails
func (GoPandas) DataFrame(columns []string, data [][]any, columns_types map[string]dataframe.SeriesType, opts ...Option) (*dataframe.DataFrame, error) {
	// Apply options
	options := defaultOptions()
	for _, opt := range opts {
		opt(options)
	}

	// Validate inputs
	if columns_types == nil {
		return nil, errors.New("columns_types map is required to assert column types")
	}

	if len(columns) == 0 {
		return nil, errors.New("at least one column name is required")
	}

	if len(data) == 0 {
		return nil, errors.New("data cannot be empty")
	}

	if len(columns) != len(data) {
		return nil, errors.New("number of columns must match number of data columns")
	}

	// Validate all columns have same length
	rowCount := len(data[0])
	for i, col := range data {
		if len(col) != rowCount {
			return nil, fmt.Errorf("inconsistent row count in column %s: expected %d, got %d", columns[i], rowCount, len(col))
		}
	}

	// Validate column types
	for _, colName := range columns {
		if _, exists := columns_types[colName]; !exists {
			return nil, fmt.Errorf("missing type definition for column: %s", colName)
		}
	}

	// Create DataFrame
	df := dataframe.NewDataFrame(columns)

	// Create series for each column
	for i, colName := range columns {
		seriesType := columns_types[colName]
		series := dataframe.CreateSeries(seriesType, colName, rowCount)

		// Fill series with data
		for j, val := range data[i] {
			if err := series.SetValue(j, val); err != nil {
				return nil, fmt.Errorf("error setting value for column %s, row %d: %w", colName, j, err)
			}
		}

		// Add series to DataFrame
		df.Series[colName] = series
	}

	return df, nil
}

// Read_csv reads a CSV file from the specified filepath and converts it into a DataFrame.
//
// It opens the CSV file, reads the header to determine the column names, and then reads all the records.
//
// The function checks for errors during file operations and ensures that the CSV file is not empty.
//
// It initializes data columns based on the number of headers and populates them with the corresponding values from the records.
//
// If the number of columns in any row is inconsistent with the header, an error is returned.
//
// Parameters:
//
//	filepath: A string representing the path to the CSV file to be read.
//	opts: Optional functional parameters to customize the CSV reading behavior.
//
// Returns:
//
//	A pointer to a DataFrame containing the data from the CSV file, or an error if the operation fails.
func (gp GoPandas) Read_csv(filepath string, opts ...Option) (*dataframe.DataFrame, error) {
	// Apply options
	options := defaultOptions()
	for _, opt := range opts {
		opt(options)
	}

	file, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = options.csvSeparator
	reader.Comment = options.csvComment

	// Read header
	headers, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("error reading headers: %w", err)
	}

	columnCount := len(headers)
	if columnCount == 0 {
		return nil, errors.New("no headers found in CSV")
	}

	// Create a context that can be cancelled when an error occurs
	ctx, cancel := context.WithCancel(options.defaultContext)
	defer cancel() // Ensure context is cancelled when function returns

	// Use a worker pool for dynamic workload distribution
	type RowData struct {
		Index int
		Row   []string
	}
	rowChan := make(chan RowData, 100)                       // Buffered channel to hold rows
	resultChan := make(chan [][]any, options.csvWorkerCount) // Channel to hold columnar data
	errChan := make(chan error, 1)                           // Channel to communicate errors
	var wg sync.WaitGroup

	// Start workers for processing rows
	for i := 0; i < options.csvWorkerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Local column buffers
			localData := make([][]any, columnCount)
			for i := range localData {
				localData[i] = make([]any, 0, 100) // Preallocate some space
			}

			for {
				select {
				case row, ok := <-rowChan:
					if !ok {
						// Channel closed, send local data and exit
						if len(localData[0]) > 0 {
							resultChan <- localData
						}
						return
					}

					// Process the row data
					for j, val := range row.Row {
						// Convert empty strings to nil for null support
						if val == "" {
							localData[j] = append(localData[j], nil)
						} else {
							// Store as string values initially
							localData[j] = append(localData[j], val)
						}
					}
				case <-ctx.Done():
					// Context cancelled, exit without sending results
					return
				}
			}
		}()
	}

	// Feed rows to workers
	go func() {
		index := 0
		defer close(rowChan) // Ensure rowChan is closed when this goroutine exits

		for {
			select {
			case <-ctx.Done():
				// Context cancelled, stop processing
				return
			default:
				record, err := reader.Read()
				if err == io.EOF {
					return
				}
				if err != nil {
					// Report error and cancel context to stop all goroutines
					select {
					case errChan <- fmt.Errorf("error reading CSV at row %d: %w", index+1, err):
					default:
					}
					cancel()
					return
				}

				// Check for inconsistent column counts
				if len(record) != columnCount {
					errMsg := fmt.Errorf("inconsistent column count in row %d: expected %d columns, got %d",
						index+1, columnCount, len(record))
					select {
					case errChan <- errMsg:
					default:
					}
					cancel()
					return
				}

				rowChan <- RowData{Index: index, Row: record}
				index++
			}
		}
	}()

	// Set up a goroutine to wait for workers and close result channel
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results in columnar format
	combinedData := make([][]any, columnCount)
	for i := range combinedData {
		combinedData[i] = make([]any, 0)
	}

	// Process results and check for errors
	var csvErr error
	resultsDone := make(chan struct{})

	go func() {
		for result := range resultChan {
			for j, colData := range result {
				combinedData[j] = append(combinedData[j], colData...)
			}
		}
		close(resultsDone)
	}()

	// Wait for either an error or all results to be processed
	select {
	case csvErr = <-errChan:
		// Error occurred, cancel context to stop all goroutines
		cancel()
		<-resultsDone // Wait for result processing to finish
	case <-resultsDone:
		// All results processed, check if there was an error
		select {
		case csvErr = <-errChan:
		default:
			// No error
		}
	}

	// Check if we had an error in processing
	if csvErr != nil {
		return nil, csvErr
	}

	// Create DataFrame and detect column types
	df := dataframe.NewDataFrame(headers)

	// Try to infer column types by checking values
	for i, header := range headers {
		// Create series from column data
		series := dataframe.CreateSeriesFromData(header, combinedData[i])
		if err := df.AddSeries(header, series); err != nil {
			return nil, fmt.Errorf("error adding series for column %s: %w", header, err)
		}
	}

	// Apply auto-typing if enabled
	if options.csvAutoType {
		df = gp.AutoType(df)
	}

	return df, nil
}

// AutoType attempts to convert string values to more appropriate types.
// This is a helper function that can be called after Read_csv to improve type detection.
func (GoPandas) AutoType(df *dataframe.DataFrame) *dataframe.DataFrame {
	if df == nil {
		return nil
	}

	result := dataframe.NewDataFrame(df.Columns)

	for _, colName := range df.Columns {
		series := df.Series[colName]

		// Skip if it's already the right type
		if _, isString := series.(*dataframe.StringSeries); !isString {
			result.Series[colName] = series.Copy()
			continue
		}

		// Try to parse as numbers
		canBeInt := true
		canBeFloat := true
		canBeBool := true

		// Check if all non-null values can be parsed as a specific type
		for i := 0; i < series.Len(); i++ {
			if series.IsNull(i) {
				continue
			}

			strVal := series.GetValue(i).(string)

			if canBeInt {
				_, err := nullable.ParseInt(strVal)
				if err != nil {
					canBeInt = false
				}
			}

			if canBeFloat {
				_, err := nullable.ParseFloat(strVal)
				if err != nil {
					canBeFloat = false
				}
			}

			if canBeBool {
				_, err := nullable.ParseBool(strVal)
				if err != nil {
					canBeBool = false
				}
			}

			// If no type is possible, stop checking
			if !canBeInt && !canBeFloat && !canBeBool {
				break
			}
		}

		// Create new series with the most specific possible type
		var newSeries dataframe.Series

		if canBeInt {
			newSeries = dataframe.NewIntSeries(colName, series.Len())
			for i := 0; i < series.Len(); i++ {
				if series.IsNull(i) {
					newSeries.SetValue(i, nil)
				} else {
					strVal := series.GetValue(i).(string)
					if intVal, err := nullable.ParseInt(strVal); err == nil {
						newSeries.SetValue(i, intVal.Value)
					}
				}
			}
		} else if canBeFloat {
			newSeries = dataframe.NewFloatSeries(colName, series.Len())
			for i := 0; i < series.Len(); i++ {
				if series.IsNull(i) {
					newSeries.SetValue(i, nil)
				} else {
					strVal := series.GetValue(i).(string)
					if floatVal, err := nullable.ParseFloat(strVal); err == nil {
						newSeries.SetValue(i, floatVal.Value)
					}
				}
			}
		} else if canBeBool {
			newSeries = dataframe.NewBoolSeries(colName, series.Len())
			for i := 0; i < series.Len(); i++ {
				if series.IsNull(i) {
					newSeries.SetValue(i, nil)
				} else {
					strVal := series.GetValue(i).(string)
					if boolVal, err := nullable.ParseBool(strVal); err == nil {
						newSeries.SetValue(i, boolVal.Value)
					}
				}
			}
		} else {
			// Keep as string
			newSeries = series.Copy()
		}

		result.Series[colName] = newSeries
	}

	return result
}
