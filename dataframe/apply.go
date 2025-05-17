package dataframe

import (
	"errors"
	"fmt"
)

// ApplyAxis represents the axis on which to apply a function
type ApplyAxis int

const (
	// ColumnWise applies the function to each column (like pandas axis=0)
	ColumnWise ApplyAxis = 0
	// RowWise applies the function to each row (like pandas axis=1)
	RowWise ApplyAxis = 1
)

// ApplyOption is a function type that modifies apply operation settings.
type ApplyOption func(*applyOptions)

// applyOptions holds all configurable settings for DataFrame apply operations.
type applyOptions struct {
	axis       ApplyAxis
	resultType SeriesType
}

// defaultApplyOptions returns the default options for apply operations.
func defaultApplyOptions() *applyOptions {
	return &applyOptions{
		axis:       ColumnWise,
		resultType: StringType, // Default result type
	}
}

// WithAxis sets the axis for apply operations (ColumnWise or RowWise).
func WithAxis(axis ApplyAxis) ApplyOption {
	return func(o *applyOptions) {
		o.axis = axis
	}
}

// WithResultType sets the type of the resulting Series.
func WithResultType(resultType SeriesType) ApplyOption {
	return func(o *applyOptions) {
		o.resultType = resultType
	}
}

// Apply applies a function to each row or column of the DataFrame.
//
// By default, the function is applied column-wise (similar to pandas axis=0).
// Use the WithAxis option to change to row-wise application.
//
// The function parameter should accept a Series or slice of values and return a single value.
// For column-wise application, the function receives each column Series.
// For row-wise application, the function receives a slice containing values from each column for that row.
//
// Parameters:
//   - fn: Function to apply to each row or column
//   - opts: Optional functional parameters to customize the operation
//
// Returns:
//   - For column-wise application: a new Series containing the result for each column
//   - For row-wise application: a new Series containing the result for each row
//   - An error if the operation fails
//
// Example (column-wise with predefined function):
//
//	// Define a function to calculate the sum of a column
//	func calculateSum(s Series) any {
//	    sum := 0.0
//	    for i := 0; i < s.Len(); i++ {
//	        if !s.IsNull(i) {
//	            switch v := s.GetValue(i).(type) {
//	            case int:
//	                sum += float64(v)
//	            case float64:
//	                sum += v
//	            }
//	        }
//	    }
//	    return sum
//	}
//
//	// Apply the function to calculate sum of each column
//	result, err := df.Apply(calculateSum, WithResultType(FloatType))
//
// Example (row-wise with predefined function):
//
//	// Define a function to calculate the product of values in a row
//	func calculateRowProduct(row []any) any {
//	    product := 1.0
//	    for _, val := range row {
//	        if val != nil {
//	            switch v := val.(type) {
//	            case int:
//	                product *= float64(v)
//	            case float64:
//	                product *= v
//	            }
//	        }
//	    }
//	    return product
//	}
//
//	// Apply the function to calculate product of each row
//	result, err := df.Apply(calculateRowProduct, WithAxis(RowWise), WithResultType(FloatType))
func (df *DataFrame) Apply(fn interface{}, opts ...ApplyOption) (Series, error) {
	if df == nil {
		return nil, errors.New("DataFrame is nil")
	}

	// Apply options
	options := defaultApplyOptions()
	for _, opt := range opts {
		opt(options)
	}

	// Validate function type based on axis
	switch options.axis {
	case ColumnWise:
		_, ok := fn.(func(Series) any)
		if !ok {
			return nil, errors.New("for column-wise application, function must have signature func(Series) any")
		}
	case RowWise:
		_, ok := fn.(func([]any) any)
		if !ok {
			return nil, errors.New("for row-wise application, function must have signature func([]any) any")
		}
	default:
		return nil, fmt.Errorf("invalid axis: %d", options.axis)
	}

	switch options.axis {
	case ColumnWise:
		return df.applyColumnWise(fn.(func(Series) any), options)
	case RowWise:
		return df.applyRowWise(fn.(func([]any) any), options)
	default:
		return nil, fmt.Errorf("invalid axis: %d", options.axis)
	}
}

// applyColumnWise applies a function to each column of the DataFrame.
func (df *DataFrame) applyColumnWise(fn func(Series) any, options *applyOptions) (Series, error) {
	resultLen := len(df.Columns)
	resultSeries := CreateSeries(options.resultType, "result", resultLen)

	for i, colName := range df.Columns {
		if series, ok := df.Series[colName]; ok {
			result := fn(series)
			if err := resultSeries.SetValue(i, result); err != nil {
				return nil, fmt.Errorf("error setting result for column %s: %w", colName, err)
			}
		} else {
			return nil, fmt.Errorf("column %s not found in DataFrame", colName)
		}
	}

	return resultSeries, nil
}

// applyRowWise applies a function to each row of the DataFrame.
func (df *DataFrame) applyRowWise(fn func([]any) any, options *applyOptions) (Series, error) {
	rows := df.Rows()
	resultSeries := CreateSeries(options.resultType, "result", rows)

	for i := 0; i < rows; i++ {
		// Build row data
		rowData := make([]any, len(df.Columns))
		for j, colName := range df.Columns {
			if series, ok := df.Series[colName]; ok {
				if series.IsNull(i) {
					rowData[j] = nil
				} else {
					rowData[j] = series.GetValue(i)
				}
			} else {
				return nil, fmt.Errorf("column %s not found in DataFrame", colName)
			}
		}

		// Apply function to row
		result := fn(rowData)
		if err := resultSeries.SetValue(i, result); err != nil {
			return nil, fmt.Errorf("error setting result for row %d: %w", i, err)
		}
	}

	return resultSeries, nil
}

// ApplyInPlace applies a function to each value in the DataFrame and modifies the DataFrame in place.
//
// This method allows you to transform each value in the DataFrame individually.
// The function will receive the current value, row index, and column name, and should return the new value.
//
// Parameters:
//   - fn: Function with signature func(value any, row int, col string) any
//   - opts: Optional functional parameters (currently not used, for future extensibility)
//
// Returns:
//   - Error if the operation fails
//
// Example:
//
//	// Define a function to double numeric values
//	func doubleNumericValues(value any, row int, col string) any {
//	    switch v := value.(type) {
//	    case int:
//	        return v * 2
//	    case float64:
//	        return v * 2
//	    default:
//	        return v
//	    }
//	}
//
//	// Apply the function to transform values in-place
//	err := df.ApplyInPlace(doubleNumericValues)
func (df *DataFrame) ApplyInPlace(fn func(value any, row int, col string) any, opts ...ApplyOption) error {
	if df == nil {
		return errors.New("DataFrame is nil")
	}

	rows := df.Rows()
	for i := 0; i < rows; i++ {
		for _, colName := range df.Columns {
			if series, ok := df.Series[colName]; ok {
				if !series.IsNull(i) {
					oldValue := series.GetValue(i)
					newValue := fn(oldValue, i, colName)
					if err := series.SetValue(i, newValue); err != nil {
						return fmt.Errorf("error setting value at row %d, column %s: %w", i, colName, err)
					}
				}
			}
		}
	}

	return nil
}
