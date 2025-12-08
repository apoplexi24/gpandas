package dataframe

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/apoplexi24/gpandas/utils/collection"
)

// GroupBy represents a grouped DataFrame.
type GroupBy struct {
	df       *DataFrame
	groups   map[string][]int // Map of group key to row indices
	axis     int
	colNames []string // Columns used for grouping
}

// GroupBy groups the DataFrame using a mapper or by a Series of columns.
// A groupby operation involves some combination of splitting the object, applying a function, and combining the results.
// This can be used to group large amounts of data and compute operations on these groups.
//
// Parameters:
//   - by: A slice of strings representing the column names to group by.
//   - axis: The axis to group along. 0 for rows, 1 for columns. Currently only axis 0 is supported for grouping by columns.
//
// Returns:
//   - A pointer to a GroupBy object.
//   - An error if the operation fails (e.g., invalid column names).
func (df *DataFrame) GroupBy(by []string, axis int) (*GroupBy, error) {
	if axis != 0 {
		return nil, fmt.Errorf("axis %d is not supported yet, only axis 0 (rows) is supported", axis)
	}

	// Validate columns
	for _, col := range by {
		if _, ok := df.Columns[col]; !ok {
			return nil, fmt.Errorf("column %s not found", col)
		}
	}

	groups := make(map[string][]int)
	numRows := df.Len()

	// Iterate over rows to build groups
	for i := 0; i < numRows; i++ {
		keyParts := make([]string, len(by))
		for j, colName := range by {
			val, err := df.Columns[colName].At(i)
			if err != nil {
				return nil, err
			}
			keyParts[j] = fmt.Sprintf("%v", val)
		}
		key := strings.Join(keyParts, "_") // Simple key generation
		groups[key] = append(groups[key], i)
	}

	return &GroupBy{
		df:       df,
		groups:   groups,
		axis:     axis,
		colNames: by,
	}, nil
}

// getSortedKeys returns the group keys sorted to ensure deterministic output order.
func (gb *GroupBy) getSortedKeys() []string {
	keys := make([]string, 0, len(gb.groups))
	for k := range gb.groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// aggregate applies a function to each column of each group.
func (gb *GroupBy) aggregate(aggFunc func(collection.Series) (any, error)) (*DataFrame, error) {
	sortedKeys := gb.getSortedKeys()
	numGroups := len(sortedKeys)

	// Identify numeric columns to aggregate (excluding grouping columns if they are not numeric,
	// but pandas usually keeps them as index. Here we will make them columns).
	// For simplicity, we aggregate all columns that support the operation.
	// We will reconstruct the grouping columns as the first columns.

	resultCols := make(map[string]collection.Series)
	resultOrder := make([]string, 0)

	// Add grouping columns first
	for _, colName := range gb.colNames {
		resultCols[colName], _ = collection.NewStringSeriesFromData(make([]string, numGroups), nil) // Using StringSeries for keys for now
		resultOrder = append(resultOrder, colName)
	}

	// Add other columns
	for _, colName := range gb.df.ColumnOrder {
		isGroupingCol := false
		for _, gCol := range gb.colNames {
			if colName == gCol {
				isGroupingCol = true
				break
			}
		}
		if !isGroupingCol {
			// Check if we can aggregate this column (e.g. numeric)
			// For now, we try to aggregate everything and fill with null if fails or skip?
			// Let's try to aggregate and see.
			resultOrder = append(resultOrder, colName)
			// We don't know the result type yet, assuming Float64 for numeric aggregations like Mean/Sum
			// For Min/Max it could be same type.
			// Let's assume Float64 for now for Mean/Sum.
			resultCols[colName], _ = collection.NewFloat64SeriesFromData(make([]float64, numGroups), nil)
		}
	}

	for i, key := range sortedKeys {
		indices := gb.groups[key]

		// Set grouping column values
		// We need to parse the key back or take from first row. Taking from first row is safer for types.
		firstIdx := indices[0]
		for _, colName := range gb.colNames {
			val, _ := gb.df.Columns[colName].At(firstIdx)
			// We are forcing StringSeries for grouping cols above, so convert to string
			resultCols[colName].Set(i, fmt.Sprintf("%v", val))
		}

		// Calculate aggregation for other columns
		for _, colName := range resultOrder {
			isGroupingCol := false
			for _, gCol := range gb.colNames {
				if colName == gCol {
					isGroupingCol = true
					break
				}
			}
			if isGroupingCol {
				continue
			}

			// Extract series for this group
			// Optimization: Avoid full Slice, just iterate indices
			// But Series interface doesn't support random access iterator easily without Slice.
			// Let's use Slice for correctness first.
			// We need a Slice method on Series that takes indices?
			// We implemented Slice on DataFrame, let's use that logic or just manually extract.

			originalSeries := gb.df.Columns[colName]
			// Create a temporary series for the group
			// This is inefficient, but works.
			groupSeries := collection.NewSeriesOfTypeWithSize(originalSeries.DType(), len(indices))
			for k, idx := range indices {
				val, _ := originalSeries.At(idx)
				if originalSeries.IsNull(idx) {
					groupSeries.SetNull(k)
				} else {
					groupSeries.Set(k, val)
				}
			}

			val, err := aggFunc(groupSeries)
			if err != nil {
				// If aggregation fails (e.g. mean of strings), set to null
				resultCols[colName].SetNull(i)
			} else {
				resultCols[colName].Set(i, val)
			}
		}
	}

	// Construct DataFrame
	// We need to set the Index to 0..n-1
	index := make([]string, numGroups)
	for i := 0; i < numGroups; i++ {
		index[i] = fmt.Sprintf("%d", i)
	}

	return &DataFrame{
		Columns:     resultCols,
		ColumnOrder: resultOrder,
		Index:       index,
	}, nil
}

// Mean computes the mean of each group.
func (gb *GroupBy) Mean() (*DataFrame, error) {
	return gb.aggregate(func(s collection.Series) (any, error) {
		// Check if series is numeric
		// This requires type switching or helper in Series
		// For now, let's assume Float64Series or try to convert.

		// We can iterate and sum.
		sum := 0.0
		count := 0
		n := s.Len()
		for i := 0; i < n; i++ {
			if !s.IsNull(i) {
				val, _ := s.At(i)
				switch v := val.(type) {
				case float64:
					sum += v
					count++
				case int:
					sum += float64(v)
					count++
				case int64:
					sum += float64(v)
					count++
				default:
					return nil, fmt.Errorf("non-numeric type")
				}
			}
		}
		if count == 0 {
			return nil, nil // Null result
		}
		return sum / float64(count), nil
	})
}

// Sum computes the sum of each group.
func (gb *GroupBy) Sum() (*DataFrame, error) {
	return gb.aggregate(func(s collection.Series) (any, error) {
		sum := 0.0
		count := 0
		n := s.Len()
		for i := 0; i < n; i++ {
			if !s.IsNull(i) {
				val, _ := s.At(i)
				switch v := val.(type) {
				case float64:
					sum += v
					count++
				case int:
					sum += float64(v)
					count++
				case int64:
					sum += float64(v)
					count++
				default:
					return nil, fmt.Errorf("non-numeric type")
				}
			}
		}
		if count == 0 {
			return 0.0, nil // Return 0 for empty sum? Or null? Pandas returns 0 usually.
		}
		return sum, nil
	})
}

// Min computes the minimum of each group.
func (gb *GroupBy) Min() (*DataFrame, error) {
	return gb.aggregate(func(s collection.Series) (any, error) {
		var minVal float64
		first := true
		n := s.Len()
		for i := 0; i < n; i++ {
			if !s.IsNull(i) {
				val, _ := s.At(i)
				var fVal float64
				switch v := val.(type) {
				case float64:
					fVal = v
				case int:
					fVal = float64(v)
				case int64:
					fVal = float64(v)
				default:
					return nil, fmt.Errorf("non-numeric type")
				}

				if first {
					minVal = fVal
					first = false
				} else {
					minVal = math.Min(minVal, fVal)
				}
			}
		}
		if first {
			return nil, nil
		}
		return minVal, nil
	})
}

// Max computes the maximum of each group.
func (gb *GroupBy) Max() (*DataFrame, error) {
	return gb.aggregate(func(s collection.Series) (any, error) {
		var maxVal float64
		first := true
		n := s.Len()
		for i := 0; i < n; i++ {
			if !s.IsNull(i) {
				val, _ := s.At(i)
				var fVal float64
				switch v := val.(type) {
				case float64:
					fVal = v
				case int:
					fVal = float64(v)
				case int64:
					fVal = float64(v)
				default:
					return nil, fmt.Errorf("non-numeric type")
				}

				if first {
					maxVal = fVal
					first = false
				} else {
					maxVal = math.Max(maxVal, fVal)
				}
			}
		}
		if first {
			return nil, nil
		}
		return maxVal, nil
	})
}

// Apply applies a function to each group and combines the results.
func (gb *GroupBy) Apply(f func(*DataFrame) (*DataFrame, error)) (*DataFrame, error) {
	sortedKeys := gb.getSortedKeys()
	var resultParts []*DataFrame

	for _, key := range sortedKeys {
		indices := gb.groups[key]

		// Create sub-DataFrame for the group
		subDF, err := gb.df.Slice(indices)
		if err != nil {
			return nil, err
		}

		// Apply function
		resDF, err := f(subDF)
		if err != nil {
			return nil, err
		}

		if resDF != nil {
			resultParts = append(resultParts, resDF)
		}
	}

	if len(resultParts) == 0 {
		return nil, nil // Or empty DataFrame
	}

	// Combine results
	// We need to implement a way to concatenate DataFrames.
	// For now, let's manually merge them assuming they have same structure.

	finalDF := resultParts[0]
	// If there are more parts, we need to append them.
	// Since we don't have a Concat/Append method on DataFrame that merges data,
	// we will just return the first one for now or implement a simple merge.

	// Simple merge implementation
	for _, part := range resultParts[1:] {
		// Append columns
		for colName, series := range part.Columns {
			finalSeries := finalDF.Columns[colName]
			// We need Append on Series
			n := series.Len()
			for i := 0; i < n; i++ {
				if series.IsNull(i) {
					finalSeries.AppendNull()
				} else {
					val, _ := series.At(i)
					finalSeries.Append(val)
				}
			}
		}
		// Append index
		finalDF.Index = append(finalDF.Index, part.Index...)
	}

	return finalDF, nil
}
