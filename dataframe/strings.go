package dataframe

import (
	"errors"
	"fmt"

	"github.com/apoplexi24/gpandas/utils/collection"
)

// Str returns a string accessor for a string column, enabling vectorized string
// operations like Lower, Upper, Contains, Replace, and Len. The returned Series
// can be added back to a DataFrame with Assign.
//
// An error is returned if the column does not exist or is not a string column.
//
// Example:
//
//	acc, _ := df.Str("Name")
//	df.Assign("name_lower", acc.Lower())
func (df *DataFrame) Str(column string) (*collection.StrAccessor, error) {
	if df == nil {
		return nil, errors.New("Str: DataFrame is nil")
	}

	df.RLock()
	defer df.RUnlock()

	series, ok := df.Columns[column]
	if !ok {
		return nil, fmt.Errorf("Str: column '%s' not found", column)
	}

	strSeries, ok := series.(*collection.StringSeries)
	if !ok {
		return nil, fmt.Errorf("Str: column '%s' is not a string column (dtype %s)", column, dtypeName(series.DType()))
	}

	return strSeries.Str(), nil
}
