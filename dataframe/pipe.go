package dataframe

import "errors"

// Pipe applies fn to the DataFrame and returns its result, enabling fluent
// method-chaining of custom operations. It is equivalent to calling fn(df) but
// reads naturally in a pipeline.
//
// This is analogous to df.pipe(fn) in pandas.
//
// Example:
//
//	result, err := df.
//	    Pipe(normalize).
//	    Pipe(addFeatures)
//
//	// where: func normalize(d *dataframe.DataFrame) (*dataframe.DataFrame, error) { ... }
func (df *DataFrame) Pipe(fn func(*DataFrame) (*DataFrame, error)) (*DataFrame, error) {
	if df == nil {
		return nil, errors.New("Pipe: DataFrame is nil")
	}
	if fn == nil {
		return nil, errors.New("Pipe: fn must not be nil")
	}
	return fn(df)
}
