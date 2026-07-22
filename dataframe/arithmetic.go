package dataframe

import (
	"errors"
	"fmt"
	"math"
	"reflect"

	"github.com/apoplexi24/gpandas/utils/collection"
)

// arithOp enumerates the supported element-wise arithmetic operations.
type arithOp int

const (
	opAdd arithOp = iota
	opSub
	opMul
	opDiv
	opPow
)

// cmpOp enumerates the supported element-wise comparison operations.
type cmpOp int

const (
	cmpGt cmpOp = iota
	cmpLt
	cmpGe
	cmpLe
	cmpEq
	cmpNe
)

// Add returns a new Series holding the element-wise sum of columns left and
// right. Both columns must be numeric. If both columns are integer-typed the
// result is an integer Series; otherwise the result is promoted to float64
// (mirroring pandas). A null in either operand yields a null in that position.
//
// The result is a standalone Series and does not modify the DataFrame. Combine
// it with Assign to attach it as a new column:
//
//	sum, err := df.Add("Q1", "Q2")
//	_ = df.Assign("Total", sum)
func (df *DataFrame) Add(left, right string) (collection.Series, error) {
	return df.arithColumns(left, right, opAdd, "Add")
}

// Sub returns a new Series holding the element-wise difference (left - right)
// of the two numeric columns. See Add for type-promotion and null semantics.
func (df *DataFrame) Sub(left, right string) (collection.Series, error) {
	return df.arithColumns(left, right, opSub, "Sub")
}

// Mul returns a new Series holding the element-wise product of the two numeric
// columns. See Add for type-promotion and null semantics.
func (df *DataFrame) Mul(left, right string) (collection.Series, error) {
	return df.arithColumns(left, right, opMul, "Mul")
}

// Div returns a new Series holding the element-wise quotient (left / right) of
// the two numeric columns. Division always yields a float64 Series (true
// division, as in pandas); division by zero produces +Inf, -Inf, or NaN rather
// than an error. A null in either operand yields a null in that position.
func (df *DataFrame) Div(left, right string) (collection.Series, error) {
	return df.arithColumns(left, right, opDiv, "Div")
}

// Pow returns a new Series holding left raised to the power of right,
// element-wise. The result is always a float64 Series. A null in either operand
// yields a null in that position.
func (df *DataFrame) Pow(left, right string) (collection.Series, error) {
	return df.arithColumns(left, right, opPow, "Pow")
}

// AddScalar returns a new Series holding the given column with scalar added to
// every element. The column must be numeric. If the column is integer-typed and
// scalar is a whole number the result stays integer; otherwise it is promoted to
// float64. Nulls are preserved.
func (df *DataFrame) AddScalar(column string, scalar float64) (collection.Series, error) {
	return df.arithScalar(column, scalar, opAdd, "AddScalar")
}

// SubScalar returns a new Series with scalar subtracted from every element of
// the numeric column. See AddScalar for type and null semantics.
func (df *DataFrame) SubScalar(column string, scalar float64) (collection.Series, error) {
	return df.arithScalar(column, scalar, opSub, "SubScalar")
}

// MulScalar returns a new Series with every element of the numeric column
// multiplied by scalar. See AddScalar for type and null semantics.
func (df *DataFrame) MulScalar(column string, scalar float64) (collection.Series, error) {
	return df.arithScalar(column, scalar, opMul, "MulScalar")
}

// DivScalar returns a new Series with every element of the numeric column
// divided by scalar. The result is always a float64 Series; division by zero
// produces +Inf, -Inf, or NaN. Nulls are preserved.
func (df *DataFrame) DivScalar(column string, scalar float64) (collection.Series, error) {
	return df.arithScalar(column, scalar, opDiv, "DivScalar")
}

// PowScalar returns a new Series with every element of the numeric column raised
// to the power of scalar. The result is always a float64 Series. Nulls are
// preserved.
func (df *DataFrame) PowScalar(column string, scalar float64) (collection.Series, error) {
	return df.arithScalar(column, scalar, opPow, "PowScalar")
}

// Gt returns a boolean Series that is true where left > right, element-wise.
// Both columns must be numeric or both must be string. A null in either operand
// yields a null in that position.
func (df *DataFrame) Gt(left, right string) (collection.Series, error) {
	return df.compareColumns(left, right, cmpGt, "Gt")
}

// Lt returns a boolean Series that is true where left < right, element-wise.
// See Gt for operand and null semantics.
func (df *DataFrame) Lt(left, right string) (collection.Series, error) {
	return df.compareColumns(left, right, cmpLt, "Lt")
}

// Ge returns a boolean Series that is true where left >= right, element-wise.
// See Gt for operand and null semantics.
func (df *DataFrame) Ge(left, right string) (collection.Series, error) {
	return df.compareColumns(left, right, cmpGe, "Ge")
}

// Le returns a boolean Series that is true where left <= right, element-wise.
// See Gt for operand and null semantics.
func (df *DataFrame) Le(left, right string) (collection.Series, error) {
	return df.compareColumns(left, right, cmpLe, "Le")
}

// Eq returns a boolean Series that is true where left == right, element-wise.
// Numeric columns are compared numerically (int and float interoperate); other
// types are compared by value. Mismatched non-numeric types compare as not
// equal. A null in either operand yields a null in that position.
func (df *DataFrame) Eq(left, right string) (collection.Series, error) {
	return df.compareColumns(left, right, cmpEq, "Eq")
}

// Ne returns a boolean Series that is true where left != right, element-wise.
// See Eq for comparison and null semantics.
func (df *DataFrame) Ne(left, right string) (collection.Series, error) {
	return df.compareColumns(left, right, cmpNe, "Ne")
}

// GtScalar returns a boolean Series that is true where the column value is
// greater than scalar. The column and scalar must be comparable (both numeric
// or both string). Nulls yield nulls.
func (df *DataFrame) GtScalar(column string, scalar any) (collection.Series, error) {
	return df.compareScalar(column, scalar, cmpGt, "GtScalar")
}

// LtScalar returns a boolean Series that is true where the column value is less
// than scalar. See GtScalar for operand and null semantics.
func (df *DataFrame) LtScalar(column string, scalar any) (collection.Series, error) {
	return df.compareScalar(column, scalar, cmpLt, "LtScalar")
}

// GeScalar returns a boolean Series that is true where the column value is
// greater than or equal to scalar. See GtScalar for operand and null semantics.
func (df *DataFrame) GeScalar(column string, scalar any) (collection.Series, error) {
	return df.compareScalar(column, scalar, cmpGe, "GeScalar")
}

// LeScalar returns a boolean Series that is true where the column value is less
// than or equal to scalar. See GtScalar for operand and null semantics.
func (df *DataFrame) LeScalar(column string, scalar any) (collection.Series, error) {
	return df.compareScalar(column, scalar, cmpLe, "LeScalar")
}

// EqScalar returns a boolean Series that is true where the column value equals
// scalar. Numeric values interoperate; other types compare by value. Nulls yield
// nulls.
func (df *DataFrame) EqScalar(column string, scalar any) (collection.Series, error) {
	return df.compareScalar(column, scalar, cmpEq, "EqScalar")
}

// NeScalar returns a boolean Series that is true where the column value differs
// from scalar. See EqScalar for comparison and null semantics.
func (df *DataFrame) NeScalar(column string, scalar any) (collection.Series, error) {
	return df.compareScalar(column, scalar, cmpNe, "NeScalar")
}

// arithColumns performs an element-wise arithmetic operation between two
// numeric columns and returns the resulting Series.
func (df *DataFrame) arithColumns(left, right string, op arithOp, name string) (collection.Series, error) {
	if df == nil {
		return nil, errors.New(name + ": DataFrame is nil")
	}

	df.RLock()
	defer df.RUnlock()

	ls, ok := df.Columns[left]
	if !ok {
		return nil, fmt.Errorf("%s: column '%s' not found", name, left)
	}
	rs, ok := df.Columns[right]
	if !ok {
		return nil, fmt.Errorf("%s: column '%s' not found", name, right)
	}

	if ls.Len() != rs.Len() {
		return nil, fmt.Errorf("%s: length mismatch: '%s' has %d rows, '%s' has %d", name, left, ls.Len(), right, rs.Len())
	}

	lData, lMask, lInt, err := extractNumeric(ls)
	if err != nil {
		return nil, fmt.Errorf("%s: column '%s': %w", name, left, err)
	}
	rData, rMask, rInt, err := extractNumeric(rs)
	if err != nil {
		return nil, fmt.Errorf("%s: column '%s': %w", name, right, err)
	}

	n := len(lData)
	keepInt := lInt && rInt && (op == opAdd || op == opSub || op == opMul)

	if keepInt {
		out := make([]int64, n)
		mask := make([]bool, n)
		for i := 0; i < n; i++ {
			if lMask[i] || rMask[i] {
				mask[i] = true
				continue
			}
			a, b := int64(lData[i]), int64(rData[i])
			switch op {
			case opAdd:
				out[i] = a + b
			case opSub:
				out[i] = a - b
			case opMul:
				out[i] = a * b
			}
		}
		return collection.NewInt64SeriesFromData(out, mask)
	}

	out := make([]float64, n)
	mask := make([]bool, n)
	for i := 0; i < n; i++ {
		if lMask[i] || rMask[i] {
			mask[i] = true
			continue
		}
		out[i] = applyArith(lData[i], rData[i], op)
	}
	return collection.NewFloat64SeriesFromData(out, mask)
}

// arithScalar performs an element-wise arithmetic operation between a numeric
// column and a scalar and returns the resulting Series.
func (df *DataFrame) arithScalar(column string, scalar float64, op arithOp, name string) (collection.Series, error) {
	if df == nil {
		return nil, errors.New(name + ": DataFrame is nil")
	}

	df.RLock()
	defer df.RUnlock()

	s, ok := df.Columns[column]
	if !ok {
		return nil, fmt.Errorf("%s: column '%s' not found", name, column)
	}

	data, mask, isInt, err := extractNumeric(s)
	if err != nil {
		return nil, fmt.Errorf("%s: column '%s': %w", name, column, err)
	}

	n := len(data)
	scalarWhole := scalar == math.Trunc(scalar) && !math.IsInf(scalar, 0)
	keepInt := isInt && scalarWhole && (op == opAdd || op == opSub || op == opMul)

	if keepInt {
		out := make([]int64, n)
		outMask := make([]bool, n)
		sc := int64(scalar)
		for i := 0; i < n; i++ {
			if mask[i] {
				outMask[i] = true
				continue
			}
			a := int64(data[i])
			switch op {
			case opAdd:
				out[i] = a + sc
			case opSub:
				out[i] = a - sc
			case opMul:
				out[i] = a * sc
			}
		}
		return collection.NewInt64SeriesFromData(out, outMask)
	}

	out := make([]float64, n)
	outMask := make([]bool, n)
	for i := 0; i < n; i++ {
		if mask[i] {
			outMask[i] = true
			continue
		}
		out[i] = applyArith(data[i], scalar, op)
	}
	return collection.NewFloat64SeriesFromData(out, outMask)
}

// applyArith applies the arithmetic operation to two float64 operands.
func applyArith(a, b float64, op arithOp) float64 {
	switch op {
	case opAdd:
		return a + b
	case opSub:
		return a - b
	case opMul:
		return a * b
	case opDiv:
		return a / b
	case opPow:
		return math.Pow(a, b)
	default:
		return math.NaN()
	}
}

// compareColumns performs an element-wise comparison between two columns and
// returns a boolean Series (null where either operand is null).
func (df *DataFrame) compareColumns(left, right string, op cmpOp, name string) (collection.Series, error) {
	if df == nil {
		return nil, errors.New(name + ": DataFrame is nil")
	}

	df.RLock()
	defer df.RUnlock()

	ls, ok := df.Columns[left]
	if !ok {
		return nil, fmt.Errorf("%s: column '%s' not found", name, left)
	}
	rs, ok := df.Columns[right]
	if !ok {
		return nil, fmt.Errorf("%s: column '%s' not found", name, right)
	}

	if ls.Len() != rs.Len() {
		return nil, fmt.Errorf("%s: length mismatch: '%s' has %d rows, '%s' has %d", name, left, ls.Len(), right, rs.Len())
	}

	lVals := ls.ValuesCopy()
	rVals := rs.ValuesCopy()

	n := len(lVals)
	out := make([]bool, n)
	mask := make([]bool, n)
	for i := 0; i < n; i++ {
		if lVals[i] == nil || rVals[i] == nil {
			mask[i] = true
			continue
		}
		res, err := evalCompare(lVals[i], rVals[i], op)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		out[i] = res
	}
	return collection.NewBoolSeriesFromData(out, mask)
}

// compareScalar performs an element-wise comparison between a column and a
// scalar and returns a boolean Series (null where the column value is null).
func (df *DataFrame) compareScalar(column string, scalar any, op cmpOp, name string) (collection.Series, error) {
	if df == nil {
		return nil, errors.New(name + ": DataFrame is nil")
	}
	if scalar == nil {
		return nil, fmt.Errorf("%s: scalar must not be nil", name)
	}

	df.RLock()
	defer df.RUnlock()

	s, ok := df.Columns[column]
	if !ok {
		return nil, fmt.Errorf("%s: column '%s' not found", name, column)
	}

	vals := s.ValuesCopy()
	n := len(vals)
	out := make([]bool, n)
	mask := make([]bool, n)
	for i := 0; i < n; i++ {
		if vals[i] == nil {
			mask[i] = true
			continue
		}
		res, err := evalCompare(vals[i], scalar, op)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		out[i] = res
	}
	return collection.NewBoolSeriesFromData(out, mask)
}

// evalCompare compares two non-nil values according to op. Numeric values are
// compared numerically regardless of concrete integer/float type. Equality and
// inequality fall back to value comparison for strings and bools; ordering
// operators additionally support strings. Incompatible types for ordering
// return an error, while equality of mismatched types is simply false.
func evalCompare(a, b any, op cmpOp) (bool, error) {
	af, aok := toFloat64(a)
	bf, bok := toFloat64(b)

	if aok && bok {
		switch op {
		case cmpGt:
			return af > bf, nil
		case cmpLt:
			return af < bf, nil
		case cmpGe:
			return af >= bf, nil
		case cmpLe:
			return af <= bf, nil
		case cmpEq:
			return af == bf, nil
		case cmpNe:
			return af != bf, nil
		}
	}

	// Non-numeric (or mixed) operands.
	as, asok := a.(string)
	bs, bsok := b.(string)

	switch op {
	case cmpEq:
		return valuesComparable(a, b) && a == b, nil
	case cmpNe:
		return !(valuesComparable(a, b) && a == b), nil
	case cmpGt, cmpLt, cmpGe, cmpLe:
		if asok && bsok {
			switch op {
			case cmpGt:
				return as > bs, nil
			case cmpLt:
				return as < bs, nil
			case cmpGe:
				return as >= bs, nil
			case cmpLe:
				return as <= bs, nil
			}
		}
		return false, fmt.Errorf("cannot order-compare %T and %T", a, b)
	}
	return false, fmt.Errorf("unsupported comparison")
}

// valuesComparable reports whether a and b share a comparable concrete type,
// guarding the == used for equality comparison of non-numeric values.
func valuesComparable(a, b any) bool {
	switch a.(type) {
	case string:
		_, ok := b.(string)
		return ok
	case bool:
		_, ok := b.(bool)
		return ok
	default:
		return false
	}
}

// extractNumeric returns the values of a Series as a []float64 slice together
// with its null mask and whether the source Series is integer-typed. It uses
// fast paths for the typed numeric Series and falls back to element-wise
// conversion for other Series types. A non-null, non-numeric value produces an
// error.
func extractNumeric(s collection.Series) (data []float64, mask []bool, isInt bool, err error) {
	switch ts := s.(type) {
	case *collection.Float64Series:
		return ts.Float64Values(), ts.MaskCopy(), false, nil
	case *collection.Int64Series:
		raw := ts.Int64Values()
		out := make([]float64, len(raw))
		for i, v := range raw {
			out[i] = float64(v)
		}
		return out, ts.MaskCopy(), true, nil
	default:
		vals := s.ValuesCopy()
		out := make([]float64, len(vals))
		m := make([]bool, len(vals))
		allInt := true
		hasValue := false
		for i, v := range vals {
			if v == nil {
				m[i] = true
				continue
			}
			f, ok := toFloat64(v)
			if !ok {
				return nil, nil, false, fmt.Errorf("non-numeric value %v (%T) at row %d", v, v, i)
			}
			hasValue = true
			if normalizedKind(v) != reflect.Int64 {
				allInt = false
			}
			out[i] = f
		}
		return out, m, hasValue && allInt, nil
	}
}
