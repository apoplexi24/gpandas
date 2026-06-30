package collection

import (
	"strings"
	"unicode/utf8"
)

// StrAccessor provides vectorized string operations over a StringSeries,
// analogous to the pandas .str accessor. Each method returns a new Series and
// preserves null values (a null input maps to a null output).
type StrAccessor struct {
	s *StringSeries
}

// Str returns a string accessor for the StringSeries, enabling vectorized
// string operations like Lower, Upper, Contains, Replace, and Len.
//
// Example:
//
//	lowered := series.Str().Lower()
func (s *StringSeries) Str() *StrAccessor {
	return &StrAccessor{s: s}
}

// mapString applies fn to each non-null string value and returns a new
// StringSeries with null positions preserved.
func (a *StrAccessor) mapString(fn func(string) string) *StringSeries {
	n := a.s.Len()
	data := make([]string, n)
	mask := make([]bool, n)
	for i := 0; i < n; i++ {
		if a.s.IsNull(i) {
			mask[i] = true
			continue
		}
		v, _ := a.s.StringValue(i)
		data[i] = fn(v)
	}
	out, _ := NewStringSeriesFromData(data, mask)
	return out
}

// mapBool applies fn to each non-null string value and returns a new BoolSeries
// with null positions preserved.
func (a *StrAccessor) mapBool(fn func(string) bool) *BoolSeries {
	n := a.s.Len()
	data := make([]bool, n)
	mask := make([]bool, n)
	for i := 0; i < n; i++ {
		if a.s.IsNull(i) {
			mask[i] = true
			continue
		}
		v, _ := a.s.StringValue(i)
		data[i] = fn(v)
	}
	out, _ := NewBoolSeriesFromData(data, mask)
	return out
}

// Lower returns a StringSeries with all values lower-cased.
func (a *StrAccessor) Lower() *StringSeries {
	return a.mapString(strings.ToLower)
}

// Upper returns a StringSeries with all values upper-cased.
func (a *StrAccessor) Upper() *StringSeries {
	return a.mapString(strings.ToUpper)
}

// Strip returns a StringSeries with leading and trailing whitespace removed.
func (a *StrAccessor) Strip() *StringSeries {
	return a.mapString(strings.TrimSpace)
}

// Title returns a StringSeries with each value title-cased.
func (a *StrAccessor) Title() *StringSeries {
	return a.mapString(strings.Title)
}

// Replace returns a StringSeries with all occurrences of old replaced by new.
func (a *StrAccessor) Replace(old, new string) *StringSeries {
	return a.mapString(func(v string) string {
		return strings.ReplaceAll(v, old, new)
	})
}

// Contains returns a BoolSeries indicating whether each value contains substr.
func (a *StrAccessor) Contains(substr string) *BoolSeries {
	return a.mapBool(func(v string) bool {
		return strings.Contains(v, substr)
	})
}

// StartsWith returns a BoolSeries indicating whether each value has the prefix.
func (a *StrAccessor) StartsWith(prefix string) *BoolSeries {
	return a.mapBool(func(v string) bool {
		return strings.HasPrefix(v, prefix)
	})
}

// EndsWith returns a BoolSeries indicating whether each value has the suffix.
func (a *StrAccessor) EndsWith(suffix string) *BoolSeries {
	return a.mapBool(func(v string) bool {
		return strings.HasSuffix(v, suffix)
	})
}

// Len returns an Int64Series with the rune length of each value.
func (a *StrAccessor) Len() *Int64Series {
	n := a.s.Len()
	data := make([]int64, n)
	mask := make([]bool, n)
	for i := 0; i < n; i++ {
		if a.s.IsNull(i) {
			mask[i] = true
			continue
		}
		v, _ := a.s.StringValue(i)
		data[i] = int64(utf8.RuneCountInString(v))
	}
	out, _ := NewInt64SeriesFromData(data, mask)
	return out
}

// Split splits each value by sep and returns a slice of string slices. Null
// values produce a nil entry at the corresponding position.
func (a *StrAccessor) Split(sep string) [][]string {
	n := a.s.Len()
	out := make([][]string, n)
	for i := 0; i < n; i++ {
		if a.s.IsNull(i) {
			out[i] = nil
			continue
		}
		v, _ := a.s.StringValue(i)
		out[i] = strings.Split(v, sep)
	}
	return out
}
