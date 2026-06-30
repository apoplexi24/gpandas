package collection

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"
)

// DateTimeSeries is a series of time.Time values with null support.
type DateTimeSeries struct {
	mu   sync.RWMutex
	data []time.Time
	mask []bool // true = null
}

// NewDateTimeSeries creates a new empty DateTimeSeries with optional capacity.
func NewDateTimeSeries(capacity int) *DateTimeSeries {
	return &DateTimeSeries{
		data: make([]time.Time, 0, capacity),
		mask: make([]bool, 0, capacity),
	}
}

// NewDateTimeSeriesFromData creates a DateTimeSeries from values and mask.
func NewDateTimeSeriesFromData(data []time.Time, mask []bool) (*DateTimeSeries, error) {
	if mask != nil && len(data) != len(mask) {
		return nil, errors.New("data and mask length mismatch")
	}
	dataCopy := make([]time.Time, len(data))
	copy(dataCopy, data)
	var maskCopy []bool
	if mask != nil {
		maskCopy = make([]bool, len(mask))
		copy(maskCopy, mask)
	} else {
		maskCopy = make([]bool, len(data))
	}
	return &DateTimeSeries{data: dataCopy, mask: maskCopy}, nil
}

func (s *DateTimeSeries) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

func (s *DateTimeSeries) DType() reflect.Type {
	return reflect.TypeOf(time.Time{})
}

func (s *DateTimeSeries) At(i int) (any, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if i < 0 || i >= len(s.data) {
		return nil, errors.New("index out of range")
	}
	if s.mask[i] {
		return nil, nil
	}
	return s.data[i], nil
}

func (s *DateTimeSeries) IsNull(i int) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if i < 0 || i >= len(s.mask) {
		return true
	}
	return s.mask[i]
}

func (s *DateTimeSeries) NullCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	count := 0
	for _, isNull := range s.mask {
		if isNull {
			count++
		}
	}
	return count
}

func (s *DateTimeSeries) Set(i int, v any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if i < 0 || i >= len(s.data) {
		return errors.New("index out of range")
	}
	if v == nil {
		s.mask[i] = true
		s.data[i] = time.Time{}
		return nil
	}
	t, ok := v.(time.Time)
	if !ok {
		return fmt.Errorf("type mismatch: expected time.Time, got %T", v)
	}
	s.data[i] = t
	s.mask[i] = false
	return nil
}

func (s *DateTimeSeries) SetNull(i int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if i < 0 || i >= len(s.data) {
		return errors.New("index out of range")
	}
	s.mask[i] = true
	s.data[i] = time.Time{}
	return nil
}

func (s *DateTimeSeries) Append(v any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v == nil {
		s.data = append(s.data, time.Time{})
		s.mask = append(s.mask, true)
		return nil
	}
	t, ok := v.(time.Time)
	if !ok {
		return fmt.Errorf("type mismatch: expected time.Time, got %T", v)
	}
	s.data = append(s.data, t)
	s.mask = append(s.mask, false)
	return nil
}

func (s *DateTimeSeries) AppendNull() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = append(s.data, time.Time{})
	s.mask = append(s.mask, true)
}

func (s *DateTimeSeries) ValuesCopy() []any {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]any, len(s.data))
	for i, v := range s.data {
		if s.mask[i] {
			out[i] = nil
		} else {
			out[i] = v
		}
	}
	return out
}

func (s *DateTimeSeries) MaskCopy() []bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]bool, len(s.mask))
	copy(out, s.mask)
	return out
}

func (s *DateTimeSeries) Slice(start, end int) (Series, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if start < 0 || end > len(s.data) || start > end {
		return nil, errors.New("invalid slice bounds")
	}
	newData := make([]time.Time, end-start)
	copy(newData, s.data[start:end])
	newMask := make([]bool, end-start)
	copy(newMask, s.mask[start:end])
	return &DateTimeSeries{data: newData, mask: newMask}, nil
}

// TimeValue returns the raw time.Time value at index i (ignores null mask).
func (s *DateTimeSeries) TimeValue(i int) (time.Time, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if i < 0 || i >= len(s.data) {
		return time.Time{}, errors.New("index out of range")
	}
	return s.data[i], nil
}

// Dt returns a datetime accessor for extracting components like year and month.
func (s *DateTimeSeries) Dt() *DtAccessor {
	return &DtAccessor{s: s}
}

// DtAccessor provides vectorized datetime component extraction, analogous to the
// pandas .dt accessor. Each method returns a new Series, preserving nulls.
type DtAccessor struct {
	s *DateTimeSeries
}

func (a *DtAccessor) mapInt(fn func(time.Time) int64) *Int64Series {
	n := a.s.Len()
	data := make([]int64, n)
	mask := make([]bool, n)
	for i := 0; i < n; i++ {
		if a.s.IsNull(i) {
			mask[i] = true
			continue
		}
		t, _ := a.s.TimeValue(i)
		data[i] = fn(t)
	}
	out, _ := NewInt64SeriesFromData(data, mask)
	return out
}

// Year returns the year of each value as an Int64Series.
func (a *DtAccessor) Year() *Int64Series {
	return a.mapInt(func(t time.Time) int64 { return int64(t.Year()) })
}

// Month returns the month (1-12) of each value.
func (a *DtAccessor) Month() *Int64Series {
	return a.mapInt(func(t time.Time) int64 { return int64(t.Month()) })
}

// Day returns the day of the month of each value.
func (a *DtAccessor) Day() *Int64Series {
	return a.mapInt(func(t time.Time) int64 { return int64(t.Day()) })
}

// Hour returns the hour (0-23) of each value.
func (a *DtAccessor) Hour() *Int64Series {
	return a.mapInt(func(t time.Time) int64 { return int64(t.Hour()) })
}

// Minute returns the minute of each value.
func (a *DtAccessor) Minute() *Int64Series {
	return a.mapInt(func(t time.Time) int64 { return int64(t.Minute()) })
}

// Second returns the second of each value.
func (a *DtAccessor) Second() *Int64Series {
	return a.mapInt(func(t time.Time) int64 { return int64(t.Second()) })
}

// Weekday returns the day of week (0=Sunday .. 6=Saturday) of each value.
func (a *DtAccessor) Weekday() *Int64Series {
	return a.mapInt(func(t time.Time) int64 { return int64(t.Weekday()) })
}

// Date returns a StringSeries with each value formatted as "2006-01-02".
func (a *DtAccessor) Date() *StringSeries {
	n := a.s.Len()
	data := make([]string, n)
	mask := make([]bool, n)
	for i := 0; i < n; i++ {
		if a.s.IsNull(i) {
			mask[i] = true
			continue
		}
		t, _ := a.s.TimeValue(i)
		data[i] = t.Format("2006-01-02")
	}
	out, _ := NewStringSeriesFromData(data, mask)
	return out
}
