package dataframe

import (
	"fmt"
	"strings"
)

// Composite keys are used wherever several column values have to be collapsed
// into a single map key: GroupBy, PivotTable and the multi-key joins. Joining
// the parts with a printable separator is unsafe, because the separator can also
// occur inside the values themselves: with "_" the two-column key ("a", "b")
// is indistinguishable from the single value "a_b", and ("x", "y_z") collides
// with ("x_y", "z"). The separators below are non-printable bytes that cannot
// appear in a formatted value, which keeps distinct rows in distinct groups.
const (
	// keySep separates the parts of a composite key. \x00 sorts before every
	// printable byte, so sorting encoded keys orders them by their parts.
	keySep = "\x00"

	// keyNull is written in place of a null value so that a null never collides
	// with a non-null value that happens to format identically (for example the
	// literal string "<nil>").
	keyNull = "\x01"
)

// compositeKeyAt builds a composite key from the values of cols at row i. Null
// values are encoded with a distinct sentinel, so rows that are null in the same
// key columns group together but never join a group of non-null values.
func compositeKeyAt(df *DataFrame, cols []string, i int) string {
	if len(cols) == 1 {
		s := df.Columns[cols[0]]
		if s == nil || s.IsNull(i) {
			return keyNull
		}
		v, _ := s.At(i)
		return fmt.Sprintf("%v", v)
	}

	var b strings.Builder
	for k, col := range cols {
		if k > 0 {
			b.WriteString(keySep)
		}
		s := df.Columns[col]
		if s == nil || s.IsNull(i) {
			b.WriteString(keyNull)
			continue
		}
		v, _ := s.At(i)
		fmt.Fprintf(&b, "%v", v)
	}
	return b.String()
}

// compositeKeyAtNonNull is like compositeKeyAt but reports false when any key
// column is null at row i. Join operations use this variant because a null key
// must never match another null key.
func compositeKeyAtNonNull(df *DataFrame, cols []string, i int) (string, bool) {
	var b strings.Builder
	for k, col := range cols {
		s := df.Columns[col]
		if s == nil || s.IsNull(i) {
			return "", false
		}
		if k > 0 {
			b.WriteString(keySep)
		}
		v, _ := s.At(i)
		fmt.Fprintf(&b, "%v", v)
	}
	return b.String(), true
}
