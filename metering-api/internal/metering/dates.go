package metering

import (
	"fmt"
	"time"
)

// Date is a calendar day in UTC, written as 2006-01-02.
type Date struct{ t time.Time }

// ParseDate parses YYYY-MM-DD.
func ParseDate(s string) (Date, error) {
	t, err := time.Parse(time.DateOnly, s)
	if err != nil {
		return Date{}, fmt.Errorf("%q is not a date (want YYYY-MM-DD)", s)
	}
	return Date{t}, nil
}

// DateOf returns the UTC calendar day of t.
func DateOf(t time.Time) Date {
	y, m, d := t.UTC().Date()
	return Date{time.Date(y, m, d, 0, 0, 0, 0, time.UTC)}
}

// Time is midnight UTC at the start of the day.
func (d Date) Time() time.Time { return d.t }

// AddDays returns the date n days later.
func (d Date) AddDays(n int) Date { return Date{d.t.AddDate(0, 0, n)} }

func (d Date) String() string { return d.t.Format(time.DateOnly) }

// MarshalText implements encoding.TextMarshaler.
func (d Date) MarshalText() ([]byte, error) { return []byte(d.String()), nil }

// UnmarshalText implements encoding.TextUnmarshaler.
func (d *Date) UnmarshalText(b []byte) error {
	parsed, err := ParseDate(string(b))
	if err != nil {
		return err
	}
	*d = parsed
	return nil
}

// Month is a billing month, written as 2006-01.
type Month struct{ start time.Time }

// ParseMonth parses YYYY-MM.
func ParseMonth(s string) (Month, error) {
	t, err := time.Parse("2006-01", s)
	if err != nil {
		return Month{}, fmt.Errorf("%q is not a month (want YYYY-MM)", s)
	}
	return Month{t}, nil
}

// MonthOf returns the month containing t (in UTC).
func MonthOf(t time.Time) Month {
	y, m, _ := t.UTC().Date()
	return Month{time.Date(y, m, 1, 0, 0, 0, 0, time.UTC)}
}

// Start is the first instant of the month.
func (m Month) Start() time.Time { return m.start }

// End is the first instant of the next month.
func (m Month) End() time.Time { return m.start.AddDate(0, 1, 0) }

// Prev is the month before.
func (m Month) Prev() Month { return Month{m.start.AddDate(0, -1, 0)} }

func (m Month) String() string { return m.start.Format("2006-01") }

// MarshalText implements encoding.TextMarshaler.
func (m Month) MarshalText() ([]byte, error) { return []byte(m.String()), nil }

// UnmarshalText implements encoding.TextUnmarshaler.
func (m *Month) UnmarshalText(b []byte) error {
	parsed, err := ParseMonth(string(b))
	if err != nil {
		return err
	}
	*m = parsed
	return nil
}
