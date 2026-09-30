package domain

import "strings"

// Problem is one rule a value breaks, named by the field at fault.
type Problem struct {
	Field   string
	Message string
	// Err is an optional sentinel for the problem, so a caller can match it with
	// errors.Is without parsing a message.
	Err error
}

// ValidationError is every problem found in one value, so a caller fixes a
// request in one pass rather than one rule per attempt.
type ValidationError struct {
	Problems []Problem
}

// Error implements error.
func (e *ValidationError) Error() string {
	var b strings.Builder
	b.WriteString("invalid value:")
	for _, p := range e.Problems {
		b.WriteString("\n  ")
		b.WriteString(p.Field)
		b.WriteString(": ")
		b.WriteString(p.Message)
	}
	return b.String()
}

// Unwrap returns the sentinels of the problems that carry one.
func (e *ValidationError) Unwrap() []error {
	var errs []error
	for _, p := range e.Problems {
		if p.Err != nil {
			errs = append(errs, p.Err)
		}
	}
	return errs
}
