package domain

import (
	"fmt"
	"strings"
)

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

func (e *ValidationError) add(field, message string, sentinel error) {
	e.Problems = append(e.Problems, Problem{Field: field, Message: message, Err: sentinel})
}

// idemKey checks a producer's idempotency key: present, and within the bound the schema
// holds. whose is the possessive that says what the key is the exactly-once identity of
// ("the broadcast's"), for the message.
func (e *ValidationError) idemKey(whose, key string) {
	switch {
	case key == "":
		e.add("IdemKey", fmt.Sprintf("is required: it is %s exactly-once identity", whose), ErrMissingIdem)
	case len(key) > MaxIdemKeyBytes:
		e.add("IdemKey", fmt.Sprintf("is %d bytes, the limit is %d", len(key), MaxIdemKeyBytes), nil)
	}
}

// err returns e, or nil when no problem was found.
func (e *ValidationError) err() error {
	if len(e.Problems) == 0 {
		return nil
	}
	return e
}
