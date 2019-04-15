package sqlavro

import "errors"

var (
	// ErrExpectRecordSchema -
	ErrExpectRecordSchema = errors.New("ErrExpectRecordSchema")
	// ErrUnsupportedTypeForCriterion -
	ErrUnsupportedTypeForCriterion = errors.New("ErrUnsupportedTypeForCriterion")
	// ErrCannotIgnoreOrder -
	ErrCannotIgnoreOrder = errors.New("ErrCannotIgnoreOrder")
	// ErrUnsupportedTypeForSQL -
	ErrUnsupportedTypeForSQL = errors.New("ErrUnsupportedTypeForSQL")
)
