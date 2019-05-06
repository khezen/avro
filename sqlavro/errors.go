package sqlavro

import "errors"

var (
	// ErrExpectRecordSchema -
	ErrExpectRecordSchema = errors.New("ErrExpectRecordSchema")
	// ErrUnsupportedTypeForCriterion -
	ErrUnsupportedTypeForCriterion = errors.New("ErrUnsupportedTypeForCriterion")
	// ErrCriterionUnknownField -
	ErrCriterionUnknownField = errors.New("ErrCriterionUnknownField")
	// ErrCannotIgnoreOrder -
	ErrCannotIgnoreOrder = errors.New("ErrCannotIgnoreOrder")
	// ErrUnsupportedTypeForSQL -
	ErrUnsupportedTypeForSQL = errors.New("ErrUnsupportedTypeForSQL")
)
