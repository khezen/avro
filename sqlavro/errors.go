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
	// ErrQueryConfigMissingDB -
	ErrQueryConfigMissingDB = errors.New("ErrQueryConfigMissingDB")
	// ErrQueryConfigMissingDBName -
	ErrQueryConfigMissingDBName = errors.New("ErrQueryConfigMissingDBName")
	// ErrQueryConfigMissingSchema -
	ErrQueryConfigMissingSchema = errors.New("ErrQueryConfigMissingSchema")
	// ErrUnsupportedOutput - query doesn't supprot this output
	ErrUnsupportedOutput = errors.New("ErrUnsupportedOutput")
)
