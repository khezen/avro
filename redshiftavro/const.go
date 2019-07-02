package redshiftavro

const (
	// SortStyleCompound -
	SortStyleCompound SortStyle = "COMPOUND"
	// SortStyleInterleaved -
	SortStyleInterleaved SortStyle = "INTERLEAVED"
	// DistStyleAuto -
	DistStyleAuto DistStyle = "AUTO"
	// DistStyleEven -
	DistStyleEven DistStyle = "EVEN"
	// DistStyleKey -
	DistStyleKey DistStyle = "KEY"
	// DistStyleAll -
	DistStyleAll DistStyle = "ALL"
)

const (
	// SmallInt -
	SmallInt RedshiftType = "SMALLINT"
	// Integer -
	Integer RedshiftType = "INTEGER"
	// BigInt -
	BigInt RedshiftType = "BIGINT"
	// Decimal -
	Decimal RedshiftType = "DECIMAL"
	// Real -
	Real RedshiftType = "REAL"
	// Double -
	Double RedshiftType = "DOUBLE"
	// Boolean -
	Boolean RedshiftType = "BOOLEAN"
	// Char -
	Char RedshiftType = "CHAR"
	// VarChar -
	VarChar RedshiftType = "VARCHAR"
	// Date -
	Date RedshiftType = "Date"
	// Timestamp -
	Timestamp RedshiftType = "TIMESTAMP"
	// Timestamptz -
	Timestamptz RedshiftType = "TIMESTAMPTZ"
)
