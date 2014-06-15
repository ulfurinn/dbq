package dbq

type Dialect interface {
	SQL(e Expression, v Args) (sql string, values []interface{}, err error)
	SQLString(e Expression) (sql string, err error)
}

type Ctx interface {
	Select(*SelectExpr) (string, error)
	Column(*ColumnExpr) (string, error)
	BinaryOp(*BinaryOp) (string, error)
	Alias(*AliasExpr) (string, error)
	StaticPlaceholder(interface{}) (string, error)
	DynamicPlaceholder(*Binding) (string, error)
}
