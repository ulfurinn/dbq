package dbq

type Dialect interface {
	SQL(e Expression, v Args) (sql string, values []interface{}, err error) // serializes an Expression to string and collects all placeholder bindings, explicit and implicit
	SQLString(e Expression) (sql string, err error)
}

/*
Ctx is the interface used by Nodes to return (possibly dialect-specific) SQL. It provides a mapping between a dbq type and an SQL subexpression.
Ctx is used because a query may contain shared state that needs to be accessible by all Nodes (like lists of known placeholders).
*/
type Ctx interface {
	Select(*SelectExpr) (string, error)
	Column(*ColumnExpr) (string, error)
	BinaryOp(*BinaryOp) (string, error)
	Alias(*AliasExpr) (string, error)
	StaticPlaceholder(interface{}) (string, error)
	DynamicPlaceholder(*Binding) (string, error)
	Join(*JoinExpr) (string, error)
	JoinCondition(*JoinCondition) (string, error)
	In(*InExpr) (string, error)
	BindValue(*Binding) (interface{}, bool)
	Cast(*CastExpr) (string, error)
	Func(*FuncExpr) (string, error)
	AggFunc(*AggFuncExpr) (string, error)
}
