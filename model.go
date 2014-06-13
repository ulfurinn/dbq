package dbq

import "reflect"

type Table struct {
	DBName string
	Type   reflect.Type
}

func (t Table) Alias(a string) AliasSpec {
	return Alias(Identifier{t.Name()}, a)
}

func (t Table) Name() string {
	return t.DBName
}

func (t Table) Col(c string) Expr {
	return Expr{Col{table: t, column: Identifier{c}}}
}
