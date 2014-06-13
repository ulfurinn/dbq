package dbq

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

type Dbq struct {
	d  dialect
	db *sql.DB
}

type Expr interface {
	String() string
}

type Named interface {
	Name() string
}

type Tabular interface {
	Named
	Col(c string) Expr
}

type AliasSpec struct {
	expr  Expr
	alias string
}

type Identifier struct {
	id string
}

type Subexpr struct {
	expr Expr
}

type Col struct {
	table  Tabular
	column Identifier
}

func (id Identifier) String() string {
	return id.id
}

func (id Identifier) Name() string {
	return id.id
}

func (id Identifier) Col(c string) Expr {
	return Col{table: id, column: Identifier{c}}
}

func (e Subexpr) String() string {
	return "(" + e.expr.String() + ")"
}

func New(db *sql.DB, d dialect) *Dbq {
	return &Dbq{
		d:  d,
		db: db,
	}
}

func (q *Dbq) Select(selectSpec ...interface{}) *SelectExpr {
	stmt := &SelectExpr{q: q}
	return stmt.parseSelectSpec(selectSpec...)
}

func Alias(expr interface{}, a string) AliasSpec {
	switch expr := expr.(type) {
	case string:
		return AliasSpec{expr: Identifier{id: expr}, alias: a}
	case Expr:
		return AliasSpec{expr: Subexpr{expr}, alias: a}
	default:
		panic(fmt.Sprintf("%v does not implement Expr", expr))
	}
}

func (a AliasSpec) String() string {
	return a.expr.String() + " AS " + a.alias
}

func (a AliasSpec) Name() string {
	return a.alias
}

func (a AliasSpec) Col(c string) Expr {
	return Col{table: a, column: Identifier{c}}
}

func (c Col) String() string {
	return c.table.Name() + "." + c.column.Name()
}
