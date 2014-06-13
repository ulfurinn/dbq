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

type Tabular interface {
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

func (id Identifier) String() string {
	return id.id
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
		return AliasSpec{expr: Identifier{expr}, alias: a}
	case Expr:
		return AliasSpec{expr: Subexpr{expr}, alias: a}
	default:
		panic(fmt.Sprintf("%v does not implement Expr", expr))
	}
}

func (a AliasSpec) String() string {
	return a.expr.String() + " AS " + a.alias
}
