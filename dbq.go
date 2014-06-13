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

type Node interface {
	String() string
}

type Expr struct {
	Node
}

func (e Expr) String() string {
	return e.Node.String()
}

func (e Expr) Eq(other Expr) Expr {
	return Binary(e, "=", other)
}

type Named interface {
	Name() string
}

type Tabular interface {
	Named
	Col(c string) Expr
}

type AliasSpec struct {
	expr  Node
	alias string
}

type Identifier struct {
	id string
}

type Subexpr struct {
	expr Node
}

type Col struct {
	table  Tabular
	column Identifier
}

type BinaryOp struct {
	a, b     Expr
	operator string
}

func (id Identifier) String() string {
	return id.id
}

func (id Identifier) Name() string {
	return id.id
}

func (id Identifier) Col(c string) Expr {
	return Expr{Col{table: id, column: Identifier{c}}}
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
	case Node:
		return AliasSpec{expr: Subexpr{expr}, alias: a}
	default:
		panic(fmt.Sprintf("%v does not implement Node", expr))
	}
}

func (a AliasSpec) String() string {
	return a.expr.String() + " AS " + a.alias
}

func (a AliasSpec) Name() string {
	return a.alias
}

func (a AliasSpec) Col(c string) Expr {
	return Expr{Col{table: a, column: Identifier{c}}}
}

func (c Col) String() string {
	return c.table.Name() + "." + c.column.Name()
}

func Binary(a Expr, op string, b Expr) Expr {
	return Expr{BinaryOp{a: a, operator: op, b: b}}
}

func (op BinaryOp) String() string {
	return op.a.String() + " " + op.operator + " " + op.b.String()
}

func Literal(v interface{}) Expr {
	return Expr{} // TODO
}
