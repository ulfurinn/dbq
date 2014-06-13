package dbq

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"

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

func (e Expr) Plus(other Expr) Expr {
	return Binary(e, "+", other)
}

func (e Expr) Minus(other Expr) Expr {
	return Binary(e, "-", other)
}

func (e Expr) Mult(other Expr) Expr {
	return Binary(e, "*", other)
}

func (e Expr) Div(other Expr) Expr {
	return Binary(e, "/", other)
}

type Named interface {
	Name() string
}

type Tabular interface {
	Named
	Col(c string) Expr
}

type AliasSpec struct {
	Expr
	source Node
}

type Identifier string

type Subexpr Expr

type Col struct {
	table  Tabular
	column Identifier
}

type BinaryOp struct {
	a, b     Expr
	operator string
}

type LiteralInt64 struct {
	v int64
}

func (l LiteralInt64) String() string {
	return strconv.FormatInt(l.v, 10)
}

type LiteralString struct {
	v string
}

func (l LiteralString) String() string {
	return l.v
}

func (id Identifier) String() string {
	return string(id)
}

func (id Identifier) Name() string {
	return string(id)
}

func (id Identifier) Col(c string) Expr {
	return Expr{Col{table: id, column: Identifier(c)}}
}

func (e Subexpr) String() string {
	return "(" + e.Node.String() + ")"
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
		return AliasSpec{source: Identifier(expr), Expr: Expr{Identifier(a)}}
	case Node:
		return AliasSpec{source: Subexpr{expr}, Expr: Expr{Identifier(a)}}
	default:
		panic(fmt.Sprintf("%v does not implement Node", expr))
	}
}

func (a AliasSpec) String() string {
	return a.source.String() + " AS " + a.Expr.String()
}

func (a AliasSpec) Name() string {
	return a.Expr.String()
}

func (a AliasSpec) Col(c string) Expr {
	return Expr{Col{table: a, column: Identifier(c)}}
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
	switch v := v.(type) {
	case int:
		return Expr{LiteralInt64{int64(v)}}
	case int64:
		return Expr{LiteralInt64{v}}
	case string:
		return Expr{LiteralString{v}}
	default:
		panic(fmt.Sprintf("Unsupported literal %v of type %v", v, reflect.TypeOf(v)))
	}
}
