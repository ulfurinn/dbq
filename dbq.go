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
	IsPrimitive() bool
}

type Primitive struct{}

func (Primitive) IsPrimitive() bool { return true }

type Compound struct{}

func (Compound) IsPrimitive() bool { return false }

type Expression interface {
	Node
	Eq(e Expression) Expression
	Plus(e Expression) Expression
	Minus(e Expression) Expression
	Mult(e Expression) Expression
	Div(e Expression) Expression
	And(e Expression) Expression
}

type Expr struct {
	Node
}

func (e Expr) Eq(other Expression) Expression {
	return Binary(e, "=", other)
}

func (e Expr) Plus(other Expression) Expression {
	return Binary(e, "+", other)
}

func (e Expr) Minus(other Expression) Expression {
	return Binary(e, "-", other)
}

func (e Expr) Mult(other Expression) Expression {
	return Binary(e, "*", other)
}

func (e Expr) Div(other Expression) Expression {
	return Binary(e, "/", other)
}

func (e Expr) And(other Expression) Expression {
	return Binary(e, "AND", other)
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

type Subexpr struct{ Expr }

type Col struct {
	table  Tabular
	column Identifier
	Primitive
}

type BinaryOp struct {
	a, b     Expression
	operator string
	Compound
}

type LiteralInt64 struct {
	v int64
	Primitive
}

func (l LiteralInt64) String() string {
	return strconv.FormatInt(l.v, 10)
}

type LiteralString struct {
	v string
	Primitive
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

func (Identifier) IsPrimitive() bool {
	return true
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

func Ident(s string) Expression {
	return Expr{Identifier(s)}
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
		return AliasSpec{source: Subexpr{Expr{expr}}, Expr: Expr{Identifier(a)}}
	default:
		panic(fmt.Sprintf("%v of type %v does not implement Node", expr, reflect.TypeOf(expr)))
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

func Binary(a Expression, op string, b Expression) Expression {
	if !a.IsPrimitive() {
		a = Subexpr{Expr{a}}
	}
	if !b.IsPrimitive() {
		b = Subexpr{Expr{b}}
	}
	return Expr{BinaryOp{a: a, operator: op, b: b}}
}

func (op BinaryOp) String() string {
	return op.a.String() + " " + op.operator + " " + op.b.String()
}

func Literal(v interface{}) Expr {
	switch v := v.(type) {
	case int:
		return Expr{LiteralInt64{v: int64(v)}}
	case int64:
		return Expr{LiteralInt64{v: v}}
	case string:
		return Expr{LiteralString{v: v}}
	default:
		panic(fmt.Sprintf("Unsupported literal %v of type %v", v, reflect.TypeOf(v)))
	}
}
