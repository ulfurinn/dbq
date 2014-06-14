package dbq

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"

	_ "github.com/lib/pq"
)

type Dbq struct {
	Dialect
	*sql.DB
}

type Args map[string]interface{}

type AliasExpr struct {
	Expression // alias
	Source     Node
}

type Identifier string

func (Identifier) IsCompound() bool              { return false }
func (id Identifier) String(Ctx) (string, error) { return string(id), nil }
func (id Identifier) Name() string               { return string(id) }
func (id Identifier) Col(column string) Expression {
	return &Expr{&ColumnExpr{table: id, column: column}}
}

type Tabular interface {
	Name() string
	Col(name string) Expression
}

type ColumnExpr struct {
	table  Tabular
	column string
	Primitive
}

func (col *ColumnExpr) String(c Ctx) (string, error) {
	return c.Column(col)
}

func NewQ(db *sql.DB, d Dialect) *Dbq {
	return &Dbq{Dialect: d, DB: db}
}

func Alias(source interface{}, name string) *AliasExpr {
	var tabular Node
	switch source := source.(type) {
	case string:
		tabular = Identifier(source)
	case Node:
		tabular = source
	default:
		panic(fmt.Errorf("Cannot use %v [%v] as alias source", source, reflect.TypeOf(source)))
	}
	return &AliasExpr{Expression: &Expr{Identifier(name)}, Source: tabular}
}

func (alias *AliasExpr) Col(column string) Expression {
	return &Expr{&ColumnExpr{table: alias, column: column}}
}
func (alias *AliasExpr) Name() string {
	return alias.Expression.(*Expr).Node.(Identifier).Name()
}
func (alias *AliasExpr) String(c Ctx) (string, error) {
	return c.Alias(alias)
}

func Ident(id string) Expression {
	return &Expr{Identifier(id)}
}

type LiteralInt64 int64

func (LiteralInt64) IsCompound() bool             { return false }
func (i LiteralInt64) String(Ctx) (string, error) { return strconv.FormatInt(int64(i), 10), nil }

func Literal(value interface{}) Expression {
	switch value := value.(type) {
	case int:
		return &Expr{LiteralInt64(value)}
	case int32:
		return &Expr{LiteralInt64(value)}
	case int64:
		return &Expr{LiteralInt64(value)}
	default:
		panic(fmt.Errorf("Cannot create a literal from %v [%v]", value, reflect.TypeOf(value)))
	}
}

type BinaryOp struct {
	a, b Expression
	op   string
	Compound
}

func (op *BinaryOp) String(c Ctx) (string, error) {
	return c.BinaryOp(op)
}

func operandToExpression(v interface{}) Expression {
	switch v := v.(type) {
	case Expression:
		return v
	case int, int32, int64:
		return Literal(v)
	default:
		panic(fmt.Errorf("Cannot create an expression from %v [%v]", v, reflect.TypeOf(v)))
	}
}

func Binary(a interface{}, op string, b interface{}) Expression {
	aEx := operandToExpression(a)
	bEx := operandToExpression(b)
	return &Expr{&BinaryOp{a: aEx, op: op, b: bEx}}
}
