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

func (id *IdentExpr) Name() string { return string(id.Node.(Identifier)) }
func (id *IdentExpr) Col(column string) Expression {
	return &Expr{&ColumnExpr{table: id, column: column}}
}

type IdentExpr struct {
	Expr
}

type Tabular interface {
	Name() string
	Col(name string) Expression
}

type TabularExpression interface {
	Tabular
	Expression
}

type Nullable interface {
	IsNull(Ctx) bool
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
		tabular = Ident(source)
	case Node:
		tabular = source
	default:
		panic(fmt.Errorf("Cannot use %v [%v] as alias source", source, reflect.TypeOf(source)))
	}
	return &AliasExpr{Expression: Ident(name), Source: tabular}
}

func (alias *AliasExpr) Col(column string) Expression {
	return &Expr{&ColumnExpr{table: alias, column: column}}
}
func (alias *AliasExpr) Name() string {
	return alias.Expression.(*IdentExpr).Name()
}
func (alias *AliasExpr) String(c Ctx) (string, error) {
	return c.Alias(alias)
}

func Ident(id string) TabularExpression {
	return &IdentExpr{Expr: Expr{Identifier(id)}}
}

type LiteralInt64 int64

func (LiteralInt64) IsCompound() bool             { return false }
func (i LiteralInt64) String(Ctx) (string, error) { return strconv.FormatInt(int64(i), 10), nil }

type LiteralString string

func (LiteralString) IsCompound() bool               { return false }
func (s LiteralString) String(c Ctx) (string, error) { return c.StaticPlaceholder(string(s)) }

type LiteralList []interface{}

func (LiteralList) IsCompound() bool               { return true }
func (l LiteralList) String(c Ctx) (string, error) { return c.StaticPlaceholder([]interface{}(l)) }

type LiteralNull struct{}

func (LiteralNull) IsCompound() bool           { return false }
func (LiteralNull) String(Ctx) (string, error) { return "NULL", nil }
func (LiteralNull) IsNull(Ctx) bool            { return true }

func Literal(value interface{}) Expression {
	if value == nil {
		return &Expr{LiteralNull{}}
	}
	switch value := value.(type) {
	case int:
		return &Expr{LiteralInt64(value)}
	case int32:
		return &Expr{LiteralInt64(value)}
	case int64:
		return &Expr{LiteralInt64(value)}
	case string:
		return &Expr{LiteralString(value)}
	case []interface{}:
		return &Expr{LiteralList(value)}
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
	case int, int32, int64, string, nil:
		return Literal(v)
	default:
		panic(fmt.Errorf("Cannot create an expression from %v [%v]", v, reflect.TypeOf(v)))
	}
}

func toInterfaceSlice(v interface{}) (result []interface{}, ok bool) {
	t := reflect.TypeOf(v)
	if t == nil || t.Kind() != reflect.Slice {
		return nil, false
	}
	ok = true
	val := reflect.ValueOf(v)
	for i := 0; i < val.Len(); i++ {
		result = append(result, val.Index(i).Interface())
	}
	return
}

func listToExpression(v interface{}) Expression {
	expr, isExpr := v.(Expression)
	if isExpr {
		return expr
	}
	if genericList, ok := toInterfaceSlice(v); ok {
		return Literal(genericList)
	}
	return Literal([]interface{}{v})
}

func Binary(a interface{}, op string, b interface{}) Expression {
	aEx := operandToExpression(a)
	bEx := operandToExpression(b)
	return &Expr{&BinaryOp{a: aEx, op: op, b: bEx}}
}

type InExpr struct {
	element Expression
	list    Expression
	Primitive
}

func (in *InExpr) String(c Ctx) (string, error) {
	return c.In(in)
}

func In(element interface{}, list interface{}) Expression {
	elementEx := operandToExpression(element)
	listEx := listToExpression(list)
	return &Expr{&InExpr{element: elementEx, list: listEx}}
}

type Binding struct {
	name     string
	Compound // required to work with IN(). should be cleaned up, maybe.
}

func (b *Binding) String(c Ctx) (string, error) {
	return c.DynamicPlaceholder(b)
}

func (b *Binding) IsNull(c Ctx) bool {
	v, ok := c.BindValue(b)
	return ok && v == nil
}

func Bind(name string) Expression {
	return &Expr{&Binding{name: name}}
}

type CastExpr struct {
	e   Expression
	typ string
	Primitive
}

func Cast(e Expression, typ string) Expression {
	return &Expr{&CastExpr{e: e, typ: typ}}
}

func (cast *CastExpr) String(c Ctx) (string, error) {
	return c.Cast(cast)
}

type FuncExpr struct {
	name   string
	values []Expression
	Primitive
}

func (f *FuncExpr) String(c Ctx) (string, error) {
	return c.Func(f)
}

func Func(name string, args ...Expression) Expression {
	return &Expr{&FuncExpr{name: name, values: args}}
}
