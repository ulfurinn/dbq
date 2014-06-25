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

type IdentExpr struct {
	Expr
}

func (id *IdentExpr) Name() string { return string(id.Node.(Identifier)) }
func (id *IdentExpr) Col(column string) Expression {
	return &Expr{&ColumnExpr{table: id, column: column}}
}

// Tabular is a value that can represent a table or a table-like expression.
type Tabular interface {
	Name() string
	Col(name string) Expression // creates a column reference
}

type TabularExpression interface {
	Tabular
	Expression
}

// Nullable is used by the equality operator to decide when to use IS NOT/IS NOT NULL instead of =/!=.
// It is most useful with bindings.
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

// NewQ returns a new dbq handle.
func NewQ(db *sql.DB, d Dialect) *Dbq {
	return &Dbq{Dialect: d, DB: db}
}

// Alias returns an alias expression.
//
// source can be of the following types:
//   string - will be cast to an Identifier
//   Node - will be used as is
// Anything else will panic.
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

// Col represents a column expression, using the alias as the table name.
func (alias *AliasExpr) Col(column string) Expression {
	return &Expr{&ColumnExpr{table: alias, column: column}}
}

// Name returns the alias part of the expression.
func (alias *AliasExpr) Name() string {
	return alias.Expression.(*IdentExpr).Name()
}

// String implements Node.
func (alias *AliasExpr) String(c Ctx) (string, error) {
	return c.Alias(alias)
}

// Ident returns an identifier expression.
func Ident(id string) TabularExpression {
	return &IdentExpr{Expr: Expr{Identifier(id)}}
}

// LiteralInt64 represents an SQL integer literal.
type LiteralInt64 int64

func (LiteralInt64) IsCompound() bool             { return false }
func (i LiteralInt64) String(Ctx) (string, error) { return strconv.FormatInt(int64(i), 10), nil }

type LiteralString string

func (LiteralString) IsCompound() bool { return false }

// String returns an implicitly generated placeholder instead of the actual literal.
// This is necessary because database/sql does not expose an interface to safely quote string literals,
// so we are forced to inject a placeholder.
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

// In returns an IN(...) expression. The argument can be an Expression (probably a LiteralList) or a []interface{}, in which case a number of implicit placeholders may be generated.
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

// Bind returns an explicit placeholder.
//
// Concrete values can be specified when calling *Dbq.SQL().
// Note that the string representation returned by *Dbq.SQL() may changed based on the values provided.
func Bind(name string) Expression {
	return &Expr{&Binding{name: name}}
}

type CastExpr struct {
	e   Expression
	typ string
	Primitive
}

// Cast returns a typecase expression.
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

type AggFuncExpr struct {
	name          string
	values        []Expression
	distinct, all bool
	order         Node
	Primitive
}

func (f *AggFuncExpr) String(c Ctx) (string, error) {
	return c.AggFunc(f)
}

func AggFunc(name string, args ...interface{}) Expression {
	e := &AggFuncExpr{name: name, values: []Expression{}}
	for _, arg := range args {
		switch arg := arg.(type) {
		case Distinct:
			e.distinct = true
		case All:
			e.all = true
		case Expression:
			e.values = append(e.values, arg)
		case *OrderExpr:
			e.order = arg
		}
	}
	return &Expr{e}
}

type OrderExpr struct {
	exprs []OrderClause
	Primitive
}

type OrderKind int

const (
	OrderDefault OrderKind = iota
	OrderAsc     OrderKind = iota
	OrderDesc    OrderKind = iota
)

type OrderClause struct {
	column Expression
	order  OrderKind
}

func (order *OrderExpr) String(c Ctx) (string, error) {
	return c.OrderBy(order)
}

func OrderBy(clauses ...interface{}) *OrderExpr {
	orderExpr := &OrderExpr{}
	for _, clause := range clauses {
		order := OrderClause{order: OrderDefault}
		switch clause := clause.(type) {
		case string:
			order.column = Ident(clause)
		case Expression:
			order.column = clause
		case OrderClause:
			order = clause
		}
		orderExpr.exprs = append(orderExpr.exprs, order)
	}
	return orderExpr
}

func Order(column interface{}, order interface{}) (result OrderClause) {
	switch column := column.(type) {
	case string:
		result.column = Ident(column)
	case Expression:
		result.column = column
	default:
		panic(fmt.Errorf("cannot create an order clause from %v [%v]", column, reflect.TypeOf(column)))
	}
	switch order := order.(type) {
	case OrderKind:
		result.order = order
	case string:
		switch order {
		case "asc", "ASC":
			result.order = OrderAsc
		case "desc", "DESC":
			result.order = OrderDesc
		default:
			result.order = OrderDefault
		}
	default:
		panic(fmt.Errorf("cannot create an order clause from %v [%v]", order, reflect.TypeOf(order)))
	}
	return
}
