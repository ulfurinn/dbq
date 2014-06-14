package dbq

type Node interface {
	String(Ctx) (string, error)
	IsCompound() bool
}

type Primitive struct{}
type Compound struct{}

func (Primitive) IsCompound() bool { return false }
func (Compound) IsCompound() bool  { return true }

type Expression interface {
	Node
	Plus(other interface{}) Expression
	Minus(other interface{}) Expression
	Mult(other interface{}) Expression
	Div(other interface{}) Expression

	Eq(other interface{}) Expression
	NotEq(other interface{}) Expression
	Less(other interface{}) Expression
	LessEq(other interface{}) Expression
	Greater(other interface{}) Expression
	GreaterEq(other interface{}) Expression

	And(other interface{}) Expression
	Or(other interface{}) Expression
}
type Expr struct {
	Node
}

func (e *Expr) Plus(other interface{}) Expression {
	return Binary(e, "+", other)
}
func (e *Expr) Minus(other interface{}) Expression {
	return Binary(e, "-", other)
}
func (e *Expr) Mult(other interface{}) Expression {
	return Binary(e, "*", other)
}
func (e *Expr) Div(other interface{}) Expression {
	return Binary(e, "/", other)
}
func (e *Expr) Eq(other interface{}) Expression {
	return Binary(e, "=", other)
}
func (e *Expr) NotEq(other interface{}) Expression {
	return Binary(e, "!=", other)
}
func (e *Expr) Less(other interface{}) Expression {
	return Binary(e, "<", other)
}
func (e *Expr) LessEq(other interface{}) Expression {
	return Binary(e, "<=", other)
}
func (e *Expr) Greater(other interface{}) Expression {
	return Binary(e, ">", other)
}
func (e *Expr) GreaterEq(other interface{}) Expression {
	return Binary(e, ">=", other)
}
func (e *Expr) And(other interface{}) Expression {
	return Binary(e, "AND", other)
}
func (e *Expr) Or(other interface{}) Expression {
	return Binary(e, "OR", other)
}
