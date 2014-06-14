package dbq

import (
	"fmt"
	"reflect"
)

type SelectQuery struct {
	Expr
	q *Dbq
}

type SelectExpr struct {
	tables     []Node
	conditions []Expression
	Compound
}

func (q *Dbq) Select(spec ...interface{}) *SelectQuery {
	return &SelectQuery{Expr: Expr{Node: &SelectExpr{}}, q: q}
}

func (s *SelectExpr) String(c Ctx) (string, error) {
	return c.Select(s)
}

func (s *SelectQuery) expr() *SelectExpr {
	return s.Expr.Node.(*SelectExpr)
}

func (s *SelectExpr) isSelectStar() bool { return true }

func (s *SelectQuery) From(specs ...interface{}) *SelectQuery {
	ex := s.expr()
	for _, spec := range specs {
		switch spec := spec.(type) {
		case string:
			ex.tables = append(ex.tables, Ident(spec))
		case *AliasExpr:
			ex.tables = append(ex.tables, spec)
		default:
			panic(fmt.Errorf("Cannot use %v [%v] as a table spec", spec, reflect.TypeOf(spec)))
		}
	}
	return s
}

func (s *SelectQuery) Where(specs ...interface{}) *SelectQuery {
	ex := s.expr()
	for _, spec := range specs {
		switch spec := spec.(type) {
		case Args:
			for ident, value := range spec {
				ex.conditions = append(ex.conditions, Ident(ident).Eq(value))
			}
		case Expression:
			ex.conditions = append(ex.conditions, spec)
		default:
			panic(fmt.Errorf("Cannot use %v [%v] as a condition", spec, reflect.TypeOf(spec)))
		}
	}
	return s
}
