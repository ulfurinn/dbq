package dbq

import (
	"fmt"
	"reflect"
)

type tableExprSpec interface {
	Node
}

type SelectNode struct {
	columns    []Node
	tables     []Node
	conditions []Expression
	Bindings   map[string]Binding
	Compound
}

type SelectExpr struct {
	Expr
	q *Dbq
}

func (s *SelectExpr) parseSelectSpec(spec ...interface{}) *SelectExpr {
	return s
}

func (s *SelectExpr) Node() *SelectNode {
	return s.Expr.Node.(*SelectNode)
}

func (s *SelectExpr) isSelectStar() bool {
	return len(s.Node().columns) == 0
}

func (s *SelectExpr) From(tableSpecs ...interface{}) *SelectExpr {
	n := s.Node()
	for _, spec := range tableSpecs {
		n.parseTableSpec(spec)
	}
	return s
}

func (s *SelectNode) parseTableSpec(spec interface{}) {
	var ts Node
	var parsed bool
	switch spec := spec.(type) {
	case string:
		parsed = true
		ts = Identifier(spec)
	case AliasSpec:
		parsed = true
		ts = spec
	}
	if parsed {
		s.tables = append(s.tables, ts)
	}
}

type Args map[string]interface{}

func (s *SelectExpr) Where(whereSpecs ...interface{}) *SelectExpr {
	n := s.Node()
	for _, spec := range whereSpecs {
		switch spec := spec.(type) {
		case Expression:
			n.conditions = append(n.conditions, spec)
		case map[string]interface{}:
			for name, value := range spec {
				n.conditions = append(n.conditions, Ident(name).Eq(value))
			}
		case Args:
			for name, value := range spec {
				n.conditions = append(n.conditions, Ident(name).Eq(value))
			}
		default:
			panic(fmt.Errorf("Don't know how to use %v of type %v as a condition", spec, reflect.TypeOf(spec)))
		}

	}
	return s
}

type Binding struct {
	Name        string
	StaticValue interface{}
}

func (s *SelectExpr) Bind(name string) Binding {
	n := s.Node()
	if existing, ok := n.Bindings[name]; ok {
		return existing
	}
	b := Binding{Name: name}
	n.Bindings[name] = b
	return b
}

func (s *SelectExpr) SQL(values ...map[string]interface{}) (query string, outValues []interface{}, err error) {
	var v map[string]interface{}
	if len(values) > 0 {
		v = values[0]
	}
	return s.q.d.SQL(Expr{s}, v)
}

func (s *SelectExpr) Into(out interface{}, values ...map[string]interface{}) error {
	return nil
}
