package dbq

import (
	"fmt"
	"reflect"
)

type tableExprSpec interface {
	Node
}

type SelectExpr struct {
	q          *Dbq
	columns    []Node
	tables     []Node
	conditions []Expression
	Compound
}

func (s *SelectExpr) parseSelectSpec(spec ...interface{}) *SelectExpr {
	return s
}

func (s *SelectExpr) isSelectStar() bool {
	return len(s.columns) == 0
}

func (s *SelectExpr) From(tableSpecs ...interface{}) *SelectExpr {
	for _, spec := range tableSpecs {
		s.parseTableSpec(spec)
	}
	return s
}

func (s *SelectExpr) parseTableSpec(spec interface{}) {
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
	for _, spec := range whereSpecs {
		switch spec := spec.(type) {
		case Expression:
			s.conditions = append(s.conditions, spec)
		case map[string]interface{}:
			for name, value := range spec {
				s.conditions = append(s.conditions, Ident(name).Eq(value))
			}
		case Args:
			for name, value := range spec {
				s.conditions = append(s.conditions, Ident(name).Eq(value))
			}
		default:
			panic(fmt.Errorf("Don't know how to use %v of type %v as a condition", spec, reflect.TypeOf(spec)))
		}

	}
	return s
}

func (s *SelectExpr) String() string {
	return s.q.d.SelectString(s)
}

func (s *SelectExpr) SQL(values ...map[string]interface{}) (query string, outValues []interface{}, err error) {
	var v map[string]interface{}
	if len(values) > 0 {
		v = values[0]
	}
	return s.q.d.SelectSQL(s, v)
}

func (s *SelectExpr) Into(out interface{}, values ...map[string]interface{}) error {
	_, _, err := s.SQL(values...)
	if err != nil {
		return err
	}
	return nil
}
