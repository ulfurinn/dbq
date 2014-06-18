package dbq

import (
	"fmt"
	"reflect"
)

type SelectQuery struct {
	Expr
	q           *Dbq
	scalarClone *SelectQuery
}

type SelectExpr struct {
	distinct      bool
	columns       []Node
	tables        []Node
	conditions    []Expression
	limit, offset uint
	Compound
}

func (s *SelectExpr) clone() *SelectExpr {
	cl := *s
	return &cl
}

type Distinct struct{}

func (q *Dbq) Select(spec ...interface{}) *SelectQuery {
	node := &SelectExpr{}
	node.parseSelect(spec)
	return &SelectQuery{Expr: Expr{Node: node}, q: q}
}

func (s *SelectExpr) parseSelect(specs []interface{}) {
	for _, spec := range specs {
		switch spec := spec.(type) {
		case string:
			s.columns = append(s.columns, Ident(spec))
		case Distinct:
			s.distinct = true
		case Node:
			s.columns = append(s.columns, spec)
		}
	}
}

func (s *SelectExpr) String(c Ctx) (string, error) {
	return c.Select(s)
}

func (s *SelectQuery) expr() *SelectExpr {
	return s.Expr.Node.(*SelectExpr)
}

func (s *SelectExpr) isSelectStar() bool { return len(s.columns) == 0 }

type JoinKind int

const (
	InnerJoinKind JoinKind = iota
	LeftJoinKind  JoinKind = iota
	RightJoinKind JoinKind = iota
	OuterJoinKind JoinKind = iota
	CrossJoinKind JoinKind = iota
)

type JoinConditionKind int

const (
	JoinOn    JoinConditionKind = iota
	JoinUsing JoinConditionKind = iota
)

type JoinCondition struct {
	kind      JoinConditionKind
	condition Node
	Compound
}

type JoinExpr struct {
	kind      JoinKind
	table     Node
	condition Node
	Primitive
}

func joinTable(t interface{}) Node {
	switch t := t.(type) {
	case string:
		return Ident(t)
	case *IdentExpr:
		return t
	case *AliasExpr:
		return t
	default:
		panic(fmt.Errorf("Cannot use %v [%v] as a join table", t, reflect.TypeOf(t)))
	}
}

func Join(table interface{}, condition *JoinCondition) *JoinExpr {
	return &JoinExpr{kind: InnerJoinKind, table: joinTable(table), condition: condition}
}

func LeftJoin(table interface{}, condition *JoinCondition) *JoinExpr {
	return &JoinExpr{kind: LeftJoinKind, table: joinTable(table), condition: condition}
}

func RightJoin(table interface{}, condition *JoinCondition) *JoinExpr {
	return &JoinExpr{kind: RightJoinKind, table: joinTable(table), condition: condition}
}

func OuterJoin(table interface{}, condition *JoinCondition) *JoinExpr {
	return &JoinExpr{kind: OuterJoinKind, table: joinTable(table), condition: condition}
}

func On(condition Node) *JoinCondition {
	return &JoinCondition{kind: JoinOn, condition: condition}
}

// TODO: can USING take multiple columns?

func Using(condition Node) *JoinCondition {
	return &JoinCondition{kind: JoinUsing, condition: condition}
}

func (jc *JoinExpr) String(c Ctx) (string, error) {
	return c.Join(jc)
}

func (jc *JoinCondition) String(c Ctx) (string, error) {
	return c.JoinCondition(jc)
}

func (s *SelectQuery) From(specs ...interface{}) *SelectQuery {
	ex := s.expr()
	for _, spec := range specs {
		switch spec := spec.(type) {
		case string:
			ex.tables = append(ex.tables, Ident(spec))
		case *AliasExpr:
			ex.tables = append(ex.tables, spec)
		case *JoinExpr:
			ex.tables = append(ex.tables, spec)
		case *IdentExpr:
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
				col := Ident(ident)
				if reflect.ValueOf(value).Kind() == reflect.Slice {
					ex.conditions = append(ex.conditions, col.In(value))
				} else {
					ex.conditions = append(ex.conditions, col.Eq(value))
				}

			}
		case Expression:
			ex.conditions = append(ex.conditions, spec)
		default:
			panic(fmt.Errorf("Cannot use %v [%v] as a condition", spec, reflect.TypeOf(spec)))
		}
	}
	return s
}
func (s *SelectQuery) Limit(l uint) *SelectQuery {
	ex := s.expr()
	ex.limit = l
	return s
}

func (s *SelectQuery) Offset(o uint) *SelectQuery {
	ex := s.expr()
	ex.offset = o
	return s
}

func isScalar(k reflect.Kind) bool {
	return !(k == reflect.Array || k == reflect.Chan || k == reflect.Func || k == reflect.Interface ||
		k == reflect.Map || k == reflect.Slice || k == reflect.Struct ||
		k == reflect.Uintptr || k == reflect.UnsafePointer)
}

func (s *SelectQuery) Into(target interface{}, args ...Args) error {
	arg := Args{}
	for _, a := range args {
		for k, v := range a {
			arg[k] = v
		}
	}

	v := reflect.ValueOf(target)
	if v.Kind() != reflect.Ptr {
		return fmt.Errorf("Into() expects a pointer")
	}
	if v.Elem().Kind() == reflect.Slice {
		if v.Type().Elem().Elem().Kind() == reflect.Struct {
			return fmt.Errorf("structs are not implemented")
		}
		// list of scalars

		sql, values, e := s.q.SQL(s, arg)
		if e != nil {
			return e
		}
		rows, e := s.q.Query(sql, values...)
		if e != nil {
			return e
		}
		defer rows.Close()
		cols, e := rows.Columns()
		if e != nil {
			return e
		}

		arr := v.Elem()

		for rows.Next() {
			acceptor := reflect.New(v.Type().Elem().Elem())
			acceptors := []interface{}{acceptor.Interface()}
			for i := 0; i < len(cols)-1; i++ {
				acceptors = append(acceptors, new([]byte))
			}
			e = rows.Scan(acceptors...)
			if e != nil {
				return e
			}
			arr = reflect.Append(arr, reflect.ValueOf(acceptors[0]).Elem())
		}
		v.Elem().Set(arr)
		return nil

	} else {
		if s.scalarClone == nil {
			s.scalarClone = &SelectQuery{Expr: Expr{Node: s.expr().clone()}, q: s.q}
			s.scalarClone.Limit(1)
		}
		if v.Elem().Kind() == reflect.Struct {
			return fmt.Errorf("structs are not implemented")
		}
		sql, values, e := s.q.SQL(s.scalarClone, arg)
		if e != nil {
			return e
		}
		rows, e := s.q.Query(sql, values...)
		if e != nil {
			return e
		}
		defer rows.Close()
		cols, e := rows.Columns()
		if e != nil {
			return e
		}
		acceptors := []interface{}{v.Interface()}
		for i := 0; i < len(cols)-1; i++ {
			acceptors = append(acceptors, new([]byte))
		}
		for rows.Next() {
			e = rows.Scan(acceptors...)
			if e != nil {
				return e
			}
		}
	}

	return nil
}
