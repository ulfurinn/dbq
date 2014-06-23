package dbq

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"
)

type SelectQuery struct {
	Expr
	q           *Dbq
	singleClone *SelectQuery
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
type All struct{}

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

func isScannable(t reflect.Type) bool {
	iface := reflect.Zero(t).Interface()
	_, isScanner := iface.(sql.Scanner)
	_, isBuffer := iface.([]byte)
	_, isTime := iface.(time.Time)
	return isScanner || isBuffer || isTime
}

//	TODO: support []byte and time.Time

func isScalar(t reflect.Type) bool {
	if isScannable(t) {
		return true
	}
	k := t.Kind()
	return !(k == reflect.Array || k == reflect.Chan || k == reflect.Func || k == reflect.Interface ||
		k == reflect.Map || k == reflect.Slice || k == reflect.Struct ||
		k == reflect.Uintptr || k == reflect.UnsafePointer)
}

func (s *SelectQuery) Into(target interface{}, args ...Args) error {
	v := reflect.ValueOf(target)
	if v.Kind() != reflect.Ptr {
		return fmt.Errorf("Into() expects a pointer")
	}

	arg := Args{}
	for _, a := range args {
		for k, v := range a {
			arg[k] = v
		}
	}

	if v.Elem().Kind() == reflect.Slice {
		return s.selectRows(v, arg)
	} else {
		if s.singleClone == nil {
			s.singleClone = &SelectQuery{Expr: Expr{Node: s.expr().clone()}, q: s.q}
			s.singleClone.Limit(1)
		}
		return s.singleClone.selectSingleRow(v, arg)
	}
}

func (s *SelectQuery) selectRows(v reflect.Value, arg Args) error {
	targetType := v.Type().Elem().Elem()
	isStruct := targetType.Kind() == reflect.Struct
	isSc := isScalar(targetType)
	if !isStruct && !isSc {
		return fmt.Errorf("only scalars and structs are implemented")
	}

	rows, cols, err := s.execute(arg)
	if err != nil {
		return err
	}
	defer rows.Close()

	targetSlice := v.Elem()

	for rows.Next() {
		acceptor := reflect.New(targetType)
		if isSc {
			err = scanScalar(acceptor, rows, cols)
		} else if isStruct {
			err = scanStruct(acceptor, rows, cols)
		}
		if err != nil {
			return err
		}
		targetSlice = reflect.Append(targetSlice, acceptor.Elem())
	}
	v.Elem().Set(targetSlice)
	return nil

}

func (s *SelectQuery) selectSingleRow(v reflect.Value, arg Args) error {
	isStruct := v.Elem().Kind() == reflect.Struct
	isSc := isScalar(v.Type().Elem())
	if !isStruct && !isSc {
		return fmt.Errorf("only scalars and structs are implemented")
	}

	rows, cols, err := s.execute(arg)
	if err != nil {
		return err
	}
	defer rows.Close()

	scannedAny := false
	for rows.Next() {
		scannedAny = true
		if isSc {
			err = scanScalar(v, rows, cols)
		} else if isStruct {
			err = scanStruct(v, rows, cols)
		}
		if err != nil {
			return err
		}
	}
	if !scannedAny {
		return sql.ErrNoRows
	}
	return nil
}

func (s *SelectQuery) execute(arg Args) (rows *sql.Rows, cols []string, err error) {
	q, values, err := s.q.SQL(s, arg)
	if err != nil {
		return
	}
	rows, err = s.q.Query(q, values...)
	if err != nil {
		return
	}
	cols, err = rows.Columns()
	return
}

func scanScalar(v reflect.Value, rows *sql.Rows, cols []string) (err error) {
	acceptor := v.Interface()
	acceptors := []interface{}{acceptor}
	//	TODO: are there cheaper dummy values? RawBytes?
	for i := 0; i < len(cols)-1; i++ {
		acceptors = append(acceptors, new([]byte))
	}
	err = rows.Scan(acceptors...)
	return
}

//	v: pointer to struct
func scanStruct(v reflect.Value, rows *sql.Rows, cols []string) (err error) {
	str := v.Elem()
	strT := v.Type().Elem()
	acceptors := make([]interface{}, len(cols))
	for i, col := range cols {
		acceptors[i] = mapColumnToAcceptor(col, str, strT)
	}
	err = rows.Scan(acceptors...)
	return
}

// TODO: work around nullable primitives

// v: struct
func mapColumnToAcceptor(col string, v reflect.Value, t reflect.Type) interface{} {
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if strings.ToLower(f.Name) == col {
			return v.Field(i).Addr().Interface()
		}
	}
	return new([]byte)
}
