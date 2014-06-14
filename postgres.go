package dbq

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

type PostgresDialect struct{}

type PostgresContext struct {
	e                 Expression
	dynamicValues     Args
	placeholderValues []interface{}
	placeholderMap    map[string]int
}

// func (PostgresDialect) SelectString(s *SelectExpr) string {
// 	q := "SELECT "
// 	if s.isSelectStar() {
// 		q += "* "
// 	}
// 	if len(s.tables) > 0 {
// 		q += "FROM "
// 		tables := []string{}
// 		for _, table := range s.tables {
// 			tables = append(tables, table.String())
// 		}
// 		q += strings.Join(tables, ", ")
// 	}
// 	if len(s.conditions) > 0 {
// 		q += " WHERE "
// 		condition := s.conditions[0]
// 		for _, c := range s.conditions[1:] {
// 			condition = condition.And(c)
// 		}
// 		q += condition.String()
// 	}
// 	return q
// }

func (d PostgresDialect) SQL(s Expression, values Args) (query string, outValues []interface{}, err error) {
	c := d.Context(s, values)
	query, err = c.SQL()
	outValues = c.Values()
	return
}

func (d PostgresDialect) SQLString(s Expression) (string, error) {
	return d.Context(s, Args{}).SQL()
}

func (d PostgresDialect) Context(e Expression, dynamicValues Args) *PostgresContext {
	return &PostgresContext{e: e, dynamicValues: dynamicValues, placeholderMap: make(map[string]int)}
}

func (c *PostgresContext) SQL() (string, error) {
	return c.subSQL(c.e)
}

func (c *PostgresContext) Values() (values []interface{}) {
	return c.placeholderValues
}

func (c *PostgresContext) subSQL(e Expression) (q string, err error) {
	switch e := e.(type) {
	case Expr:
		switch n := e.Node.(type) {
		case Expr:
			return c.subSQL(n)
		case Identifier:
			return string(n), nil
		case LiteralInt64:
			return strconv.FormatInt(n.v, 10), nil
		case LiteralString:
			c.placeholderValues = append(c.placeholderValues, n.v)
			return fmt.Sprintf("$%d", len(c.placeholderValues)), nil
		case Col:
			return fmt.Sprintf(`"%s"."%s"`, n.table.Name(), n.column.Name()), nil
		case Subexpr:
			sql, err := c.subSQL(n.Expression)
			if err != nil {
				return "", err
			}
			return "(" + sql + ")", nil
		case BinaryOp:
			a, err1 := c.subSQL(n.a)
			if err1 != nil {
				return "", fmt.Errorf("in binaryop: %v", err1)
			}
			b, err2 := c.subSQL(n.b)
			if err2 != nil {
				return "", fmt.Errorf("in binaryop: %v", err2)
			}
			return a + " " + n.operator + " " + b, nil
		case AliasSpec:
			return c.subSQL(n)
		case *SelectExpr:
			return c.selectSQL(n)
		default:
			panic(fmt.Errorf("PostgresDialect cannot handle node type %v", reflect.TypeOf(n)))
		}
	case Subexpr:
		sql, err := c.subSQL(e.Expression)
		if err != nil {
			return "", err
		}
		return "(" + sql + ")", nil
	case AliasSpec:
		sourceSQL, err := c.subSQL(Expr{e.source})
		if err != nil {
			return "", err
		}
		return sourceSQL + " AS " + e.Name(), nil
	case *SelectExpr:
		return c.selectSQL(e)
	default:
		panic(fmt.Errorf("PostgresDialect cannot handle expression type %v", reflect.TypeOf(e)))
	}
	return
}

func (c *PostgresContext) selectSQL(s *SelectExpr) (string, error) {
	n := s.Node()
	q := "SELECT "
	if s.isSelectStar() {
		q += "* "
	}
	if len(n.tables) > 0 {
		q += "FROM "
		tables := []string{}
		for _, table := range n.tables {
			sql, err := c.subSQL(Expr{table})
			if err != nil {
				return "", err
			}
			tables = append(tables, sql)
		}
		q += strings.Join(tables, ", ")
	}
	if len(n.conditions) > 0 {
		q += " WHERE "
		condition := n.conditions[0]
		for _, c := range n.conditions[1:] {
			condition = condition.And(c)
		}
		sql, err := c.subSQL(condition)
		if err != nil {
			return "", err
		}
		q += sql
	}
	return q, nil
}
