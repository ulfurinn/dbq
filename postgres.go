package dbq

import (
	"reflect"
	"strings"

	"fmt"
)

type PostgresDialect struct{}

func (d PostgresDialect) SQL(e Expression, v Args) (sql string, values []interface{}, err error) {
	c := d.Ctx()
	sql, err = e.String(c)
	for _, v := range c.placeholderValues {
		values = append(values, v)
	}
	for k, v := range v {
		index, ok := c.placeholderNameToIndex[k]
		if ok {
			values[index-1] = v
		}
	}
	return
}
func (d PostgresDialect) SQLString(e Expression) (sql string, err error) {
	c := d.Ctx()
	sql, err = e.String(c)
	return
}

func (PostgresDialect) Ctx() *PostgresCtx {
	return &PostgresCtx{placeholderNameToIndex: make(map[string]int)}
}

type PostgresCtx struct {
	placeholderValues      []interface{}
	placeholderNameToIndex map[string]int
}

func (c *PostgresCtx) BinaryOp(e *BinaryOp) (sql string, err error) {
	a, err := e.a.String(c)
	if err != nil {
		return
	}
	b, err := e.b.String(c)
	if err != nil {
		return
	}
	if e.a.IsCompound() {
		a = "(" + a + ")"
	}
	if e.b.IsCompound() {
		b = "(" + b + ")"
	}
	sql = a + " " + e.op + " " + b
	return
}

func (c *PostgresCtx) Column(col *ColumnExpr) (sql string, err error) {
	return fmt.Sprintf(`"%s"."%s"`, col.table.Name(), col.column), nil
}

func (c *PostgresCtx) Select(s *SelectExpr) (sql string, err error) {
	sql = "SELECT "
	if s.distinct {
		sql += "DISTINCT "
	}
	if s.isSelectStar() {
		sql += "*"
	} else {
		columns := []string{}
		for _, col := range s.columns {
			sql, err := col.String(c)
			if err != nil {
				return "", err
			}
			columns = append(columns, sql)
		}
		sql += strings.Join(columns, ", ")
	}
	if len(s.tables) > 0 {
		tables := []string{}
		for i, table := range s.tables {
			tableSQL, err := table.String(c)
			if err != nil {
				return "", err
			}
			_, isJoin := table.(*JoinExpr)
			if i > 0 && !isJoin { //	two tables without an explicit join condition
				tableSQL = ", " + tableSQL
			}
			tables = append(tables, tableSQL)
		}
		sql += " FROM " + strings.Join(tables, " ")
	}
	if len(s.conditions) > 0 {
		acc := s.conditions[0]
		for _, condition := range s.conditions[1:] {
			acc = acc.And(condition)
		}
		conditionSQL, err := acc.String(c)
		if err != nil {
			return "", err
		}
		sql += " WHERE " + conditionSQL
	}
	return
}

func (c *PostgresCtx) Alias(alias *AliasExpr) (sql string, err error) {
	source, err := alias.Source.String(c)
	if err != nil {
		return
	}
	if alias.Source.IsCompound() {
		source = "(" + source + ")"
	}
	sql = source + " AS " + alias.Name()
	return
}

func (c *PostgresCtx) StaticPlaceholder(value interface{}) (sql string, err error) {
	c.placeholderValues = append(c.placeholderValues, value)
	sql = fmt.Sprintf("$%d", len(c.placeholderValues))
	return
}

func (c *PostgresCtx) DynamicPlaceholder(b *Binding) (sql string, err error) {
	existing, ok := c.placeholderNameToIndex[b.name]
	if ok {
		sql = fmt.Sprintf("$%d", existing)
		return
	}
	c.placeholderValues = append(c.placeholderValues, nil)
	c.placeholderNameToIndex[b.name] = len(c.placeholderValues)
	sql = fmt.Sprintf("$%d", len(c.placeholderValues))
	return
}

func (c *PostgresCtx) Join(j *JoinExpr) (sql string, err error) {
	var join string
	switch j.kind {
	case InnerJoinKind:
		join = "INNER JOIN"
	case LeftJoinKind:
		join = "LEFT JOIN"
	case RightJoinKind:
		join = "RIGHT JOIN"
	case OuterJoinKind:
		join = "OUTER JOIN"
	}
	tableSql, err := j.table.String(c)
	if err != nil {
		return "", err
	}
	conditionSql, err := j.condition.String(c)
	if err != nil {
		return "", err
	}
	return join + " " + tableSql + " " + conditionSql, nil
}

func (c *PostgresCtx) JoinCondition(jc *JoinCondition) (sql string, err error) {
	switch jc.kind {
	case JoinOn:
		sql, err := jc.condition.String(c)
		if err != nil {
			return "", err
		}
		return "ON (" + sql + ")", nil
	case JoinUsing:
		sql, err := jc.condition.String(c)
		if err != nil {
			return "", err
		}
		return "USING (" + sql + ")", nil
	}
	return "", fmt.Errorf("Could not use %v [%v] as a join condition", jc, reflect.TypeOf(jc))
}
