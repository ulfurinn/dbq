package dbq

import (
	"strings"

	"fmt"
)

type PostgresDialect struct{}

func (PostgresDialect) SQL(e Expression, v Args) (sql string, values []interface{}, err error) {
	c := &PostgresCtx{}
	sql, err = e.String(c)
	return
}
func (PostgresDialect) SQLString(e Expression) (sql string, err error) {
	c := &PostgresCtx{}
	sql, err = e.String(c)
	return
}

type PostgresCtx struct {
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
	if s.isSelectStar() {
		sql += "*"
	}
	if len(s.tables) > 0 {
		tables := []string{}
		for _, table := range s.tables {
			tableSQL, err := table.String(c)
			if err != nil {
				return "", err
			}
			tables = append(tables, tableSQL)
		}
		sql += " FROM " + strings.Join(tables, ", ")
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
