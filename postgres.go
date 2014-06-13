package dbq

import (
	"strings"
)

type PostgresDialect struct{}

func (PostgresDialect) SelectString(s *SelectExpr) string {
	q := "SELECT "
	if s.isSelectStar() {
		q += "* "
	}
	if len(s.tables) > 0 {
		q += "FROM "
		tables := []string{}
		for _, table := range s.tables {
			tables = append(tables, table.String())
		}
		q += strings.Join(tables, ", ")
	}
	return q
}

func (d PostgresDialect) SelectSQL(s *SelectExpr, values map[string]interface{}) (query string, outValues []interface{}, err error) {
	return d.SelectString(s), nil, nil
}
