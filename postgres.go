package dbq

import (
	"reflect"
	"strings"

	"fmt"
)

type PostgresDialect struct{}

func (d PostgresDialect) SQL(e Expression, v Args) (sql string, values []interface{}, err error) {
	c := d.Ctx()
	c.dynamicValues = v
	sql, err = e.String(c)
	for _, v := range c.placeholderValues {
		values = append(values, v)
	}
	for k, v := range v {
		indexes, ok := c.placeholderNameToIndexes[k]
		if ok {
			if genericList, ok := toInterfaceSlice(v); ok {
				for i, index := range indexes {
					values[index-1] = genericList[i]
				}
			} else {
				values[indexes[0]-1] = v
			}
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
	return &PostgresCtx{placeholderNameToIndexes: make(map[string][]int)}
}

type PostgresCtx struct {
	placeholderValues        []interface{}
	placeholderNameToIndexes map[string][]int
	dynamicValues            Args
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
	list, ok := value.([]interface{})
	if ok {
		strs := []string{}
		for _, e := range list {
			sql, err = c.StaticPlaceholder(e)
			if err != nil {
				return
			}
			strs = append(strs, sql)
		}
		sql = strings.Join(strs, ",")
		return
	}
	c.placeholderValues = append(c.placeholderValues, value)
	sql = fmt.Sprintf("$%d", len(c.placeholderValues))
	return
}

func (c *PostgresCtx) DynamicPlaceholder(b *Binding) (sql string, err error) {
	existing, ok := c.placeholderNameToIndexes[b.name]
	if ok {
		strs := []string{}
		for _, i := range existing {
			strs = append(strs, fmt.Sprintf("$%d", i))
		}
		sql = strings.Join(strs, ",")
		return
	}
	v := reflect.ValueOf(c.dynamicValues[b.name])
	if v.Kind() == reflect.Slice {
		indexes := []int{}
		strs := []string{}
		for i := 0; i < v.Len(); i++ {
			c.placeholderValues = append(c.placeholderValues, nil)
			indexes = append(indexes, len(c.placeholderValues))
			strs = append(strs, fmt.Sprintf("$%d", len(c.placeholderValues)))
		}
		c.placeholderNameToIndexes[b.name] = indexes
		sql = strings.Join(strs, ",")
		return
	}
	c.placeholderValues = append(c.placeholderValues, nil)
	c.placeholderNameToIndexes[b.name] = []int{len(c.placeholderValues)}
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

func (c *PostgresCtx) In(in *InExpr) (sql string, err error) {
	element, err := in.element.String(c)
	if err != nil {
		return "", err
	}
	list, err := in.list.String(c)
	if err != nil {
		return "", err
	}
	if in.list.IsCompound() {
		list = "(" + list + ")"
	}
	return element + " IN " + list, nil
}
