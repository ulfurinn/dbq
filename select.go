package dbq

type selectColumnSpec struct{}

type tableExprSpec interface {
	Expr
}

type SelectExpr struct {
	q       *Dbq
	columns []selectColumnSpec
	tables  []Expr
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
	var ts Expr
	var parsed bool
	switch spec := spec.(type) {
	case string:
		parsed = true
		ts = Identifier{id: spec}
	case AliasSpec:
		parsed = true
		ts = spec
	}
	if parsed {
		s.tables = append(s.tables, ts)
	}
}

func (s *SelectExpr) Where(whereSpecs ...interface{}) *SelectExpr {
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
