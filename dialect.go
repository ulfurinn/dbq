package dbq

type dialect interface {
	SelectString(s *SelectExpr) string
	SelectSQL(s *SelectExpr, values map[string]interface{}) (query string, outValues []interface{}, err error)
}
