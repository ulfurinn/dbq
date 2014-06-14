package dbq

type dialect interface {
	SQLString(s Expression) (string, error)
	SQL(s Expression, values Args) (query string, outValues []interface{}, err error)
}
