package dbq

import (
	"database/sql"

	_ "github.com/lib/pq"
)

type Dbq struct {
	d  dialect
	db *sql.DB
}

type aliasSpec struct {
	expr  interface{}
	alias string
}

type Identifier struct {
	id string
}

type Subquery struct {
	q *SelectExpr
}

func (id Identifier) tableExpr() string {
	return id.id
}

func (q Subquery) tableExpr() string {
	return q.q.String()
}

func New(db *sql.DB, d dialect) *Dbq {
	return &Dbq{
		d:  d,
		db: db,
	}
}

func (q *Dbq) Select(selectSpec ...interface{}) *SelectExpr {
	stmt := &SelectExpr{q: q}
	return stmt.parseSelectSpec(selectSpec...)
}

func Alias(id, a string) aliasSpec {
	return aliasSpec{expr: Identifier{id}, alias: a}
}
