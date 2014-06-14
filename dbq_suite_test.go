package dbq

import (
	"database/sql"
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

const User = "dbqtestuser"
const Db = "dbqtestdb"
const Host = "localhost"

func TestDbq(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Dbq Suite")
}

func dsn() string {
	return fmt.Sprintf("user=%s dbname=%s host=%s", User, Db, Host)
}

var _ = Describe("dbq", func() {
	db, dberr := sql.Open("postgres", dsn())

	var q *Dbq

	BeforeEach(func() {
		if dberr != nil {
			Fail(dberr.Error())
		}
		q = New(db, PostgresDialect{})
	})

	Describe("Select", func() {
		var s *SelectExpr

		Context("()", func() {
			BeforeEach(func() {
				s = q.Select()
			})
			It("should create a statement", func() {
				Expect(s).NotTo(Equal(nil))
			})
			It("should select all", func() {
				Expect(s.isSelectStar()).To(Equal(true))
			})
		})

		Describe("From()", func() {
			It("should add a table to the FROM clause", func() {
				s = q.Select().From("t")
				Expect(s.String()).To(Equal("SELECT * FROM t"))
			})
			It("should add a table with an alias to the FROM clause", func() {
				s = q.Select().From(Alias("table", "t"))
				Expect(s.String()).To(Equal("SELECT * FROM table AS t"))
			})
			It("should add a subquery to the FROM clause", func() {
				s1 := q.Select().From("t")
				s = q.Select().From(Alias(s1, "s"))
				Expect(s.String()).To(Equal("SELECT * FROM (SELECT * FROM t) AS s"))
			})
		})

		Describe("Where()", func() {
			It("should add conditions", func() {
				expr := q.Select().From("t").Where(Ident("x").Eq(Literal(42)))
				Expect(expr.String()).To(Equal("SELECT * FROM t WHERE x = 42"))
			})
			It("should chain conditions", func() {
				expr := q.Select().From("t").Where(Ident("x").Eq(Literal(42))).Where(Ident("y").Eq(Ident("z")))
				Expect(expr.String()).To(Equal("SELECT * FROM t WHERE (x = 42) AND (y = z)"))
			})
			It("should take a map", func() {
				expr := q.Select().From("t").Where(Args{"x": 42})
				Expect(expr.String()).To(Equal("SELECT * FROM t WHERE x = 42"))
			})
		})

	})

	Describe("Alias", func() {
		It("should be an expression", func() {
			alias := Alias(Literal(2).Mult(Literal(2)), "x")
			eq := alias.Eq(Literal(4))
			Expect(alias.String()).To(Equal("(2 * 2) AS x"))
			Expect(eq.String()).To(Equal("x = 4"))
		})
	})

	Describe("Col()", func() {
		It("should be usable on identifiers", func() {
			t := Identifier("t")
			Expect(t.Col("a").String()).To(Equal("t.a"))
		})
		It("should be usable on aliases", func() {
			t := Alias("table", "t")
			Expect(t.Col("a").String()).To(Equal("t.a"))
		})
		It("should support operators", func() {
			t := Identifier("t")
			expr := t.Col("a").Eq(t.Col("b"))
			Expect(expr.String()).To(Equal("t.a = t.b"))
		})
	})

	Describe("Binary()", func() {
		It("should nest", func() {
			expr1 := Literal(2).Plus(Literal(3)).Mult(Literal(5))
			expr2 := Literal(2).Plus(Literal(3).Mult(Literal(5)))
			Expect(expr1.String()).To(Equal("(2 + 3) * 5"))
			Expect(expr2.String()).To(Equal("2 + (3 * 5)"))
		})
		It("should cast into expressions", func() {
			expr := Binary(42, "=", "42")
			Expect(expr.String()).To(Equal("42 = '42'"))
		})
	})

})
