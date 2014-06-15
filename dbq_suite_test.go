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
	Q := func(e Expression) string {
		sql, err := q.SQLString(e)
		if err != nil {
			Fail(err.Error())
			return ""
		}
		return sql
	}
	QB := func(e Expression) (string, []interface{}) {
		sql, v, err := q.SQL(e, Args{})
		if err != nil {
			Fail(err.Error())
			return "", nil
		}
		return sql, v
	}

	BeforeEach(func() {
		if dberr != nil {
			Fail(dberr.Error())
		}
		q = NewQ(db, PostgresDialect{})
	})

	Describe("Select", func() {
		var s *SelectQuery

		Context("()", func() {
			BeforeEach(func() {
				s = q.Select()
			})
			It("should create a statement", func() {
				Expect(s).NotTo(Equal(nil))
			})
		})

		Describe("From()", func() {
			It("should add a table to the FROM clause", func() {
				s = q.Select().From("t")
				Expect(Q(s)).To(Equal("SELECT * FROM t"))
			})
			It("should add a table with an alias to the FROM clause", func() {
				s = q.Select().From(Alias("table", "t"))
				Expect(Q(s)).To(Equal("SELECT * FROM table AS t"))
			})
			It("should add a subquery to the FROM clause", func() {
				s1 := q.Select().From("t")
				s = q.Select().From(Alias(s1, "s"))
				Expect(Q(s)).To(Equal("SELECT * FROM (SELECT * FROM t) AS s"))
			})
		})

		Describe("Where()", func() {
			It("should add conditions", func() {
				s := q.Select().From("t").Where(Ident("x").Eq(Literal(42)))
				Expect(Q(s)).To(Equal("SELECT * FROM t WHERE x = 42"))
			})
			It("should chain conditions", func() {
				s := q.Select().From("t").Where(Ident("x").Eq(Literal(42))).Where(Ident("y").Eq(Ident("z")))
				Expect(Q(s)).To(Equal("SELECT * FROM t WHERE (x = 42) AND (y = z)"))
			})
			It("should take a map", func() {
				s := q.Select().From("t").Where(Args{"x": 42})
				Expect(Q(s)).To(Equal("SELECT * FROM t WHERE x = 42"))
			})
		})

	})

	Describe("Alias", func() {
		It("should be an expression", func() {
			alias := Alias(Literal(2).Mult(Literal(2)), "x")
			eq := alias.Eq(Literal(4))
			Expect(Q(alias)).To(Equal("(2 * 2) AS x"))
			Expect(Q(eq)).To(Equal("x = 4"))
		})
	})

	Describe("Col()", func() {
		It("should be usable on identifiers", func() {
			t := Identifier("t")
			Expect(Q(t.Col("a"))).To(Equal(`"t"."a"`))
		})
		It("should be usable on aliases", func() {
			t := Alias("table", "t")
			Expect(Q(t.Col("a"))).To(Equal(`"t"."a"`))
		})
		It("should support operators", func() {
			t := Identifier("t")
			expr := t.Col("a").Eq(t.Col("b"))
			Expect(Q(expr)).To(Equal(`"t"."a" = "t"."b"`))
		})
	})

	Describe("Binary()", func() {
		It("should use =", func() { Expect(Q(Literal(1).Eq(1))).To(Equal("1 = 1")) })
		It("should use !=", func() { Expect(Q(Literal(1).NotEq(1))).To(Equal("1 != 1")) })
		It("should use <", func() { Expect(Q(Literal(1).Less(1))).To(Equal("1 < 1")) })
		It("should use <=", func() { Expect(Q(Literal(1).LessEq(1))).To(Equal("1 <= 1")) })
		It("should use >", func() { Expect(Q(Literal(1).Greater(1))).To(Equal("1 > 1")) })
		It("should use >=", func() { Expect(Q(Literal(1).GreaterEq(1))).To(Equal("1 >= 1")) })

		It("should nest", func() {
			expr1 := Literal(2).Plus(Literal(3)).Mult(Literal(5))
			expr2 := Literal(2).Plus(Literal(3).Mult(Literal(5)))
			Expect(Q(expr1)).To(Equal("(2 + 3) * 5"))
			Expect(Q(expr2)).To(Equal("2 + (3 * 5)"))
		})

		It("should turn go values into expressions", func() {
			expr := Binary(42, "=", "42")
			sql, v := QB(expr)
			Expect(sql).To(Equal("42 = $1"))
			Expect(v[0].(string)).To(Equal("42"))
		})
	})

	Describe("Bind()", func() {
		It("should be usable as an expression", func() {
			e := q.Select().From("t").Where(Ident("x").Eq(Bind("myValue")))
			sql, v := QB(e)
			Expect(sql).To(Equal("SELECT * FROM t WHERE x = $1"))
			Expect(v).To(HaveLen(1))
			Expect(v[0]).To(BeNil())
		})
		It("should be reusable", func() {
			e := q.Select().From("t").Where(Bind("myValue").Eq(Bind("myValue")))
			sql, v := QB(e)
			Expect(sql).To(Equal("SELECT * FROM t WHERE $1 = $1"))
			Expect(v).To(HaveLen(1))
			Expect(v[0]).To(BeNil())
		})
		It("should be mappable to values", func() {
			e := q.Select().From("t").Where(Ident("x").Eq(Bind("myValue")))
			sql, v, _ := q.SQL(e, Args{"myValue": 42})
			Expect(sql).To(Equal("SELECT * FROM t WHERE x = $1"))
			Expect(v).To(HaveLen(1))
			Expect(v[0]).To(Equal(42))
		})
	})

})
