package dbq

import (
	"database/sql"
	"os"

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

func testenv(key, value string) {
	if os.Getenv(key) == "" {
		os.Setenv(key, value)
	}
}

func exec(db *sql.DB, q string) {
	_, err := db.Exec(q)
	if err != nil {
		Fail(err.Error())
	}
}

func testschema(db *sql.DB) {
	exec(db, "CREATE TABLE test ( id serial, a integer, b integer, primary key (id) )")
}

var _ = Describe("dbq", func() {
	testenv("PGDATABASE", Db)
	testenv("PGHOST", Host)
	testenv("PGSSLMODE", "disable")
	testenv("PGUSER", User)
	db, dberr := sql.Open("postgres", "")

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
		_, err := db.Exec("BEGIN")
		if err != nil {
			Fail(err.Error())
		}

	})

	AfterEach(func() {
		db.Exec("ROLLBACK")
	})

	Describe("Expression", func() {

		It("should be castable", func() {
			e := Literal(2).Plus(2).Cast("double precision")
			Expect(Q(e)).To(Equal("(2 + 2)::double precision"))
		})

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

		It("should accept Distinct", func() {
			e := q.Select(Distinct{}).From("t")
			Expect(Q(e)).To(Equal("SELECT DISTINCT * FROM t"))
		})
		It("should accept a column list", func() {
			e := q.Select(Ident("a"), "a1", Alias("b", "b_alias"), Alias(Literal(2).Plus(2), "c")).From("t")
			Expect(Q(e)).To(Equal("SELECT a, a1, b AS b_alias, (2 + 2) AS c FROM t"))
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
			It("should join two tables with a comma", func() {
				s = q.Select().From("t1", "t2")
				Expect(Q(s)).To(Equal("SELECT * FROM t1 , t2"))
			})
			It("should use JOIN ON", func() {
				s = q.Select().From("t1", Join("t2", On(Ident("c1").Eq(Ident("c2")))))
				Expect(Q(s)).To(Equal("SELECT * FROM t1 INNER JOIN t2 ON (c1 = c2)"))
			})
			It("should use JOIN USING", func() {
				s = q.Select().From("t1", Join("t2", Using(Ident("c1"))))
				Expect(Q(s)).To(Equal("SELECT * FROM t1 INNER JOIN t2 USING (c1)"))
			})
			It("should join using column expressions", func() {
				t1 := Ident("t1")
				t2 := Ident("t2")
				s = q.Select().From(t1, Join(t2, On(t1.Col("c1").Eq(t2.Col("c2")))))
				Expect(Q(s)).To(Equal(`SELECT * FROM t1 INNER JOIN t2 ON ("t1"."c1" = "t2"."c2")`))
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
			It("should take lists in a map", func() {
				s := q.Select().From("t").Where(Args{"x": []int{42, 57}}).Where(Args{"y": []string{"c"}})
				sql, v := QB(s)
				Expect(sql).To(Equal("SELECT * FROM t WHERE x IN ($1,$2) AND y IN ($3)"))
				Expect(v).To(HaveLen(3))
				Expect(v[0]).To(Equal(42))
				Expect(v[1]).To(Equal(57))
				Expect(v[2]).To(Equal("c"))
			})
		})

		Describe("Group()", func() {
			It("should use expressions", func() {
				e := q.Select().From("t").Group("a")
				Expect(Q(e)).To(Equal("SELECT * FROM t GROUP BY a"))
			})
		})

		Describe("Order()", func() {
			It("should sort by expression", func() {
				e := q.Select().From("t").OrderBy(Order("x", "asc"))
				Expect(Q(e)).To(Equal("SELECT * FROM t ORDER BY x ASC"))
			})
		})

		Describe("Into()", func() {
			It("should accept a scalar", func() {
				testschema(db)
				_, e := db.Exec("INSERT INTO test (a, b) VALUES (42, 1)")
				if e != nil {
					Fail(e.Error())
				}
				var a int
				e = q.Select(Ident("a")).From("test").Into(&a)
				if e != nil {
					Fail(e.Error())
				}
				Expect(a).To(Equal(42))
			})
			It("should accept a list of scalars", func() {
				testschema(db)
				_, e := db.Exec("INSERT INTO test (a, b) VALUES (42, 1), (43, 2)")
				if e != nil {
					Fail(e.Error())
				}
				var a []int
				e = q.Select(Ident("a")).From("test").Into(&a)
				if e != nil {
					Fail(e.Error())
				}
				Expect(a).To(HaveLen(2))
				Expect(a[0]).To(Equal(42))
				Expect(a[1]).To(Equal(43))
			})
			It("should accept a struct", func() {
				testschema(db)
				_, e := db.Exec("INSERT INTO test (a, b) VALUES (42, 1)")
				if e != nil {
					Fail(e.Error())
				}
				var a struct {
					ID int
					A  int
					B  int
				}
				e = q.Select().From("test").Into(&a)
				if e != nil {
					Fail(e.Error())
				}
				Expect(a.A).To(Equal(42))
				Expect(a.B).To(Equal(1))
			})
			It("should accept a list of structs", func() {
				testschema(db)
				_, e := db.Exec("INSERT INTO test (a, b) VALUES (42, 1), (43, 2)")
				if e != nil {
					Fail(e.Error())
				}
				var a []struct {
					ID int
					A  int
					B  int
				}
				e = q.Select().From("test").Into(&a)
				if e != nil {
					Fail(e.Error())
				}
				Expect(a).To(HaveLen(2))
				Expect(a[0].A).To(Equal(42))
				Expect(a[0].B).To(Equal(1))
				Expect(a[1].A).To(Equal(43))
				Expect(a[1].B).To(Equal(2))
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
			t := Ident("t")
			Expect(Q(t.Col("a"))).To(Equal(`"t"."a"`))
		})
		It("should be usable on aliases", func() {
			t := Alias("table", "t")
			Expect(Q(t.Col("a"))).To(Equal(`"t"."a"`))
		})
		It("should support operators", func() {
			t := Ident("t")
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
		It("should support = with nulls", func() {
			expr := Ident("x").Eq(nil)
			Expect(Q(expr)).To(Equal("x IS NULL"))
		})
		It("should support != with nulls", func() {
			expr := Ident("x").NotEq(nil)
			Expect(Q(expr)).To(Equal("x IS NOT NULL"))
		})
	})

	Describe("Bind()", func() {
		It("should be mappable to values", func() {
			e := q.Select().From("t").Where(Ident("x").Eq(Bind("myValue")))
			sql, v, _ := q.SQL(e, Args{"myValue": 42})
			Expect(sql).To(Equal("SELECT * FROM t WHERE x = ($1)"))
			Expect(v).To(HaveLen(1))
			Expect(v[0]).To(Equal(42))
		})
		It("should be reusable", func() {
			e := q.Select().From("t").Where(Bind("myValue").Eq(Bind("myValue")))
			sql, v, _ := q.SQL(e, Args{"myValue": 42})
			Expect(sql).To(Equal("SELECT * FROM t WHERE ($1) = ($1)"))
			Expect(v).To(HaveLen(1))
			Expect(v[0]).To(Equal(42))
		})
		It("should play nicely with string literals", func() {
			e := q.Select().From("t").Where(Ident("a").Eq("meh")).Where(Ident("x").Eq(Bind("myValue")))
			sql, v, _ := q.SQL(e, Args{"myValue": 42})
			Expect(sql).To(Equal("SELECT * FROM t WHERE (a = $1) AND (x = ($2))"))
			Expect(v).To(HaveLen(2))
			Expect(v[0]).To(Equal("meh"))
			Expect(v[1]).To(Equal(42))
		})
		It("should support = with nulls", func() {
			e := q.Select().From("t").Where(Ident("x").Eq(Bind("myValue")))
			sql, v, _ := q.SQL(e, Args{"myValue": nil})
			Expect(sql).To(Equal("SELECT * FROM t WHERE x IS NULL"))
			Expect(v).To(BeEmpty())
		})
	})

	Describe("In()", func() {
		It("should take a go value and make it into placeholders", func() {
			s := Ident("a").In([]int{1, 2, 5})
			sql, v := QB(s)
			Expect(sql).To(Equal("a IN ($1,$2,$3)"))
			Expect(v).To(HaveLen(3))
			Expect(v[0]).To(Equal(1))
			Expect(v[1]).To(Equal(2))
			Expect(v[2]).To(Equal(5))
		})
		It("should take a binding", func() {
			s := Ident("a").In(Bind("myArray"))
			sql, v, _ := q.SQL(s, Args{"myArray": []string{"a", "b", "c"}})
			Expect(sql).To(Equal("a IN ($1,$2,$3)"))
			Expect(v).To(HaveLen(3))
			Expect(v[0]).To(Equal("a"))
			Expect(v[1]).To(Equal("b"))
			Expect(v[2]).To(Equal("c"))
		})
	})

	Describe("Func()", func() {
		It("should generate function calls", func() {
			Expect(Q(Func("now"))).To(Equal("now()"))
		})
		It("should generate function calls with arguments", func() {
			Expect(Q(Func("myfunc", Literal(42)))).To(Equal("myfunc(42)"))
		})
	})
	Describe("AggFunc()", func() {
		It("should use DISTINCT", func() {
			Expect(Q(AggFunc("count", Distinct{}, Ident("x")))).To(Equal("count(DISTINCT x)"))
		})
		It("should use ALL", func() {
			Expect(Q(AggFunc("count", All{}, Ident("x")))).To(Equal("count(ALL x)"))
		})
		It("should use order", func() {
			e := AggFunc("count", Ident("x"), OrderBy(Order("x", "asc")))
			Expect(Q(e)).To(Equal("count(x ORDER BY x ASC)"))
		})
	})

})
