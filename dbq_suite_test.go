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

		Context("()", func() {
			var s *SelectExpr
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

	})

})
