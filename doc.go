/*
Package dbq builds SQL ASTs and generates query strings from them.

dbq works by dynamically constructing SQL syntax trees and generating queries from them. It can be used separately from any data modeling packages and even without an open database connection.

Examples in this document assume the package is dot-imported for brevity.

Getting started

A *Dbq value is needed to generate queries. Obtain it like this:
	q := NewQ(dbconn, PostgresDialect{})

dbconn doesn't need to be a valid connection unless you want to use dbq for loading data (which is only partially implemented at the moment). PostgresDialect is currently the only available dialect.

Expressions and composition

Two basic types in dbq are Node and Expression. Everything is a Node; most things are also Expressions. An Expression can be combined with other Expressions to form more complex ones.

The basic expressions are literals, created with Literal(), and identifiers, created with Ident(). They can be combined with binary operations to abritrary levels of nesting. Other expressions are aliases, column references (obtained from types that implement Tabular), and entire select queries, allowing subqueries as values.

Keep in mind that dbq is generally very liberal in what types of arguments it accepts, and not all combinations result in valid SQL. Aliases are one such example: there is no structural difference between a table alias and a column/expression alias, but the database engine will complain if you mix them up.

SELECT

A SELECT expression has the following basic structure:
	q.Select(columns...).From(tables...).Where(conditions...).Limit(n).Offset(n)

Each of the methods returns the same *SelectQuery value, so you can chain them as you like. Multiple calls to the same method will accumulate arguments.

Column list

Any Expression can be used as a column. You can wrap them with Alias() to give them a name. A bare string is also accepted here, and is identical to using an identifier.

Table list

...

Condition list

...

Limit and Offset

...

*/
package dbq
