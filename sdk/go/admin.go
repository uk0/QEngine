package tsdb

import (
	"fmt"
	"strings"
)

// sqlQuoteStr escapes s for embedding in a single-quoted SQL string literal:
// every single quote is doubled, matching the server lexer's '' escape.
func sqlQuoteStr(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// CreateUser issues
//
//	CREATE USER <name> IDENTIFIED BY '<pass>' [ROLE ADMIN]
//
// over the standard query channel.  The server defaults the role to NORMAL
// when the ROLE clause is omitted.  Requires an ADMIN session when auth is
// enabled.
func (c *Client) CreateUser(name, pass string, admin bool) error {
	sql := fmt.Sprintf("CREATE USER %s IDENTIFIED BY '%s'", name, sqlQuoteStr(pass))
	if admin {
		sql += " ROLE ADMIN"
	}
	_, err := c.Query(sql)
	return err
}

// DropUser issues DROP USER <name>.
func (c *Client) DropUser(name string) error {
	_, err := c.Query("DROP USER " + name)
	return err
}

// Grant issues GRANT <priv> ON <table> TO <user>.  priv is one of SELECT,
// INSERT, DDL, or ALL (server parse.c); table may be "*" for all tables.
func (c *Client) Grant(priv, table, user string) error {
	_, err := c.Query(fmt.Sprintf("GRANT %s ON %s TO %s", priv, table, user))
	return err
}

// Revoke issues REVOKE <priv> ON <table> FROM <user>.  Same priv/table
// grammar as Grant.
func (c *Client) Revoke(priv, table, user string) error {
	_, err := c.Query(fmt.Sprintf("REVOKE %s ON %s FROM %s", priv, table, user))
	return err
}

// CreateDatabase issues CREATE DATABASE <name>.
func (c *Client) CreateDatabase(name string) error {
	_, err := c.Query("CREATE DATABASE " + name)
	return err
}
