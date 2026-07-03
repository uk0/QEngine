package tsdb

import (
	"fmt"
	"strings"
)

// udfTypeKeyword maps a wire column type byte to its CREATE FUNCTION
// keyword.  UDF ABI v1 supports only TIMESTAMP, INT64 and FLOAT64.
func udfTypeKeyword(t byte) (string, error) {
	switch t {
	case TypeTimestamp:
		return "TIMESTAMP", nil
	case TypeInt64:
		return "INT64", nil
	case TypeFloat64:
		return "FLOAT64", nil
	}
	return "", fmt.Errorf("tsdb: UDF v1 supports TIMESTAMP/INT64/FLOAT64, got type byte %d", t)
}

// RegisterUDF registers a scalar UDF by issuing
//
//	CREATE FUNCTION <name>(<argTypes>...) RETURNS <retType> FROM '<soPath>' SYMBOL '<symbol>'
//
// argTypes and retType use the wire type bytes (TypeTimestamp, TypeInt64,
// TypeFloat64).  symbol may be empty — the server then defaults the exported
// symbol to the function name.  Registration is metadata-only on the server
// (dlopen happens lazily at first call), so the .so must be present at
// soPath on every server node before the function's first use.  Requires an
// ADMIN session when auth is enabled.
func (c *Client) RegisterUDF(name string, argTypes []byte, retType byte, soPath, symbol string) error {
	var sb strings.Builder
	sb.WriteString("CREATE FUNCTION ")
	sb.WriteString(name)
	sb.WriteByte('(')
	for i, t := range argTypes {
		kw, err := udfTypeKeyword(t)
		if err != nil {
			return err
		}
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(kw)
	}
	sb.WriteByte(')')
	ret, err := udfTypeKeyword(retType)
	if err != nil {
		return err
	}
	sb.WriteString(" RETURNS ")
	sb.WriteString(ret)
	sb.WriteString(" FROM '")
	sb.WriteString(soPath)
	sb.WriteByte('\'')
	if symbol != "" {
		sb.WriteString(" SYMBOL '")
		sb.WriteString(symbol)
		sb.WriteByte('\'')
	}
	_, err = c.Query(sb.String())
	return err
}

// DropUDF unregisters a scalar UDF by issuing DROP FUNCTION <name>.
func (c *Client) DropUDF(name string) error {
	_, err := c.Query("DROP FUNCTION " + name)
	return err
}
