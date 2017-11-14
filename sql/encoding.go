/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package sql

import (
	"fmt"
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
	"strconv"
)

type charsetEncoding map[string]encoding.Encoding

var charsetEncodingMap charsetEncoding

func init() {
	charsetEncodingMap = make(map[string]encoding.Encoding)
	// Begin mappings
	charsetEncodingMap["latin1"] = charmap.Windows1252
}

// EscapeName will escape a db/table/column/... name by wrapping with backticks.
// It is not fool proof. I'm just trying to do the right thing here, not solving
// SQL injection issues, which should be irrelevant for this tool.
func EscapeName(name string) string {
	if unquoted, err := strconv.Unquote(name); err == nil {
		name = unquoted
	}
	return fmt.Sprintf("`%s`", name)
}
