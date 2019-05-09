package sqlavro

import (
	"bytes"
	"regexp"
)

var (
	mustNotStartWith, _ = regexp.Compile("[^A-Za-z_]")
	mustNotContains, _  = regexp.Compile("[^A-Za-z0-9_]")
)

func formatString(str string) string {
	strBuf := new(bytes.Buffer)
	if len(str) > 0 {
		firstChar := mustNotStartWith.ReplaceAllString(string(str[0]), "")
		strBuf.WriteString(firstChar)
		if len(str) > 1 {
			str = str[1:]
			str = mustNotContains.ReplaceAllString(str, "")
			strBuf.WriteString(str)
		}
	}
	return strBuf.String()
}
