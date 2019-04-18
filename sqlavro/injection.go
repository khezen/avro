package sqlavro

import "bytes"

var (
	rogueRunes = map[rune]struct{}{
		'\'': struct{}{},
		'`':  struct{}{},
		'(':  struct{}{},
		';':  struct{}{},
		'*':  struct{}{},
		'\\': struct{}{},
	}
)

func sqlEscape(input string) string {
	outputBuf := bytes.NewBuffer([]byte{})
	for _, r := range input {
		if _, ok := rogueRunes[r]; ok {
			outputBuf.WriteRune('\\')
		}
		outputBuf.WriteRune(r)
	}
	return outputBuf.String()
}
