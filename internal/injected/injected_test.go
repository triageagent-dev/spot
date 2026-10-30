package injected

import "testing"

func TestOnlyThePlaceholderFileReadsAsThePlaceholder(t *testing.T) {
	if tok, ok := Read(File); !ok || tok != Token {
		t.Errorf("%q %v", tok, ok)
	}
	if _, ok := Read("/secrets/otel/token"); ok {
		t.Error("a real path read as the placeholder")
	}
	if _, ok := Read(""); ok {
		t.Error("no file read as the placeholder")
	}
}
