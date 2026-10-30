// Package injected names the placeholder a substrate actor sends where a
// credential belongs. The actor holds no credential: the egress gateway in front
// of the destination replaces the header with a Secret's value, and only when the
// request carries the header, so the header has to be there.
package injected

// File stands in for a token file, which an actor does not have: a bearer token
// read from it is the placeholder. It is no path anything can open.
const File = ":injected:"

// Token is what is sent in place of a credential.
const Token = "replaced-by-egress"

// Read returns the token a file holds, trimmed by the caller; for File it is the
// placeholder and ok is true.
func Read(file string) (token string, ok bool) {
	if file == File {
		return Token, true
	}
	return "", false
}
