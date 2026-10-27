//go:build cloud

package durable

import (
	"encoding/json"
	"flag"
	"os"
	"testing"

	"go.temporal.io/sdk/client"
)

// The same tests, against a live service. Run them by hand:
//
//	go test -tags cloud ./internal/durable -run 'Test(A|The)' -cloud-conn <file>
//
// The file is JSON: {"address", "namespace", "apiKeyFile", "payloadKeyFile"}.
// The key files are paths; nothing here reads a credential from an argument or
// a variable. Every payload goes encrypted, so the service never holds the text
// of a test turn.
var cloudConn = flag.String("cloud-conn", "", "JSON file naming a live Temporal service: address, namespace, apiKeyFile, payloadKeyFile")

type conn struct {
	Address        string `json:"address"`
	Namespace      string `json:"namespace"`
	APIKeyFile     string `json:"apiKeyFile"`
	PayloadKeyFile string `json:"payloadKeyFile"`
}

func loadConn(t *testing.T) conn {
	t.Helper()
	b, err := os.ReadFile(*cloudConn)
	if err != nil {
		t.Fatal(err)
	}
	var c conn
	if err := json.Unmarshal(b, &c); err != nil {
		t.Fatal(err)
	}
	if c.Address == "" || c.Namespace == "" || c.APIKeyFile == "" || c.PayloadKeyFile == "" {
		t.Fatal("the connection file needs address, namespace, apiKeyFile and payloadKeyFile")
	}
	return c
}

func init() {
	useCloud = func() bool { return *cloudConn != "" }
	slow = 4
	testClient = func(t *testing.T, o Options) client.Client {
		c := loadConn(t)
		o.Address, o.Namespace, o.APIKeyFile, o.PayloadKeyFile = c.Address, c.Namespace, c.APIKeyFile, c.PayloadKeyFile
		cl, err := Dial(o)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(cl.Close)
		return cl
	}
	rawClient = func(t *testing.T) client.Client {
		c := loadConn(t)
		cl, err := Dial(Options{Address: c.Address, Namespace: c.Namespace, APIKeyFile: c.APIKeyFile})
		if err != nil {
			t.Fatal(err)
		}
		return cl
	}
}
