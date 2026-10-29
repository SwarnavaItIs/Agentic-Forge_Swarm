package config

import (
	"testing"
)

func TestPrivateHTTPNeedsTLSAndExactOrigin(t *testing.T) {
	for _, value := range []Config{{HTTPAddr: "0.0.0.0:8443"}, {HTTPAddr: "0.0.0.0:8443", HTTPCert: "cert"}, {HTTPAddr: "0.0.0.0:8443", HTTPCert: "cert", HTTPKey: "key", UIOrigin: "http://control:8443"}, {HTTPAddr: "0.0.0.0:8443", HTTPCert: "cert", HTTPKey: "key", UIOrigin: "https://control:8443/path"}} {
		if value.ValidateHTTP(true) == nil {
			t.Fatal("unsafe nonlocal transport accepted")
		}
	}
	if (Config{HTTPAddr: "127.0.0.1:8080"}).ValidateHTTP(true) != nil {
		t.Fatal("legacy local listener changed")
	}
	if (Config{HTTPAddr: "0.0.0.0:8443", HTTPCert: "cert", HTTPKey: "key", UIOrigin: "https://127.0.0.1:8443"}).ValidateHTTP(true) != nil {
		t.Fatal("private TLS listener rejected")
	}
}
