package main

import (
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
)

func TestIssuedLeavesVerifyAndBindAuthority(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "tls")
	if err := generate(dir, "10.0.1.10"); err != nil {
		t.Fatal(err)
	}
	read := func(name string) *x509.Certificate {
		body, err := os.ReadFile(filepath.Join(dir, name+".pem"))
		if err != nil {
			t.Fatal(err)
		}
		block, _ := pem.Decode(body)
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			t.Fatal(err)
		}
		return cert
	}
	ca := read("ca")
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	for _, name := range []string{"api", "coordinator", "broker", "db"} {
		cert := read(name)
		if len(cert.AuthorityKeyId) == 0 {
			t.Fatal("missing authority key identifier")
		}
		if _, err := cert.Verify(x509.VerifyOptions{Roots: roots, DNSName: name}); err != nil {
			t.Fatal(err)
		}
		if _, err := cert.Verify(x509.VerifyOptions{Roots: roots, DNSName: "10.0.1.10"}); err != nil {
			t.Fatal(err)
		}
		if name != "coordinator" {
			if _, err := cert.Verify(x509.VerifyOptions{Roots: roots, DNSName: "coordinator"}); err == nil {
				t.Fatal("service certificate impersonates coordinator DNS")
			}
		}
	}
	if err := generate(dir, ""); err == nil {
		t.Fatal("overwrote keys")
	}
}
