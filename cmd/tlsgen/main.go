// tlsgen creates a local private CA and scoped service certificates. It never
// replaces existing keys; the CA key is not mounted into any service.
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func main() {
	out := flag.String("out", "", "new private certificate directory")
	hosts := flag.String("hosts", "", "optional comma-separated private service DNS/IP SANs")
	flag.Parse()
	if err := generate(*out, *hosts); err != nil {
		fmt.Fprintln(os.Stderr, "certificate preparation failed")
		os.Exit(1)
	}
}
func generate(out, hosts string) error {
	if out == "" {
		return errors.New("directory required")
	}
	if err := os.Mkdir(out, 0700); err != nil {
		return err
	}
	serial := func() *big.Int {
		v, e := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
		if e != nil {
			panic(e)
		}
		return v
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	ca := &x509.Certificate{SerialNumber: serial(), Subject: pkix.Name{CommonName: "ForgeSwarm private installation CA"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().AddDate(1, 0, 0), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		return err
	}
	write := func(name, kind string, body []byte, mode os.FileMode) error {
		return os.WriteFile(filepath.Join(out, name), pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: body}), mode)
	}
	if err = write("ca.pem", "CERTIFICATE", der, 0644); err != nil {
		return err
	}
	// Use the encoded parent's generated subject key identifier when issuing
	// leaves. Strict OpenSSL chain builders require the matching authority ID.
	ca, err = x509.ParseCertificate(der)
	if err != nil {
		return err
	}
	raw, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return err
	}
	if err = write("ca-key.pem", "PRIVATE KEY", raw, 0600); err != nil {
		return err
	}
	names := []string{"localhost"}
	if hosts != "" {
		names = append(names, strings.Split(hosts, ",")...)
	}
	if len(names) > 16 {
		return errors.New("SAN ceiling")
	}
	for _, service := range []string{"api", "coordinator", "broker", "db"} {
		serverKey, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if e != nil {
			return e
		}
		cert := &x509.Certificate{SerialNumber: serial(), Subject: pkix.Name{CommonName: service}, NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, DNSNames: []string{service}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}}
		for _, name := range names {
			if ip := net.ParseIP(name); ip != nil {
				cert.IPAddresses = append(cert.IPAddresses, ip)
			} else if name != "" && len(name) < 254 && !strings.ContainsAny(name, "/\\\n\r :@") {
				cert.DNSNames = append(cert.DNSNames, name)
			} else {
				return errors.New("invalid SAN")
			}
		}
		body, e := x509.CreateCertificate(rand.Reader, cert, ca, &serverKey.PublicKey, key)
		if e != nil {
			return e
		}
		if e = write(service+".pem", "CERTIFICATE", body, 0644); e != nil {
			return e
		}
		raw, e := x509.MarshalPKCS8PrivateKey(serverKey)
		if e != nil {
			return e
		}
		if e = write(service+"-key.pem", "PRIVATE KEY", raw, 0600); e != nil {
			return e
		}
	}
	fmt.Println("Private CA and scoped TLS certificates created.")
	return nil
}
