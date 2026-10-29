package config

import (
	"errors"
	"net"
	"net/url"
)

// Nonlocal listeners are opt-in, authenticated HTTPS with one exact UI origin.
// Forwarded headers never grant trust or select that origin.
func (c Config) ValidateHTTP(requireListenerAuth bool) error {
	if (c.HTTPCert == "") != (c.HTTPKey == "") {
		return errors.New("HTTP TLS certificate and key must be configured together")
	}
	if c.UIOrigin != "" {
		u, err := url.Parse(c.UIOrigin)
		if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" || c.HTTPCert == "" {
			return errors.New("UI_ORIGIN requires an exact HTTPS origin and configured TLS")
		}
	}
	host, _, _ := net.SplitHostPort(c.HTTPAddr)
	if requireListenerAuth && host != "localhost" && !Loopback(c.HTTPAddr) && (c.HTTPCert == "" || c.UIOrigin == "") {
		return errors.New("nonlocal HTTP requires TLS, operator authentication and UI_ORIGIN")
	}
	return nil
}
