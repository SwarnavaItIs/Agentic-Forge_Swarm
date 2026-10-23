// Explicitly local fake GitHub for repeatable acceptance tests, never real writes.
package main

import (
	"flag"
	"forgeswarm/internal/githubtest"
	"forgeswarm/internal/httpapi"
	"log"
	"net/http"
	"time"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9088", "Numeric loopback TEST/DEMO address")
	flag.Parse()
	if !httpapi.LoopbackAddress(*addr) {
		log.Fatal("TEST_DEMO loopback required")
	}
	server := &http.Server{Addr: *addr, Handler: githubtest.New(), ReadHeaderTimeout: time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, MaxHeaderBytes: 8192}
	log.Fatal(server.ListenAndServe())
}
