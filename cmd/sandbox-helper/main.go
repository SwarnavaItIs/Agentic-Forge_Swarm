// Immutable Linux-only PID 1 helper in the prepared read-only execution image.
package main

import (
	"context"
	"encoding/json"
	"forgeswarm/internal/sandbox"
	"io"
	"os"
	"runtime"
	"time"
)

func main() {
	// Survives a gateway crash: command cancellation alone does not cover a
	// blocked input stream or artifact collector. Exiting namespace PID 1 also
	// terminates its remaining descendants. No repository-supplied timeout.
	watchdog := time.AfterFunc(63*time.Second, func() { os.Exit(124) })
	defer watchdog.Stop()
	if runtime.GOOS == "linux" && len(os.Args) == 3 && os.Args[1] == "probe" {
		if sandbox.RunProbe(os.Args[2]) != nil {
			os.Exit(2)
		}
		return
	}
	if runtime.GOOS != "linux" || len(os.Args) != 2 || os.Args[1] != "execute" {
		os.Exit(2)
	}
	body, err := io.ReadAll(io.LimitReader(os.Stdin, 2*sandbox.MaxFixture+32769))
	var p sandbox.Payload
	if err != nil || sandbox.Decode(body, &p, 2*sandbox.MaxFixture+32768) != nil {
		os.Exit(2)
	}
	if !p.Request.Valid() {
		os.Exit(2)
	}
	watchdog.Reset(time.Duration(p.Request.Policy.TimeoutMS)*time.Millisecond + 2*time.Second)
	result, err := sandbox.ExecuteHelper(context.Background(), p)
	if err != nil {
		os.Exit(2)
	}
	if json.NewEncoder(os.Stdout).Encode(result) != nil {
		os.Exit(2)
	}
}
