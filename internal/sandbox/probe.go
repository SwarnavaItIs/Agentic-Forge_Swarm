package sandbox

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// RunProbe is used only by real-runtime verification. These conservative fixed
// workloads have no user-supplied parameters and run inside the same constraints.
func RunProbe(mode string) error {
	if runtime.GOOS != "linux" {
		return ErrPolicy
	}
	switch mode {
	case "memory":
		blocks := [][]byte{}
		for i := 0; i < 40; i++ {
			b := make([]byte, 4<<20)
			for n := 0; n < len(b); n += 4096 {
				b[n] = 1
			}
			blocks = append(blocks, b)
			runtime.KeepAlive(blocks)
		}
		return fmt.Errorf("memory ceiling did not kill bounded workload")
	case "pids":
		children := []*exec.Cmd{}
		defer func() {
			for _, c := range children {
				_ = c.Process.Kill()
			}
			for _, c := range children {
				_ = c.Wait()
			}
		}()
		for i := 0; i < 80; i++ {
			c := exec.Command("/bin/sleep", "10")
			if err := c.Start(); err != nil {
				if len(children) < 1 {
					return err
				}
				fmt.Println("PIDS_ENFORCED", len(children))
				return nil
			}
			children = append(children, c)
		}
		return fmt.Errorf("PID ceiling did not reject bounded spawn")
	case "cpu":
		throttled := func() (int64, error) {
			b, e := os.ReadFile("/sys/fs/cgroup/cpu.stat")
			if e != nil {
				return 0, e
			}
			fields := strings.Fields(string(b))
			for i := 0; i+1 < len(fields); i += 2 {
				if fields[i] == "nr_throttled" {
					return strconv.ParseInt(fields[i+1], 10, 64)
				}
			}
			return 0, ErrPolicy
		}
		before, err := throttled()
		if err != nil {
			return err
		}
		runtime.GOMAXPROCS(1)
		until := time.Now().Add(1500 * time.Millisecond)
		var n uint64
		for time.Now().Before(until) {
			n++
		}
		runtime.KeepAlive(n)
		after, err := throttled()
		if err != nil || after <= before {
			return fmt.Errorf("CPU quota did not throttle bounded workload")
		}
		fmt.Println("CPU_ENFORCED", after-before)
		return nil
	}
	return ErrPolicy
}
