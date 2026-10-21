//go:build linux

package sandbox

import (
	"os"
	"strconv"
	"strings"
	"syscall"
)

// A malicious replacement with a FIFO must not block between Lstat and Fstat.
// Root still confines all path resolution; nonregular handles are rejected.
func openConfined(root *os.Root, name string) (*os.File, error) {
	return root.OpenFile(name, os.O_RDONLY|syscall.O_NONBLOCK, 0)
}

// Check the actual kernel boundary before loading or executing repository code.
// A daemon setting or unsupported controller must never become silent fallback.
func verifyKernel(p Policy) error {
	if os.Getuid() != 10001 {
		return ErrPolicy
	}
	body, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return ErrPolicy
	}
	fields := map[string]string{}
	for _, line := range strings.Split(string(body), "\n") {
		parts := strings.Fields(line)
		if len(parts) >= 2 {
			fields[parts[0]] = parts[1]
		}
	}
	if fields["CapEff:"] != "0000000000000000" || fields["NoNewPrivs:"] != "1" || fields["Seccomp:"] != "2" {
		return ErrPolicy
	}
	for name, want := range map[string]int64{"memory.max": p.MemoryBytes, "memory.swap.max": 0, "pids.max": int64(p.PIDs)} {
		body, err = os.ReadFile("/sys/fs/cgroup/" + name)
		if err != nil {
			return ErrPolicy
		}
		actual, e := strconv.ParseInt(strings.TrimSpace(string(body)), 10, 64)
		if e != nil || actual != want {
			return ErrPolicy
		}
	}
	body, err = os.ReadFile("/sys/fs/cgroup/cpu.max")
	if err != nil {
		return ErrPolicy
	}
	cpu := strings.Fields(string(body))
	if len(cpu) != 2 {
		return ErrPolicy
	}
	quota, e := strconv.ParseInt(cpu[0], 10, 64)
	if e != nil {
		return ErrPolicy
	}
	period, e := strconv.ParseInt(cpu[1], 10, 64)
	if e != nil || quota <= 0 || period <= 0 || quota*1000 != period*int64(p.CPUMillis) {
		return ErrPolicy
	}
	interfaces, err := os.ReadDir("/sys/class/net")
	if err != nil || len(interfaces) != 1 || interfaces[0].Name() != "lo" {
		return ErrPolicy
	}
	var root syscall.Statfs_t
	if syscall.Statfs("/", &root) != nil || root.Flags&1 == 0 {
		return ErrPolicy
	}
	for name, want := range map[string]int64{"/workspace": p.WorkspaceBytes, "/tmp": p.TemporaryBytes, "/dev/shm": 1 << 20} {
		var actual syscall.Statfs_t
		if syscall.Statfs(name, &actual) != nil || actual.Type != 0x01021994 || actual.Bsize <= 0 || actual.Blocks*uint64(actual.Bsize) != uint64(want) {
			return ErrPolicy
		}
	}
	return nil
}
