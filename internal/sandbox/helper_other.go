//go:build !linux

package sandbox

import "os"

func verifyKernel(Policy) error                                 { return ErrPolicy }
func openConfined(root *os.Root, name string) (*os.File, error) { return root.Open(name) }
