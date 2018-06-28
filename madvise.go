// +build !darwin,!windows

package pto3

import (
	"syscall"
)

func madviseSequential(b []byte) error {
	return syscall.Madvise(b, syscall.MADV_SEQUENTIAL)
}
