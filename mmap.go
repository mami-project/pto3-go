// +build !windows

package pto3

import (
	"fmt"
	"os"
	"syscall"
)

// MapFile maps a file into read-only memory. If available, it also
// advises the operating system that access is going to be sequential.
func MapFile(f *os.File) ([]byte, int64, error) {
	// Adapted from https://github.com/golang/exp/blob/master/mmap/mmap_unix.go
	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}

	size := fi.Size()
	if size < 0 {
		return nil, 0, fmt.Errorf("mmap: file %q has negative size", f.Name())
	}
	if size != int64(int(size)) {
		return nil, 0, fmt.Errorf("mmap: file %q is too large", f.Name())
	}

	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, 0, err
	}

	if err := madviseSequential(data); err != nil {
		return nil, 0, err
	}

	return data, size, nil
}

// UnmapFile unmaps bytes that were mapped by MapFile().
func UnmapFile(bytes []byte) error {
	return syscall.Munmap(bytes)
}
