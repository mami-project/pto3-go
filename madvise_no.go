// +build darwin OR windows

package pto3

func madviseSequential(b []byte) error {
	return nil
}
