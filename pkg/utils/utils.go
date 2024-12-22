package utils

import "unsafe"

func ReadUint32(ptr uintptr) uint32 {
	return *(*uint32)(unsafe.Pointer(ptr))
}
