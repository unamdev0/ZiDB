package storage

import (
	"fmt"
	"github.com/unamdev0/ZiDB/pkg/constants"
	"unsafe"
)

type Row struct {
	Id       uint32
	Username [constants.COLUMN_USERNAME_SIZE]byte
	Email    [constants.COLUMN_EMAIL_SIZE]byte
}

func Store(to uintptr, data *Row) {
	*(*uint32)(unsafe.Pointer(to)) = data.Id
	usernameSlice := (*[constants.USERNAME_SIZE]byte)(unsafe.Pointer(to + uintptr(constants.USERNAME_OFFSET)))
	copy(usernameSlice[:], data.Username[:])
	emailSlice := (*[constants.EMAIL_SIZE]byte)(unsafe.Pointer(to + uintptr(constants.EMAIL_OFFSET)))
	copy(emailSlice[:], data.Email[:])
}

func Read(data *Row, from uintptr) {
	data.Id = *(*uint32)(unsafe.Pointer(from))
	copy(data.Username[:], (*(*[constants.USERNAME_SIZE]byte)(unsafe.Pointer(from + uintptr(constants.USERNAME_OFFSET))))[:])
	copy(data.Email[:], (*(*[constants.EMAIL_SIZE]byte)(unsafe.Pointer(from + uintptr(constants.EMAIL_OFFSET))))[:])
}

func PrintRow(row *Row) string {
	return fmt.Sprintf("\nROW ID->%d, USERNAME->%s, EMAIL->%s\n", row.Id, row.Username, row.Email)
}
