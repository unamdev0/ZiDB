package constants

import (
	"errors"
	"math"
	"unsafe"
)

var (
	ErrUnrecognizedStatement = errors.New("unrecognized statement")
	ErrUnrecognizedCommand   = errors.New("unrecognized command")
	ErrSyntaxError           = errors.New("syntax error")
	ErrOutOfBoundPageNum     = errors.New("page number entered is greater than total DB capacity")
	ErrReadingFile           = errors.New("can't read db file")
	ErrNoContentFound        = errors.New("page does not exist")
	ErrDataNotSaved          = errors.New("could not save data to DB")
)

const INVALID_PAGE_NUM = math.MaxUint32

const (
	COLUMN_USERNAME_SIZE = 32
	COLUMN_EMAIL_SIZE    = 255
	PAGE_SIZE            = 4096
	ROWS_PER_PAGE        = 100
	MAX_PAGE_NUM         = 100
	ID_OFFSET            = 0
	ID_SIZE              = unsafe.Sizeof(uint32(0))
	USERNAME_SIZE        = COLUMN_USERNAME_SIZE
	EMAIL_SIZE           = COLUMN_EMAIL_SIZE
	USERNAME_OFFSET      = ID_OFFSET + ID_SIZE
	EMAIL_OFFSET         = USERNAME_OFFSET + USERNAME_SIZE
	ROW_SIZE             = ID_SIZE + EMAIL_SIZE + USERNAME_SIZE
)

const (
	NODE_TYPE_SIZE                = unsafe.Sizeof(uint8(0))
	NODE_TYPE_OFFSET              = 0
	IS_ROOT_SIZE                  = unsafe.Sizeof(uint8(0))
	IS_ROOT_OFFSET                = NODE_TYPE_SIZE
	PARENT_POINTER_SIZE           = unsafe.Sizeof(uint32(0))
	PARENT_POINTER_OFFSET         = NODE_TYPE_OFFSET + IS_ROOT_OFFSET
	COMMON_NODE_HEADER_SIZE       = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE
	LEAF_NODE_NUM_CELLS_SIZE      = unsafe.Sizeof(uint32(0))
	LEAF_NODE_NUM_CELLS_OFFSET    = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE
	LEAF_NODE_NEXT_LEAF_NODE_SIZE = unsafe.Sizeof(uint32(0))
	LEAF_NODE_NEXT_LEAF_OFFSET    = LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE

	LEAF_NODE_HEADER_SIZE       = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE + LEAF_NODE_NEXT_LEAF_NODE_SIZE
	LEAF_NODE_KEY_SIZE          = unsafe.Sizeof(uint32(0))
	LEAF_NODE_KEY_OFFSET        = 0
	LEAF_NODE_VALUE_SIZE        = ROW_SIZE
	LEAF_NODE_VALUE_OFFSET      = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE
	LEAF_NODE_CELL_SIZE         = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE
	LEAF_NODE_SPACE_FOR_CELLS   = PAGE_SIZE - LEAF_NODE_HEADER_SIZE
	LEAF_NODE_MAX_CELLS         = uint32(LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE)
	LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) / 2
	LEAF_NODE_LEFT_SPLIT_COUNT  = (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT

	INTERNAL_NODE_NUM_KEYS_SIZE      = unsafe.Sizeof(uint32(0))
	INTERNAL_NODE_NUM_KEYS_OFFSET    = COMMON_NODE_HEADER_SIZE
	INTERNAL_NODE_RIGHT_CHILD_SIZE   = unsafe.Sizeof(uint32(0))
	INTERNAL_NODE_RIGHT_CHILD_OFFSET = INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE
	INTERNAL_NODE_HEADER_SIZE        = COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE

	INTERNAL_NODE_KEY_SIZE   = unsafe.Sizeof(uint32(0))
	INTERNAL_NODE_CHILD_SIZE = unsafe.Sizeof(uint32(0))
	INTERNAL_NODE_CELL_SIZE  = INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE

	INTERNAL_NODE_MAX_KEYS = 3
)

const (
	// starting with 1 as 0 will be put as default value
	Select = iota + 1
	Insert
)

const (
	// Node type 1 denotes leaf node 2 denotes internal node
	Leaf = iota + 1
	Internal
)
