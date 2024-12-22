package storage

import (
	"fmt"
	"os"
	"unsafe"

	"github.com/unamdev0/ZiDB/pkg/constants"
)

type MemoryBlock struct {
	Data [constants.PAGE_SIZE]byte
}

type Pager struct {
	File     *os.File
	FileSize uint32
	NumPages uint32
	Pages    [constants.MAX_PAGE_NUM]*MemoryBlock
}

func OpenOrCreateFile(filename string) (*Pager, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open or create file: %w", err)
	}

	fi, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file stats: %w", err)
	}

	fmt.Printf("The file is %d bytes long\n", fi.Size())

	pager := &Pager{
		File:     file,
		FileSize: uint32(fi.Size()),
		NumPages: uint32(fi.Size() / constants.PAGE_SIZE),
	}
	return pager, nil
}

func GetPage(pager *Pager, pageNum uint32) (uintptr, error) {
	if pageNum > constants.MAX_PAGE_NUM {
		return 0, constants.ErrOutOfBoundPageNum
	}

	if pager.Pages[pageNum] == nil {
		var page MemoryBlock
		var numPages uint32
		page = MemoryBlock{}
		numPages = pager.FileSize / constants.PAGE_SIZE

		if pager.FileSize%uint32(constants.PAGE_SIZE) != 0 {
			numPages += 1
		}

		if pageNum <= numPages {
			// Calculate the offset and seek to the desired position
			offset := int64(pageNum * constants.PAGE_SIZE)
			_, err := pager.File.Seek(offset, 0) // 0 is equivalent to SEEK_SET
			if err != nil {
				fmt.Println("Error seeking file:", err)
				return 0, constants.ErrReadingFile
			}
			len, err := pager.File.Read(page.Data[:])
			if len == -1 {
				fmt.Println("Error reading file:", err)
				return 0, constants.ErrReadingFile
			}

		}
		pager.Pages[pageNum] = &page

		if pageNum >= pager.NumPages {
			pager.NumPages = pageNum + 1
		}
	}
	return uintptr(unsafe.Pointer(&pager.Pages[pageNum].Data[0])), nil
}

func WriteToFile(pager *Pager, pageNum uint32) {
	if pager.Pages[pageNum] == nil {
		fmt.Println(constants.ErrNoContentFound)
		os.Exit(-1)
	}

	offset := int64(pageNum * constants.PAGE_SIZE)
	_, err := pager.File.WriteAt(pager.Pages[pageNum].Data[:], offset)
	if err != nil {
		fmt.Println(constants.ErrDataNotSaved)
		os.Exit(-1)
	}
}

func FetchUnusedPageNum(pager *Pager) uint32 {
	return pager.NumPages
}
