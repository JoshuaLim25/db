package storage

import (
	"fmt"
	"math/rand"
	"os"
)

// SaveData atomically saves data to a file using temporary file and rename
func SaveData(pathName string, data []byte) error {
	// NOTE: we can't just make a `pathName` file and save it with `fysnc`:
	// the temporary file req for atomicity and durability. If you don't,
	// you can have an empty or half-written file. Need to **rename** it.
	tempFile := fmt.Sprintf("%s.tmp.%d", pathName, rand.Int())
	fp, err := os.OpenFile(tempFile, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0664)
	defer func() {
		fp.Close()
		if err != nil {
			os.Remove(tempFile)
		}
	}()

	if err != nil {
		return err
	}

	if _, err := fp.Write(data); err != nil {
		return err
	}
	if err := fp.Sync(); err != nil {
		return err
	}
	err = os.Rename(tempFile, pathName)
	return err
}