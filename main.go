package main

import (
	"encoding/binary"
	"github.com/huandu/skiplist"
)

type DataEntry struct {
	keySize   int32
	valueSize int32
	key       string
	value     []byte
}

type IndexEntry struct {
	keySize    int32
	key        string
	dataOffset int32
}

func main() {
	list := skiplist.New(skiplist.StringAsc)

	list.Set("a", []byte{1})
	list.Set("c", []byte{1, 2})
	list.Set("bbb/a/b", []byte{1, 2, 3})
	list.Set("bbb/c", []byte{1, 2, 3, 4})
	list.Set("bbb/d/b/e", []byte{1, 2, 3, 4, 5})

	el := list.Front()

	//indexFile, err := os.Open("index.bin")
	//if err != nil {
	//	panic(err)
	//}
	//
	//dataFile, err := os.Open("data.bin")
	//if err != nil {
	//	panic(err)
	//}

	dataOffset := 0
	indexOffset := 0

	for el != nil {
		strKey := el.Key().(string)
		binVal := el.Value.([]byte)

		data := DataEntry{
			keySize:   int32(binary.Size([]byte(strKey))),
			valueSize: int32(binary.Size(binVal)),
			key:       strKey,
			value:     binVal,
		}

		dataSize := binary.Size(data.keySize) + binary.Size(data.valueSize) + binary.Size(data.key) + binary.Size(data.value)

		index := IndexEntry{
			keySize:    int32(binary.Size([]byte(strKey))),
			key:        strKey,
			dataOffset: int32(dataOffset),
		}

		indexSize := binary.Size(index.keySize) + binary.Size(index.key) + binary.Size(index.dataOffset)

		println(indexOffset, dataOffset)

		dataOffset += dataSize
		indexOffset += indexSize

		el = el.Next()
	}

	//indexFile.Close()
	//dataFile.Close()
}
