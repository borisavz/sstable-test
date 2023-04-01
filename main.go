package main

import (
	"encoding/binary"
	"fmt"
	"github.com/huandu/skiplist"
	"os"
)

type DataEntry struct {
	keySize   uint32
	valueSize uint32
	key       string
	value     []byte
}

func (d DataEntry) String() string {
	return fmt.Sprintf("DataEntry[keySize: %d, valueSize: %d, key: %s, value: ???]", d.keySize, d.valueSize, d.key)
}

type IndexEntry struct {
	keySize    uint32
	key        string
	dataOffset uint32
}

func (i IndexEntry) String() string {
	return fmt.Sprintf("IndexEntry[keySize: %d, key: %s, dataOffset: %d]", i.keySize, i.key, i.dataOffset)
}

func main() {
	Store()
	Load()
}

func Store() {
	list := skiplist.New(skiplist.StringAsc)

	list.Set("a", []byte{1})
	list.Set("c", []byte{1, 2})
	list.Set("bbb/a/b", []byte{1, 2, 3})
	list.Set("bbb/c", []byte{1, 2, 3, 4})
	list.Set("bbb/d/b/e", []byte{1, 2, 3, 4, 5})

	el := list.Front()

	indexFile, err := os.Create("index.bin")
	if err != nil {
		panic(err)
	}

	dataFile, err := os.Create("data.bin")
	if err != nil {
		panic(err)
	}

	dataOffset := 0
	indexOffset := 0

	for el != nil {
		strKey := el.Key().(string)
		binVal := el.Value.([]byte)

		binKey := []byte(strKey)
		binKeySize := binary.Size(binKey)

		data := DataEntry{
			keySize:   uint32(binKeySize),
			valueSize: uint32(binary.Size(binVal)),
			key:       strKey,
			value:     binVal,
		}

		dataSize := binary.Size(data.keySize) + binary.Size(data.valueSize) + int(binKeySize) + int(data.valueSize)

		index := IndexEntry{
			keySize:    uint32(binKeySize),
			key:        strKey,
			dataOffset: uint32(dataOffset),
		}

		indexSize := binary.Size(index.keySize) + int(binKeySize) + binary.Size(index.dataOffset)

		binary.Write(dataFile, binary.BigEndian, data.keySize)
		binary.Write(dataFile, binary.BigEndian, data.valueSize)
		binary.Write(dataFile, binary.BigEndian, binKey)
		binary.Write(dataFile, binary.BigEndian, data.value)

		binary.Write(indexFile, binary.BigEndian, index.keySize)
		binary.Write(indexFile, binary.BigEndian, binKey)
		binary.Write(indexFile, binary.BigEndian, index.dataOffset)

		println(index.String())
		println(data.String())
		println("---")

		dataOffset += dataSize
		indexOffset += indexSize

		el = el.Next()
	}

	indexFile.Close()
	dataFile.Close()
}

func Load() {
	indexFile, err := os.Open("index.bin")
	if err != nil {
		panic(err)
	}

	dataFile, err := os.Open("data.bin")
	if err != nil {
		panic(err)
	}

	for {
		keySizeBin := make([]byte, 4)
		err := binary.Read(indexFile, binary.BigEndian, keySizeBin)
		if err != nil {
			break
		}

		keySize := binary.BigEndian.Uint32(keySizeBin)

		keyBin := make([]byte, keySize)
		binary.Read(indexFile, binary.BigEndian, keyBin)

		key := string(keyBin)

		dataOffsetBin := make([]byte, 4)
		binary.Read(indexFile, binary.BigEndian, dataOffsetBin)

		dataOffset := binary.BigEndian.Uint32(dataOffsetBin)

		index := IndexEntry{
			keySize:    keySize,
			key:        key,
			dataOffset: dataOffset,
		}

		println(index.String())
	}

	indexFile.Close()
	dataFile.Close()
}
