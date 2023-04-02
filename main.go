package main

import (
	"encoding/binary"
	"fmt"
	"github.com/huandu/skiplist"
	"os"
	"time"
)

type MemtableEntry struct {
	timestamp uint64
	tombstone bool
	value     []byte
}

func NewItem(value []byte) MemtableEntry {
	return MemtableEntry{
		timestamp: uint64(time.Now().UnixNano()),
		tombstone: false,
		value:     value,
	}
}

func NewDeletion() MemtableEntry {
	return MemtableEntry{
		timestamp: uint64(time.Now().UnixNano()),
		tombstone: true,
		value:     nil,
	}
}

type DataEntry struct {
	keySize   uint32
	valueSize uint32
	timestamp uint64
	tombstone bool
	key       string
	value     []byte
}

func (d *DataEntry) BinarySize() int {
	binKey := []byte(d.key)
	binKeySize := binary.Size(binKey)

	return binary.Size(d.keySize) + binary.Size(d.valueSize) + binary.Size(d.timestamp) + binary.Size(d.tombstone) + int(binKeySize) + int(d.valueSize)
}

func (d *DataEntry) String() string {
	return fmt.Sprintf("DataEntry[keySize: %d, valueSize: %d, tombstone: %t, key: %s, value: ???]", d.keySize, d.valueSize, d.tombstone, d.key)
}

type IndexEntry struct {
	keySize    uint32
	key        string
	dataOffset uint32
}

func (i *IndexEntry) BinarySize() int {
	binKey := []byte(i.key)
	binKeySize := binary.Size(binKey)

	return binary.Size(i.keySize) + binKeySize + binary.Size(i.dataOffset)
}

func (i *IndexEntry) String() string {
	return fmt.Sprintf("IndexEntry[keySize: %d, key: %s, dataOffset: %d]", i.keySize, i.key, i.dataOffset)
}

func main() {
	list1 := skiplist.New(skiplist.StringAsc)

	list1.Set("a", NewItem([]byte{1}))
	list1.Set("c", NewItem([]byte{1, 2}))
	list1.Set("bbb/a/b", NewItem([]byte{1, 2, 3}))
	list1.Set("bbb/c", NewItem([]byte{1, 2, 3, 4}))
	list1.Set("bbb/d/b/e", NewItem([]byte{1, 2, 3, 4, 5}))

	list2 := skiplist.New(skiplist.StringAsc)

	list2.Set("bbb/a/b", NewDeletion())
	list2.Set("bbb/c", NewItem([]byte{1, 2, 3, 4, 4, 4, 4}))

	Store(list1, "index1.bin", "data1.bin")
	Store(list2, "index2.bin", "data2.bin")

	Load("index1.bin", "data1.bin")
	Find("bbb/a/b", "index2.bin", "data2.bin")

	Compact()
}

func Store(list *skiplist.SkipList, indexFilePath string, dataFilePath string) {
	el := list.Front()

	indexFile, err := os.Create(indexFilePath)
	if err != nil {
		panic(err)
	}

	dataFile, err := os.Create(dataFilePath)
	if err != nil {
		panic(err)
	}

	dataOffset := 0
	indexOffset := 0

	for el != nil {
		strKey := el.Key().(string)
		val := el.Value.(MemtableEntry)

		binKey := []byte(strKey)
		binKeySize := binary.Size(binKey)

		data := DataEntry{
			keySize:   uint32(binKeySize),
			valueSize: uint32(binary.Size(val.value)),
			timestamp: val.timestamp,
			tombstone: val.tombstone,
			key:       strKey,
			value:     val.value,
		}

		index := IndexEntry{
			keySize:    uint32(binKeySize),
			key:        strKey,
			dataOffset: uint32(dataOffset),
		}

		WriteDataRow(dataFile, data)
		WriteIndexRow(indexFile, index)

		dataOffset += data.BinarySize()
		indexOffset += index.BinarySize()

		el = el.Next()
	}

	indexFile.Close()
	dataFile.Close()
}

func Load(indexFilePath string, dataFilePath string) {
	indexFile, err := os.Open(indexFilePath)
	if err != nil {
		panic(err)
	}

	dataFile, err := os.Open(dataFilePath)
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

func ReadDataRow(dataFile *os.File, dataOffset int64) *DataEntry {
	dataFile.Seek(dataOffset, 0)

	dataKeySizeBin := make([]byte, 4)
	binary.Read(dataFile, binary.BigEndian, dataKeySizeBin)
	dataKeySize := binary.BigEndian.Uint32(dataKeySizeBin)

	dataValueSizeBin := make([]byte, 4)
	binary.Read(dataFile, binary.BigEndian, dataValueSizeBin)
	dataValueSize := binary.BigEndian.Uint32(dataValueSizeBin)

	dataTimestampBin := make([]byte, 8)
	binary.Read(dataFile, binary.BigEndian, dataTimestampBin)
	dataTimestamp := binary.BigEndian.Uint64(dataTimestampBin)

	dataTombstoneBin := make([]byte, 1)
	binary.Read(dataFile, binary.BigEndian, dataTombstoneBin)
	dataTombstone := false
	if dataTombstoneBin[0] == 1 {
		dataTombstone = true
	}

	dataKeyBin := make([]byte, dataKeySize)
	binary.Read(dataFile, binary.BigEndian, dataKeyBin)
	dataKey := string(dataKeyBin)

	dataValueBin := make([]byte, dataValueSize)
	if dataValueSize != 0 {
		binary.Read(dataFile, binary.BigEndian, dataValueBin)
	}

	return &DataEntry{
		keySize:   dataKeySize,
		valueSize: dataValueSize,
		timestamp: dataTimestamp,
		tombstone: dataTombstone,
		key:       dataKey,
		value:     dataValueBin,
	}
}

func WriteDataRow(dataFile *os.File, dataEntry DataEntry) {
	binKey := []byte(dataEntry.key)

	binary.Write(dataFile, binary.BigEndian, dataEntry.keySize)
	binary.Write(dataFile, binary.BigEndian, dataEntry.valueSize)
	binary.Write(dataFile, binary.BigEndian, dataEntry.timestamp)
	binary.Write(dataFile, binary.BigEndian, dataEntry.tombstone)
	binary.Write(dataFile, binary.BigEndian, binKey)
	binary.Write(dataFile, binary.BigEndian, dataEntry.value)
}

func ReadIndexRow(indexFile *os.File) *IndexEntry {
	keySizeBin := make([]byte, 4)
	err := binary.Read(indexFile, binary.BigEndian, keySizeBin)
	if err != nil {
		return nil
	}

	keySize := binary.BigEndian.Uint32(keySizeBin)

	keyBin := make([]byte, keySize)
	binary.Read(indexFile, binary.BigEndian, keyBin)

	key := string(keyBin)

	dataOffsetBin := make([]byte, 4)
	binary.Read(indexFile, binary.BigEndian, dataOffsetBin)

	dataOffset := binary.BigEndian.Uint32(dataOffsetBin)

	return &IndexEntry{
		keySize:    keySize,
		key:        key,
		dataOffset: dataOffset,
	}
}

func WriteIndexRow(indexFile *os.File, indexEntry IndexEntry) {
	binKey := []byte(indexEntry.key)

	binary.Write(indexFile, binary.BigEndian, indexEntry.keySize)
	binary.Write(indexFile, binary.BigEndian, binKey)
	binary.Write(indexFile, binary.BigEndian, indexEntry.dataOffset)
}

func Find(searchKey string, indexFilePath string, dataFilePath string) {
	indexFile, err := os.Open(indexFilePath)
	if err != nil {
		panic(err)
	}

	dataFile, err := os.Open(dataFilePath)
	if err != nil {
		panic(err)
	}

	for {
		index := ReadIndexRow(indexFile)

		if index == nil {
			break
		}

		if index.key == searchKey {
			data := ReadDataRow(dataFile, int64(index.dataOffset))

			println("---")
			println(data.String())

			break
		}
	}

	indexFile.Close()
	dataFile.Close()
}

func Compact() {

}
