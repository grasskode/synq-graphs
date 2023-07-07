package graph

import (
	"fmt"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

// ParquetRecord holds a single record for graph input.
type ParquetRecord struct {
	source string `parquet:"name=source, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	target string `parquet:"name=target, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

// ReadParquet expects a parquet filename and returns the graph records
// in the file.
func ReadParquet(filename string, skip int, limit int) ([]*ParquetRecord, error) {
	fr, err := local.NewLocalFileReader(filename)
	if err != nil {
		fmt.Println("Error reading file.")
		return nil, err
	}
	pr, err := reader.NewParquetReader(fr, new(ParquetRecord), int64(limit))
	if err != nil {
		fmt.Println("Error creating NewParquetReader.")
		return nil, err
	}
	records := make([]*ParquetRecord, limit)
	if err = pr.Read(&records); err != nil {
		fmt.Println("Error reading records.")
		return nil, err
	}
	pr.ReadStop()
	fr.Close()
	return records, nil
}
