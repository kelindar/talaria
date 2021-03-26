package block

import (
	"fmt"
	"github.com/kelindar/talaria/internal/column"
	"github.com/kelindar/talaria/internal/encoding/parquet"
	"github.com/kelindar/talaria/internal/encoding/typeof"
	"strconv"
	"strings"
)

// FromParquetBy decodes a set of blocks from a Parquet file and repartitions
// it by the specified partition key.
func FromParquetBy(payload []byte, partitionBy string, filter *typeof.Schema, apply applyFunc) ([]Block, error) {
	const max = 10000000 // 10MB

	iter, err := parquet.FromBuffer(payload)
	if err != nil {
		return nil, err
	}

	// Find the partition index
	schema := iter.Schema()
	cols := schema.Columns()
	partitionIdx, ok := findString(cols, partitionBy)
	if !ok {
		return nil, nil // Skip the file if it has no partition column
	}

	// The resulting set of blocks, repartitioned and chunked
	blocks := make([]Block, 0, 128)

	// Create presto columns and iterate
	result, size := make(map[string]column.Columns, 16), 0
	_, _ = iter.Range(func(rowIdx int, r []interface{}) bool {
		if size >= max {
			pending, err := makeBlocks(result)
			if err != nil {
				return true
			}

			size = 0 // Reset the size
			blocks = append(blocks, pending...)
			result = make(map[string]column.Columns, 16)
		}

		// Get the partition value, must be a string
		partition, ok := convertToStringForParquet(r[partitionIdx])
		if !ok {
			return true
		}

		// Skip the record if the partition is actually empty
		if partition == "" {
			return false
		}

		// Get the block for that partition
		columns, exists := result[partition]
		if !exists {
			columns = column.MakeColumns(filter)
			result[partition] = columns
		}

		// Prepare a row for transformation
		row := NewRow(schema, len(r))
		for i, v := range r {
			columnName := cols[i]
			columnType := schema[columnName]

			// Encode to JSON
			if columnType == typeof.JSON {
				if encoded, ok := convertToJSON(v); ok {
					v = encoded
				}
			}

			row.Set(columnName, v)
		}

		// Append computed columns and fill nulls for the row
		out, _ := apply(row)

		size += out.AppendTo(columns)
		size += columns.FillNulls()
		return false
	}, cols...)

	// Write the last chunk
	last, err := makeBlocks(result)
	if err != nil {
		return nil, err
	}

	blocks = append(blocks, last...)
	return blocks, nil
}

// convertToStringForParquet converts value to string because currently all the keys in Badger are stored in the form of string before hashing to the byte array
func convertToStringForParquet(value interface{}) (string, bool) {
	var arr []interface{}

	arr = append(arr, value)
	a := fmt.Sprint(arr)
	b := strings.Replace(a, "[", "", -1)
	v := strings.Replace(b, "]", "", -1)

	if len(v) > 0 {
		return v, true
	}

	v, ok := value.(string)
	if ok {
		return v, true
	}
	valueInt, ok := value.(int64)
	if ok {
		return strconv.FormatInt(valueInt, 10), true
	}
	return "", false
}
