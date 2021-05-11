package merge

import (
	"bytes"
	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/kelindar/talaria/internal/encoding/block"
	"github.com/kelindar/talaria/internal/encoding/typeof"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestToParquet(t *testing.T) {

	schema := typeof.Schema{
		"col0": typeof.String,
		"col1": typeof.Int64,
		"col2": typeof.Float64,
		"col3": typeof.JSON,
	}
	parquetSchema, fieldHandlers, err := deriveSchema(schema)

	if err != nil {
		t.Fatal(err)
	}

	parquetBuffer1 := &bytes.Buffer{}

	writer := goparquet.NewFileWriter(parquetBuffer1,
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
		goparquet.WithSchemaDefinition(parquetSchema),
		goparquet.WithCreator("write-lowlevel"),
	)

	data := make(map[string]interface{})

	data["col0"], _ = fieldHandlers[0]("foo")
	data["col1"], _ = fieldHandlers[1](5)
	data["col2"], _ = fieldHandlers[2](14.6)
	data["col3"], _ = fieldHandlers[3]("[{\"column\":\"a\",\"type\":\"VARCHAR\"}")

	err = writer.AddData(data)
	assert.NoError(t, err)

	err = writer.Close()
	assert.NoError(t, err)

	parquetBuffer2 := &bytes.Buffer{}
	writer = goparquet.NewFileWriter(parquetBuffer2,
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
		goparquet.WithSchemaDefinition(parquetSchema),
		goparquet.WithCreator("write-lowlevel"),
	)

	data2 := make(map[string]interface{})

	data2["col0"], _ = fieldHandlers[0]("foofoo")
	data2["col1"], _ = fieldHandlers[1](10)
	data2["col2"], _ = fieldHandlers[2](17)
	data2["col3"], _ = fieldHandlers[3]("[{\"column\":\"a\",\"type\":\"VARCHAR\"}")

	err = writer.AddData(data2)
	assert.NoError(t, err)

	err = writer.FlushRowGroup()
	assert.NoError(t, err)

	err = writer.Close()
	assert.NoError(t, err)

	apply := block.Transform(nil)

	block1, err := block.FromParquetBy(parquetBuffer1.Bytes(), "col1", nil, apply)
	block2, err := block.FromParquetBy(parquetBuffer2.Bytes(), "col1", nil, apply)

	mergedBlocks := []block.Block{}
	for _, blk := range block1 {
		mergedBlocks = append(mergedBlocks, blk)
	}
	for _, blk := range block2 {
		mergedBlocks = append(mergedBlocks, blk)
	}
	mergedValue, err := ToParquet(mergedBlocks, schema)
	assert.NoError(t, err)

	parquetBuffer := &bytes.Buffer{}

	writer = goparquet.NewFileWriter(parquetBuffer,
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
		goparquet.WithSchemaDefinition(parquetSchema),
		goparquet.WithCreator("write-lowlevel"),
	)

	_ = writer.AddData(data)
	_ = writer.AddData(data2)
	_ = writer.Close()

	if !bytes.Equal(parquetBuffer.Bytes(), mergedValue) {
		t.Fatal("Merged parquet value differ")
	}
}

func TestMergeParquet_DifferentSchema(t *testing.T) {

	schema := typeof.Schema{
		"col0": typeof.String,
		"col1": typeof.Int64,
		"col2": typeof.Float64,
	}
	parquetSchema, fieldHandlers, err := deriveSchema(schema)

	if err != nil {
		t.Fatal(err)
	}

	schema2 := typeof.Schema{
		"col0": typeof.String,
		"col1": typeof.Int64,
		"col2": typeof.Float64,
		"col3": typeof.Float64,
	}
	parquetSchema2, fieldHandlers, err := deriveSchema(schema2)

	if err != nil {
		t.Fatal(err)
	}

	parquetBuffer1 := &bytes.Buffer{}

	writer := goparquet.NewFileWriter(parquetBuffer1,
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
		goparquet.WithSchemaDefinition(parquetSchema),
		goparquet.WithCreator("write-lowlevel"),
	)

	data := make(map[string]interface{})

	data["col0"], _ = fieldHandlers[0]("foo")
	data["col1"], _ = fieldHandlers[1](5)
	data["col2"], _ = fieldHandlers[2](14.6)

	_ = writer.AddData(data)
	_ = writer.Close()

	parquetBuffer2 := &bytes.Buffer{}
	writer = goparquet.NewFileWriter(parquetBuffer2,
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
		goparquet.WithSchemaDefinition(parquetSchema2),
		goparquet.WithCreator("write-lowlevel"),
	)

	data2 := make(map[string]interface{})

	data2["col0"], _ = fieldHandlers[0]("foofoo")
	data2["col1"], _ = fieldHandlers[1](10)
	data2["col2"], _ = fieldHandlers[2](17)
	data2["col3"], _ = fieldHandlers[2](19)

	_ = writer.AddData(data2)
	_ = writer.FlushRowGroup()

	_ = writer.Close()

	apply := block.Transform(nil)

	block1, err := block.FromParquetBy(parquetBuffer1.Bytes(), "col1", nil, apply)
	block2, err := block.FromParquetBy(parquetBuffer2.Bytes(), "col1", nil, apply)

	mergedBlocks := []block.Block{}
	for _, blk := range block1 {
		mergedBlocks = append(mergedBlocks, blk)
	}
	for _, blk := range block2 {
		mergedBlocks = append(mergedBlocks, blk)
	}
	mergedValue, err := ToParquet(mergedBlocks, schema2)
	assert.NoError(t, err)

	parquetBuffer := &bytes.Buffer{}

	writer = goparquet.NewFileWriter(parquetBuffer,
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
		goparquet.WithSchemaDefinition(parquetSchema2),
		goparquet.WithCreator("write-lowlevel"),
	)

	data3 := make(map[string]interface{})

	data3["col0"], _ = fieldHandlers[0]("foo")
	data3["col1"], _ = fieldHandlers[1](5)
	data3["col2"], _ = fieldHandlers[2](14.6)
	data3["col3"] = nil

	_ = writer.AddData(data3)
	_ = writer.AddData(data2)
	_ = writer.Close()

	if !bytes.Equal(parquetBuffer.Bytes(), mergedValue) {
		t.Fatal("Merged parquet value differ")
	}
}

