package parquet

import (
	goparquet "github.com/fraugster/parquet-go"
	"os"
	"testing"

	"github.com/fraugster/parquet-go/parquet"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testFile = "../../../test/test2.parquet"
const testFileWithMissingAttributes = "../../../test/testfilewithmissingatts.parquet"

const column = "foo"

func TestReadFile(t *testing.T) {
	testFunc := func(testFileName string) {
		i, err := FromFile(testFileName)
		defer func() { _ = i.Close() }()
		assert.NoError(t, err)

		schema := i.Schema()
		assert.Equal(t, 5, len(schema))

		{
			kind, ok := schema[column]
			assert.True(t, ok)
			assert.Equal(t, "int64", kind.String())
		}

		{
			kind, ok := schema["bar"]
			assert.True(t, ok)
			assert.Equal(t, "int32", kind.String())
		}

		{
			kind, ok := schema["foofoo"]
			assert.True(t, ok)
			assert.Equal(t, "string", kind.String())
		}

		{
			kind, ok := schema["barbar"]
			assert.True(t, ok)
			assert.Equal(t, "float64", kind.String())
		}

		{
			kind, ok := schema["fooBar"]
			assert.True(t, ok)
			assert.Equal(t, "float64", kind.String())
		}

		count := 0
		i.Range(func(int, []interface{}) bool {
			count++
			return false
		}, column)

		assert.Equal(t, 10000, count)
	}

	// Enable when you want to create a Parquet file for the test
	//initFunc(t, goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY), goparquet.WithCreator("talaria-parquet-unittest"))

	// Enable when you want to create a Parquet file with missing attributes for the test
	//initFuncMissingAttributesFile(t, goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY), goparquet.WithCreator("talaria-parquet-unittest"))

	testFunc(testFile)
	testFunc(testFileWithMissingAttributes)
}

// Only use if you wish to generate the Parquet file needed for testing
func initFunc(t *testing.T, opts ...goparquet.FileWriterOption) {
	_ = os.Mkdir("files", 0755)

	wf, err := os.OpenFile(testFile, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	require.NoError(t, err, "creating file failed")

	w := goparquet.NewFileWriter(wf, opts...)

	fooStore, err := goparquet.NewInt64Store(parquet.Encoding_PLAIN, true, &goparquet.ColumnParameters{})
	require.NoError(t, err, "failed to create fooStore")

	barStore, err := goparquet.NewInt32Store(parquet.Encoding_PLAIN, true, &goparquet.ColumnParameters{})
	require.NoError(t, err, "failed to create barStore")

	foofooStore, err := goparquet.NewByteArrayStore(parquet.Encoding_PLAIN, true, &goparquet.ColumnParameters{})
	require.NoError(t, err, "failed to create foofooStore")

	barbarStore, err := goparquet.NewFloatStore(parquet.Encoding_PLAIN, true, &goparquet.ColumnParameters{})
	require.NoError(t, err, "failed to create barbarStore")

	fooBarStore, err := goparquet.NewDoubleStore(parquet.Encoding_PLAIN, true, &goparquet.ColumnParameters{})
	require.NoError(t, err, "failed to create fooBarStore")

	require.NoError(t, w.AddColumn("foo", goparquet.NewDataColumn(fooStore, parquet.FieldRepetitionType_REQUIRED)))
	require.NoError(t, w.AddColumn("bar", goparquet.NewDataColumn(barStore, parquet.FieldRepetitionType_OPTIONAL)))
	require.NoError(t, w.AddColumn("foofoo", goparquet.NewDataColumn(foofooStore, parquet.FieldRepetitionType_OPTIONAL)))
	require.NoError(t, w.AddColumn("barbar", goparquet.NewDataColumn(barbarStore, parquet.FieldRepetitionType_OPTIONAL)))
	require.NoError(t, w.AddColumn("fooBar", goparquet.NewDataColumn(fooBarStore, parquet.FieldRepetitionType_OPTIONAL)))

	const (
		numRecords = 10000
		flushLimit = 1000
	)

	for idx := 0; idx < numRecords; idx++ {
		if idx > 0 && idx%flushLimit == 0 {
			require.NoError(t, w.FlushRowGroup(), "%d. AddData failed", idx)
		}

		require.NoError(t, w.AddData(map[string]interface{}{"foo": int64(idx), "bar": int32(idx), "foofoo":[]byte("foo"), "barbar":float32(idx),
			"fooBar":float64(idx)}), "%d. AddData failed", idx)
	}

	assert.NoError(t, w.Close(), "Close failed")

	require.NoError(t, wf.Close())
}

// Only use if you wish to generate the Parquet file with missing attributes needed for testing
func initFuncMissingAttributesFile(t *testing.T, opts ...goparquet.FileWriterOption) {
	_ = os.Mkdir("files", 0755)

	wf, err := os.OpenFile(testFileWithMissingAttributes, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	require.NoError(t, err, "creating file failed")

	w := goparquet.NewFileWriter(wf, opts...)

	fooStore, err := goparquet.NewInt64Store(parquet.Encoding_PLAIN, true, &goparquet.ColumnParameters{})
	require.NoError(t, err, "failed to create fooStore")

	barStore, err := goparquet.NewInt32Store(parquet.Encoding_PLAIN, true, &goparquet.ColumnParameters{})
	require.NoError(t, err, "failed to create barStore")

	foofooStore, err := goparquet.NewByteArrayStore(parquet.Encoding_PLAIN, true, &goparquet.ColumnParameters{})
	require.NoError(t, err, "failed to create foofooStore")

	barbarStore, err := goparquet.NewFloatStore(parquet.Encoding_PLAIN, true, &goparquet.ColumnParameters{})
	require.NoError(t, err, "failed to create barbarStore")

	fooBarStore, err := goparquet.NewDoubleStore(parquet.Encoding_PLAIN, true, &goparquet.ColumnParameters{})
	require.NoError(t, err, "failed to create fooBarStore")

	require.NoError(t, w.AddColumn("foo", goparquet.NewDataColumn(fooStore, parquet.FieldRepetitionType_REQUIRED)))
	require.NoError(t, w.AddColumn("bar", goparquet.NewDataColumn(barStore, parquet.FieldRepetitionType_OPTIONAL)))
	require.NoError(t, w.AddColumn("foofoo", goparquet.NewDataColumn(foofooStore, parquet.FieldRepetitionType_OPTIONAL)))
	require.NoError(t, w.AddColumn("barbar", goparquet.NewDataColumn(barbarStore, parquet.FieldRepetitionType_OPTIONAL)))
	require.NoError(t, w.AddColumn("fooBar", goparquet.NewDataColumn(fooBarStore, parquet.FieldRepetitionType_OPTIONAL)))

	const (
		numRecords = 10000
		flushLimit = 1000
	)

	for idx := 0; idx < numRecords; idx++ {
		if idx > 0 && idx%flushLimit == 0 {
			require.NoError(t, w.FlushRowGroup(), "%d. AddData failed", idx)
		}

		require.NoError(t, w.AddData(map[string]interface{}{"foo": int64(idx), "foofoo":[]byte("foo"),
			"fooBar":float64(idx)}), "%d. AddData failed", idx)
	}

	assert.NoError(t, w.Close(), "Close failed")

	require.NoError(t, wf.Close())
}
