// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package column

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/kelindar/talaria/internal/encoding/typeof"
	"github.com/kelindar/talaria/internal/presto"
	"github.com/stretchr/testify/assert"
)

func TestColumns(t *testing.T) {
	nc := make(Columns, 2)
	assert.Nil(t, nc.Any())

	// Fill level 1
	assert.NotZero(t, nc.Append("a", int32(1), typeof.Int32))
	assert.NotZero(t, nc.Append("b", int32(2), typeof.Int32))
	assert.Zero(t, nc.Append("123", int32(2), typeof.Int32)) // Invalid
	assert.Zero(t, nc.Append("x", complex128(1), typeof.Unsupported))
	assert.Equal(t, 1, nc.Max())
	assert.Equal(t, 2, len(nc.LastRow()))
	nc.FillNulls()
	assert.NotNil(t, nc.Any())

	// Fill level 2
	assert.NotZero(t, nc.Append("a", int32(1), typeof.Int32))
	assert.NotZero(t, nc.Append("c", "hi", typeof.String))
	assert.Equal(t, 2, nc.Max())
	nc.FillNulls()

	// Fill level 3
	assert.NotZero(t, nc.Append("b", int32(1), typeof.Int32))
	assert.NotZero(t, nc.Append("c", "hi", typeof.String))
	assert.NotZero(t, nc.Append("d", float64(1.5), typeof.Float64))
	assert.Equal(t, 3, nc.Max())
	nc.FillNulls()

	// Must have 3 levels with nulls in the middle
	assert.Equal(t, []int32{1, 1, 0}, nc["a"].AsThrift().IntegerData.Ints)
	assert.Equal(t, []bool{false, false, true}, nc["a"].AsThrift().IntegerData.Nulls)
	assert.Equal(t, []int32{2, 0, 1}, nc["b"].AsThrift().IntegerData.Ints)
	assert.Equal(t, []bool{false, true, false}, nc["b"].AsThrift().IntegerData.Nulls)
	assert.Equal(t, []byte{0x68, 0x69, 0x68, 0x69}, nc["c"].AsThrift().VarcharData.Bytes)
	assert.Equal(t, []int32{0, 2, 2}, nc["c"].AsThrift().VarcharData.Sizes)
	assert.Equal(t, []bool{true, false, false}, nc["c"].AsThrift().VarcharData.Nulls)
	assert.Equal(t, []float64{0, 0, 1.5}, nc["d"].AsThrift().DoubleData.Doubles)
	assert.Equal(t, []bool{true, true, false}, nc["d"].AsThrift().DoubleData.Nulls)
	assert.Equal(t, 4, len(nc.LastRow()))

}

func TestMakeColumns(t *testing.T) {
	tests := []struct {
		input  *typeof.Schema
		output Columns
	}{
		{
			input:  &typeof.Schema{
				"a":    typeof.Int64,
				"b":    typeof.Timestamp,
			},
			output: Columns {
				"a": NewColumn(typeof.Int64),
				"b": NewColumn(typeof.Timestamp),
			},
		},
		{
			input:  nil,
			output: make(Columns, 16),
		},
	}
	for _, tc := range tests {
		c := MakeColumns(tc.input)
		assert.Equal(t, tc.output, c)
	}
}

func TestNewColumn(t *testing.T) {
	tests := []struct {
		input  interface{}
		output interface{}
	}{
		{
			input:  "hi",
			output: new(presto.PrestoThriftVarchar),
		},
		{
			input:  int64(1),
			output: new(presto.PrestoThriftBigint),
		},
		{
			input:  float64(1),
			output: new(presto.PrestoThriftDouble),
		},
		{
			input:  true,
			output: new(presto.PrestoThriftBoolean),
		},
		{
			input:  time.Unix(1, 0),
			output: new(presto.PrestoThriftTimestamp),
		},
		{
			input:  json.RawMessage(nil),
			output: new(presto.PrestoThriftJson),
		},
	}

	for _, tc := range tests {
		rt, ok := typeof.FromType(reflect.TypeOf(tc.input))
		assert.True(t, ok)

		c := NewColumn(rt)
		assert.Equal(t, tc.output, c)

		if c != nil {
			assert.Equal(t, 0, c.AsThrift().Size())
			assert.Equal(t, 0, c.Size())
		}
	}
}

func TestIsValidName(t *testing.T) {
	tests := []struct {
		input  string
		output bool
	}{
		{input: "hi", output: true},
		{input: "/api/v1/eta/nearby/", output: false},
		{input: "15ffe3ca0ba2bef00000010955e2d54c", output: false},
		{input: "b3802fb30f58430ca7fa8c6e04cb8c76", output: true},
		{input: "server", output: true},
	}

	for _, tc := range tests {
		assert.Equal(t, tc.output, IsValidName(tc.input))
	}
}
