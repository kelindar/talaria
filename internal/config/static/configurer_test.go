// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package static

import (
	"testing"

	"github.com/kelindar/talaria/internal/config"
	"github.com/stretchr/testify/assert"
)

func TestConfigure(t *testing.T) {
	c := &config.Config{}
	st := New()
	err := st.Configure(c)
	assert.Nil(t, err)

	assert.Equal(t, c.Readers.Presto.Port, int32(8042))

	assert.Equal(t, c.Writers.GRPC.Port, int32(8080))

	assert.Equal(t, c.Tables.Timeseries.Name, "eventlog")
	assert.Equal(t, c.Tables.Timeseries.TTL, int64(3600))
	assert.Equal(t, c.Tables.Timeseries.SortBy, "tsi")
	assert.Equal(t, c.Tables.Timeseries.HashBy, "event")

	assert.Equal(t, c.Tables.Log.TTL, int64(24*3600))
	assert.Equal(t, c.Tables.Log.Name, "log")

	assert.Equal(t, c.Statsd.Port, int64(8125))
	assert.Equal(t, c.Statsd.Host, "localhost")

	assert.NotNil(t, c.Writers.GRPC)
	assert.NotNil(t, c.Statsd.Port)
	assert.NotNil(t, c.Tables.Timeseries)
	assert.NotNil(t, c.Tables.Log)
}
