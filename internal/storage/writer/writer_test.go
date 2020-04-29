package writer

import (
	"testing"

	"github.com/kelindar/talaria/internal/config"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/kelindar/talaria/internal/monitor/logging"
	"github.com/kelindar/talaria/internal/monitor/statsd"
	"github.com/kelindar/talaria/internal/scripting"
	"github.com/kelindar/talaria/internal/storage/disk"
	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	cfg := &config.Compaction{}
	compact := New(cfg,
		monitor.New(logging.NewStandard(), statsd.NewNoop(), "x", "x"),
		disk.New(monitor.NewNoop()),
		script.NewLoader(nil),
	)

	assert.NotNil(t, compact)
}

func TestHash(t *testing.T) {

	row := map[string]interface{}{
		"a": 1654,
		"b": "hello world",
		"c": true,
	}

	h1 := hashOfRow(row)
	h2 := hashOfRow(row)
	assert.Equal(t, h1, h2)
}
