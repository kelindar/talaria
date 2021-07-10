// Copyright 2019-2020 Grabtaxi Holdings PTE LTE (GRAB), All rights reserved.
// Use of this source code is governed by an MIT-style license that can be found in the LICENSE file

package main

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	eorc "github.com/crphang/orc"
	"github.com/gorilla/mux"
	"github.com/kelindar/lua"
	"github.com/kelindar/talaria/internal/config"
	"github.com/kelindar/talaria/internal/config/env"
	"github.com/kelindar/talaria/internal/config/s3"
	"github.com/kelindar/talaria/internal/config/static"
	"github.com/kelindar/talaria/internal/monitor"
	"github.com/kelindar/talaria/internal/monitor/logging"
	"github.com/kelindar/talaria/internal/monitor/statsd"
	script "github.com/kelindar/talaria/internal/scripting"
	mlog "github.com/kelindar/talaria/internal/scripting/log"
	mnet "github.com/kelindar/talaria/internal/scripting/net"
	mstats "github.com/kelindar/talaria/internal/scripting/stats"
	"github.com/kelindar/talaria/internal/server"
	"github.com/kelindar/talaria/internal/server/cluster"
	"github.com/kelindar/talaria/internal/storage"
	"github.com/kelindar/talaria/internal/storage/disk"
	"github.com/kelindar/talaria/internal/storage/writer"
	"github.com/kelindar/talaria/internal/table"
	"github.com/kelindar/talaria/internal/table/log"
	"github.com/kelindar/talaria/internal/table/nodes"
	"github.com/kelindar/talaria/internal/table/timeseries"
)

const (
	logTag = "main"
)

func main() {

	// test
	eorc.DefaultCompressionChunkSize = 16 * eorc.DefaultCompressionChunkSize
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s3Configurer := s3.New(logging.NewStandard())
	configure := config.Load(ctx, 60*time.Second, static.New(), env.New("TALARIA"), s3Configurer)
	conf := configure()

	// Setup gossip
	gossip := cluster.New(7946)

	// Create a log table and a simple stdout monitor
	stats := statsd.New(conf.Statsd.Host, int(conf.Statsd.Port))
	logTable := log.New(configure, gossip, monitor.New(
		logging.NewStandard(), stats, conf.AppName, conf.Env), // Use stdout monitor
	)

	// Setup the final logger and a monitor
	logger := logging.NewComposite(logTable, logging.NewStandard())
	monitor := monitor.New(logger, stats, conf.AppName, conf.Env)

	// Updating the logger to use the composite logger. This is to make sure the logs from the config is sent to log table as well as stdout
	s3Configurer.SetLogger(logger)

	// Create a script loader
	loader := script.NewLoader([]lua.Module{
		mlog.New(monitor),
		mstats.New(monitor),
		mnet.New(monitor),
	})

	// Open every table configured
	tables := []table.Table{nodes.New(gossip), logTable}
	for name, tableConf := range conf.Tables {
		tables = append(tables, openTable(name, conf.Storage, tableConf, gossip, monitor, loader))
	}

	// Start the new server
	server := server.New(configure, monitor, loader, tables...)

	// onSignal will be called when a OS-level signal is received.
	onSignal(func(_ os.Signal) {
		cancel()       // Cancel the context
		gossip.Close() // Close the gossip layer
		server.Close() // Close the server and database
	})

	// Join the cluster
	monitor.Info("server: joining cluster on %s...", conf.Domain)
	gossip.JoinHostname(conf.Domain)

	// run HTTP server for readiness and liveness probes if k8s config is set
	if conf.K8s != nil {
		startHTTPServerAsync(conf.K8s.ProbePort)
	}

	// Start listenHandler
	monitor.Info("server: starting...")
	monitor.Count1(logTag, "start")
	if err := server.Listen(ctx, conf.Readers.Presto.Port, conf.Writers.GRPC.Port); err != nil {
		panic(err)
	}
}

// openTable creates a new table with storage & optional compaction fully configured
func openTable(name string, storageConf config.Storage, tableConf config.Table, cluster cluster.Membership, monitor monitor.Monitor, loader *script.Loader) table.Table {
	monitor.Info("server: opening table %s...", name)

	// Create a new storage layer and optional compaction
	store := storage.Storage(disk.Open(storageConf.Directory, name, monitor, storageConf.Badger))
	if tableConf.Compact != nil {
		var err error
		store, err = writer.ForCompaction(tableConf.Compact, monitor, store, loader)
		if err != nil {
			panic(err)
		}
	}

	// Returns noop streamer if array is empty
	streams, err := writer.ForStreaming(tableConf.Streams, monitor, loader)
	if err != nil {
		panic(err)
	}

	return timeseries.New(name, cluster, monitor, store, &tableConf, streams)
}

// onSignal hooks a callback for a signal.
func onSignal(callback func(sig os.Signal)) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		for sig := range c {
			callback(sig)
		}
	}()
}

func startHTTPServerAsync(portNum int32) {
	go func() {
		handler := mux.NewRouter()
		handler.HandleFunc("/healthz", func(resp http.ResponseWriter, req *http.Request) {
			_, _ = resp.Write([]byte(`talaria-health-check`))
		}).Methods(http.MethodGet, http.MethodHead)
		handler.PathPrefix("/debug/pprof/").Handler(http.DefaultServeMux)

		server := &http.Server{
			Addr:    fmt.Sprintf(":%d", portNum),
			Handler: handler,
		}
		if err := server.ListenAndServe(); err != nil {
			panic(err)
		}
	}()
}
