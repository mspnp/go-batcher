package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	gobatcher "github.com/plasne/go-batcher"
	goconfig "github.com/plasne/go-config"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var flagPort, flagCapacity int

func init() {

	// startup config
	err := goconfig.Startup()
	if err != nil {
		panic(err)
	}

	// start config block
	fmt.Println("CONFIGURATION:")

	// configure logging
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
	logLevels := map[string]int{
		"trace":    int(zerolog.TraceLevel),
		"debug":    int(zerolog.DebugLevel),
		"info":     int(zerolog.InfoLevel),
		"warn":     int(zerolog.WarnLevel),
		"error":    int(zerolog.ErrorLevel),
		"fatal":    int(zerolog.FatalLevel),
		"panic":    int(zerolog.PanicLevel),
		"nolevel":  int(zerolog.NoLevel),
		"disabled": int(zerolog.Disabled),
	}
	logLevel := goconfig.AsInt().TrySetByEnv("LOG_LEVEL").Lookup(logLevels).Clamp(-1, 7).DefaultTo(int(zerolog.InfoLevel)).PrintLookup(logLevels).Value()
	zerolog.SetGlobalLevel(zerolog.Level(logLevel))

	// allow for flags to override env
	flag.IntVar(&flagPort, "port", 0, "Determines the port to listen for HTTP requests. This overrides PORT.")
	flag.IntVar(&flagCapacity, "capacity", 0, "Determines the capacity to shared. This overrides CAPACITY.")

	// seed the random number generator
	rand.Seed(time.Now().UnixNano())

}

func main() {
	ctx := context.Background()

	// complete configuration
	flag.Parse()
	PORT := goconfig.AsInt().TrySetValue(flagPort).TrySetByEnv("PORT").DefaultTo(8080).Print().Value()
	CAPACITY := goconfig.AsInt().TrySetValue(flagCapacity).TrySetByEnv("CAPACITY").DefaultTo(10000).Print().Value()
	AZBLOB_ACCOUNT := goconfig.AsString().TrySetByEnv("AZBLOB_ACCOUNT").Print().Require().Value()
	AZBLOB_KEY := goconfig.AsString().TrySetByEnv("AZBLOB_KEY").PrintMasked().Require().Value()
	AZBLOB_CONTAINER := goconfig.AsString().TrySetByEnv("AZBLOB_CONTAINER").Print().Require().Value()

	// start getting shared resource capacity
	azresource := gobatcher.NewAzureSharedResource(AZBLOB_ACCOUNT, AZBLOB_CONTAINER, uint32(CAPACITY)).
		WithMasterKey(AZBLOB_KEY).
		WithFactor(1000)
	resourceListener := azresource.AddListener(func(event string, val int, msg *string) {
		switch event {
		case "shutdown":
			log.Debug().Msgf("AzureSharedResource shutdown.")
		case "capacity":
			log.Trace().Msgf("AzureSharedResource has procured %v capacity.", val)
		case "failed":
			log.Trace().Msgf("AzureSharedResource failed to take control of partition %v.", val)
		case "released":
			log.Trace().Msgf("AzureSharedResource lost control of partition %v.", val)
		case "allocated":
			log.Trace().Msgf("AzureSharedResource gained control of partition %v.", val)
		case "error":
			log.Err(fmt.Errorf(*msg)).Msgf("AzureSharedResource raised the following error...")
		case "created-container":
			log.Trace().Msgf("AzureSharedResource created container %v.", *msg)
		case "verified-container":
			log.Trace().Msgf("AzureSharedResource verified that container %v already exists.", *msg)
		case "created-blob":
			log.Trace().Msgf("AzureSharedResource created blob %v.", val)
		case "verified-blob":
			log.Trace().Msgf("AzureSharedResource verified that blob %v already exists.", val)
		}
	})
	defer azresource.RemoveListener(resourceListener)
	if err := azresource.Provision(ctx); err != nil {
		panic(err)
	}
	if err := azresource.Start(ctx); err != nil {
		panic(err)
	}

	// configure the batcher
	batcher := gobatcher.NewBatcher().
		WithSharedResource(azresource)
	batcherListener := batcher.AddListener(func(event string, val int, msg *string) {
		switch event {
		case "shutdown":
			log.Debug().Msgf("batcher shutdown.")
		case "pause":
			log.Debug().Msgf("batcher paused for %v ms to alleviate pressure on the datastore.", val)
		case "audit-fail":
			log.Debug().Msgf("batcher audit-fail: %v", *msg)
		case "audit-pass":
			//log.Debug().Msgf("batcher audit-pass: no irregularities were found.")
		case "audit-skip":
			//log.Debug().Msgf("batcher audit-skip: the target capacity is still in flux.")
		}
	})
	defer batcher.RemoveListener(batcherListener)

	// start the batcher
	batcher.Start()

	http.HandleFunc("/query", func(res http.ResponseWriter, req *http.Request) {
		// use same batcher here
		// use different watcher here
	})

	// handle the ingest
	http.HandleFunc("/ingest", func(res http.ResponseWriter, req *http.Request) {
		log.Debug().Msgf("started ingest request...")
		started := time.Now()

		// create a wait-group to ensure everything gets done
		wg := sync.WaitGroup{}
		total := rand.Intn(20000) + 1000
		var completed uint32
		wg.Add(total)

		// create a batch watcher
		watcher := gobatcher.NewWatcher(func(batch []*gobatcher.Operation, done func()) {
			len := len(batch)
			log.Info().Msgf("inserting a batch of %v records...", len)
			optime := rand.Intn(1000)
			time.Sleep(time.Duration(optime) * time.Millisecond)
			log.Info().Msgf("inserted a batch of %v records after %v ms.", len, optime)
			done()
			atomic.AddUint32(&completed, uint32(len))
			for i := 0; i < len; i++ {
				wg.Done()
			}
		}).
			WithMaxAttempts(3).
			WithMaxBatchSize(10)

		// insert some fake records into a fake datastore
		log.Debug().Msgf("generating %v records...", total)
		for i := 0; i < total; i++ {
			payload := struct{}{}
			op := gobatcher.NewOperation(watcher, 10, payload).AllowBatch()
			if errorOnEnqueue := batcher.Enqueue(op); errorOnEnqueue != nil {
				panic(errorOnEnqueue)
			}
		}
		log.Debug().Msgf("generated %v records.", total)

		// create some status reporting
		done := make(chan bool)
		reporter := time.NewTicker(1 * time.Second)
		display := func() {
			log.Debug().Msgf("total: %v, buffered: %v, completed: %v, capacity: %v allocated of %v needed, elapsed: %v.",
				total,
				batcher.OperationsInBuffer(),
				atomic.LoadUint32(&completed),
				azresource.Capacity(),
				batcher.NeedsCapacity(),
				time.Since(started),
			)
		}
		go func() {
			for {
				select {
				case <-done:
					log.Trace().Msgf("reporter is shutdown.")
					return
				case <-reporter.C:
					display()
				}
			}
		}()

		// wait for completion
		wg.Wait()
		close(done)
		display()

		log.Debug().Msgf("completed ingest request.")
	})

	// start listening for HTTP requests
	fmt.Printf("LISTENING on %v:\n", PORT)
	err := http.ListenAndServe(fmt.Sprintf(":%v", PORT), nil)
	if err != nil {
		panic(err)
	}

}
