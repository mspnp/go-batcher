package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	gobatcher "github.com/plasne/go-batcher/v2"
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
	flag.IntVar(&flagCapacity, "capacity", -1, "Determines the capacity to shared. This overrides CAPACITY.")

	// seed the random number generator
	rand.Seed(time.Now().UnixNano())

}

func main() {
	ctx := context.Background()

	// complete configuration
	flag.Parse()
	PORT := goconfig.AsInt().TrySetValue(flagPort).TrySetByEnv("PORT").DefaultTo(8080).Print().Value()
	CAPACITY := goconfig.AsInt().SetEmpty(-1).TrySetValue(flagCapacity).TrySetByEnv("CAPACITY").DefaultTo(10000).Print().Value()
	AZBLOB_ACCOUNT := goconfig.AsString().TrySetByEnv("AZBLOB_ACCOUNT").Print().Require().Value()
	AZBLOB_KEY := goconfig.AsString().TrySetByEnv("AZBLOB_KEY").PrintMasked().Require().Value()
	AZBLOB_CONTAINER := goconfig.AsString().TrySetByEnv("AZBLOB_CONTAINER").Print().Require().Value()

	// start getting shared resource capacity
	leaseMgr := gobatcher.NewAzureBlobLeaseManager(AZBLOB_ACCOUNT, AZBLOB_CONTAINER, AZBLOB_KEY)
	azresource := gobatcher.NewSharedResource().
		WithSharedCapacity(uint32(CAPACITY), leaseMgr).
		WithFactor(1000)
	resourceListener := azresource.AddListener(func(event string, val int, msg string, metadata interface{}) {
		switch event {
		case gobatcher.ShutdownEvent:
			log.Debug().Msgf("SharedResource shutdown.")
		case gobatcher.CapacityEvent:
			log.Trace().Msgf("SharedResource has procured %v capacity.", val)
		case gobatcher.FailedEvent:
			log.Trace().Msgf("SharedResource failed to take control of partition %v.", val)
		case gobatcher.ReleasedEvent:
			log.Trace().Msgf("SharedResource lost control of partition %v.", val)
		case gobatcher.AllocatedEvent:
			log.Trace().Msgf("SharedResource gained control of partition %v.", val)
		case gobatcher.ErrorEvent:
			log.Err(errors.New(msg)).Msgf("SharedResource raised the following error...")
		case gobatcher.CreatedContainerEvent:
			log.Trace().Msgf("SharedResource created container %v.", msg)
		case gobatcher.VerifiedContainerEvent:
			log.Trace().Msgf("SharedResource verified that container %v already exists.", msg)
		case gobatcher.CreatedBlobEvent:
			log.Trace().Msgf("SharedResource created blob %v.", val)
		case gobatcher.VerifiedBlobEvent:
			log.Trace().Msgf("SharedResource verified that blob %v already exists.", val)
		}
	})
	defer azresource.RemoveListener(resourceListener)
	if err := azresource.Start(ctx); err != nil {
		panic(err)
	}

	// configure the batcher
	batcher := gobatcher.NewBatcher().
		WithRateLimiter(azresource)
	batcherListener := batcher.AddListener(func(event string, val int, msg string, metadata interface{}) {
		switch event {
		case gobatcher.ShutdownEvent:
			log.Debug().Msgf("batcher shutdown.")
		case gobatcher.PauseEvent:
			log.Debug().Msgf("batcher paused for %v ms to alleviate pressure on the datastore.", val)
		case gobatcher.AuditFailEvent:
			log.Debug().Msgf("batcher audit-fail: %v", msg)
		case gobatcher.AuditPassEvent:
			//log.Debug().Msgf("batcher audit-pass: no irregularities were found.")
		case gobatcher.AuditSkipEvent:
			//log.Debug().Msgf("batcher audit-skip: the target capacity is still in flux.")
		case gobatcher.BatchEvent:
			operations := metadata.([]*gobatcher.Operation)
			log.Debug().Msgf("batcher raised a batch with %v operations...", val)
			for _, op := range operations {
				log.Debug().Msgf("...including %+v", op)
			}
		}
	})
	defer batcher.RemoveListener(batcherListener)

	// start the batcher
	err := batcher.Start(ctx)
	if err != nil {
		panic(err)
	}

	var capacityOffset uint32

	http.HandleFunc("/inc", func(res http.ResponseWriter, req *http.Request) {
		capacityOffset += 1000
		capacity := capacityOffset + uint32(CAPACITY)
		azresource.SetSharedCapacity(capacity)
		log.Debug().Msgf("increased shared capacity by 1000 to %v.", capacity)
	})

	http.HandleFunc("/dec", func(res http.ResponseWriter, req *http.Request) {
		if capacityOffset+uint32(CAPACITY) >= 1000 {
			capacityOffset -= 1000
			capacity := capacityOffset + uint32(CAPACITY)
			azresource.SetSharedCapacity(capacity)
			log.Debug().Msgf("decreased shared capacity by 1000 to %v.", capacity)
		} else {
			log.Debug().Msgf("shared capacity cannot go below 0.")
		}
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
		watcher := gobatcher.NewWatcher(func(batch []gobatcher.Operation) {
			len := len(batch)
			log.Info().Msgf("inserting a batch of %v records...", len)
			optime := rand.Intn(1000)
			time.Sleep(time.Duration(optime) * time.Millisecond)
			log.Info().Msgf("inserted a batch of %v records after %v ms.", len, optime)
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
			op := gobatcher.NewOperation(watcher, 10, payload, true)
			if errorOnEnqueue := batcher.Enqueue(op); errorOnEnqueue != nil {
				res.WriteHeader(http.StatusInternalServerError)
				_, _ = res.Write([]byte(errorOnEnqueue.Error()))
				return
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
	err = http.ListenAndServe(fmt.Sprintf(":%v", PORT), nil)
	if err != nil {
		panic(err)
	}

}
