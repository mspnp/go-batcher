
# Issues

## Why is the target so low?

```text
020-12-08T12:03:36-05:00 INF inserted a batch of 10 records after 266 ms.
2020-12-08T12:03:36-05:00 INF inserted a batch of 10 records after 573 ms.
2020-12-08T12:03:36-05:00 INF inserting a batch of 10 records...
2020-12-08T12:03:36-05:00 INF inserting a batch of 10 records...
2020-12-08T12:03:36-05:00 INF inserting a batch of 10 records...
2020-12-08T12:03:36-05:00 INF inserting a batch of 10 records...
2020-12-08T12:03:36-05:00 INF inserted a batch of 10 records after 370 ms.
2020-12-08T12:03:36-05:00 INF inserting a batch of 10 records...
2020-12-08T12:03:36-05:00 INF inserting a batch of 10 records...
2020-12-08T12:03:36-05:00 DBG total: 2004, buffered: 10000, completed: 0, capacity: 1000 allocated of 690 needed, elapsed: 42.126694506s.
2020-12-08T12:03:36-05:00 INF inserted a batch of 10 records after 927 ms.
2020-12-08T12:03:36-05:00 INF inserted a batch of 10 records after 440 ms.
2020-12-08T12:03:36-05:00 INF inserting a batch of 10 records...
2020-12-08T12:03:36-05:00 INF inserted a batch of 10 records after 335 ms.
2020-12-08T12:03:36-05:00 INF inserting a batch of 10 records...
2020-12-08T12:03:36-05:00 INF inserted a batch of 10 records after 48 ms.
2020-12-08T12:03:36-05:00 INF inserting a batch of 10 records...
2020-12-08T12:03:36-05:00 INF inserted a batch of 10 records after 915 ms.
```

## Why did the batch never complete?

```text
2020-12-08T12:04:10-05:00 INF inserted a batch of 10 records after 776 ms.
2020-12-08T12:04:10-05:00 INF inserted a batch of 10 records after 675 ms.
2020-12-08T12:04:10-05:00 DBG total: 14369, buffered: 8873, completed: 7500, capacity: 0 allocated of 0 needed, elapsed: 1m17.822002826s.
2020-12-08T12:04:11-05:00 DBG total: 2004, buffered: 8873, completed: 0, capacity: 0 allocated of 0 needed, elapsed: 1m17.126755397s.
2020-12-08T12:04:11-05:00 DBG total: 14369, buffered: 8873, completed: 7500, capacity: 0 allocated of 0 needed, elapsed: 1m18.819636999s.
2020-12-08T12:04:12-05:00 DBG total: 2004, buffered: 8873, completed: 0, capacity: 0 allocated of 0 needed, elapsed: 1m18.127703893s.
2020-12-08T12:04:12-05:00 DBG total: 14369, buffered: 8873, completed: 7500, capacity: 0 allocated of 0 needed, elapsed: 1m19.819222156s.
2020-12-08T12:04:13-05:00 DBG total: 2004, buffered: 8873, completed: 0, capacity: 0 allocated of 0 needed, elapsed: 1m19.126850917s.
2020-12-08T12:04:13-05:00 DBG total: 14369, buffered: 8873, completed: 7500, capacity: 0 allocated of 0 needed, elapsed: 1m20.817800643s.
```

## Why did the batch never start?

```text
2020-12-08T11:58:59-05:00 TRC AzureSharedResource lost control of partition 1.
2020-12-08T11:58:59-05:00 TRC AzureSharedResource has procured 0 capacity.
2020-12-08T11:59:54-05:00 DBG started ingest request...
2020-12-08T11:59:54-05:00 DBG generating 16373 records...
2020-12-08T11:59:54-05:00 TRC AzureSharedResource failed to take control of partition 9.
```

## Race

```text
==================
WARNING: DATA RACE
Read at 0x00c000124a98 by goroutine 31:
  main.main.func3.2()
      /Users/plasne/Documents/go-batcher/sample/main.go:172 +0x21a
  main.main.func3.3()
      /Users/plasne/Documents/go-batcher/sample/main.go:186 +0x53

Previous write at 0x00c000124a98 by goroutine 55:
  sync/atomic.AddInt32()
      /usr/local/Cellar/go/1.15.5/libexec/src/runtime/race_amd64.s:269 +0xb
  main.main.func3.1()
      /Users/plasne/Documents/go-batcher/sample/main.go:147 +0x22d
  github.com/plasne/go-batcher.(*Batcher).Start.func1.1()
      /Users/plasne/Documents/go-batcher/batcher.go:281 +0x17e

Goroutine 31 (running) created at:
  main.main.func3()
      /Users/plasne/Documents/go-batcher/sample/main.go:179 +0x7ae
  net/http.HandlerFunc.ServeHTTP()
      /usr/local/Cellar/go/1.15.5/libexec/src/net/http/server.go:2042 +0x51
  net/http.(*ServeMux).ServeHTTP()
      /usr/local/Cellar/go/1.15.5/libexec/src/net/http/server.go:2417 +0xaf
  net/http.serverHandler.ServeHTTP()
      /usr/local/Cellar/go/1.15.5/libexec/src/net/http/server.go:2843 +0xca
  net/http.(*conn).serve()
      /usr/local/Cellar/go/1.15.5/libexec/src/net/http/server.go:1925 +0x84c

Goroutine 55 (finished) created at:
  github.com/plasne/go-batcher.(*Batcher).Start.func1()
      /Users/plasne/Documents/go-batcher/batcher.go:272 +0x144
  github.com/plasne/go-batcher.(*Batcher).Start.func2()
      /Users/plasne/Documents/go-batcher/batcher.go:300 +0x7e
  github.com/plasne/go-batcher.(*Batcher).Start.func3()
      /Users/plasne/Documents/go-batcher/batcher.go:387 +0x3c1
==================
```

```text
==================
WARNING: DATA RACE
Write at 0x00c00013cef4 by goroutine 29:
  github.com/plasne/go-batcher.(*Batcher).incTarget()
      /Users/plasne/Documents/go-batcher/batcher.go:246 +0xf3
  github.com/plasne/go-batcher.(*Batcher).Enqueue()
      /Users/plasne/Documents/go-batcher/batcher.go:196 +0xed
  main.main.func3()
      /Users/plasne/Documents/go-batcher/sample/main.go:160 +0x4e6
  net/http.HandlerFunc.ServeHTTP()
      /usr/local/Cellar/go/1.15.5/libexec/src/net/http/server.go:2042 +0x51
  net/http.(*ServeMux).ServeHTTP()
      /usr/local/Cellar/go/1.15.5/libexec/src/net/http/server.go:2417 +0xaf
  net/http.serverHandler.ServeHTTP()
      /usr/local/Cellar/go/1.15.5/libexec/src/net/http/server.go:2843 +0xca
  net/http.(*conn).serve()
      /usr/local/Cellar/go/1.15.5/libexec/src/net/http/server.go:1925 +0x84c

Previous read at 0x00c00013cef4 by goroutine 27:
  sync/atomic.LoadInt32()
      /usr/local/Cellar/go/1.15.5/libexec/src/runtime/race_amd64.s:206 +0xb
  github.com/plasne/go-batcher.(*Batcher).NeedsCapacity()
      /Users/plasne/Documents/go-batcher/batcher.go:218 +0xa16
  github.com/plasne/go-batcher.(*Batcher).Start.func3()
      /Users/plasne/Documents/go-batcher/batcher.go:351 +0xa06

Goroutine 29 (running) created at:
  net/http.(*Server).Serve()
      /usr/local/Cellar/go/1.15.5/libexec/src/net/http/server.go:2969 +0x5d3
  net/http.(*Server).ListenAndServe()
      /usr/local/Cellar/go/1.15.5/libexec/src/net/http/server.go:2866 +0x105
  net/http.ListenAndServe()
      /usr/local/Cellar/go/1.15.5/libexec/src/net/http/server.go:3120 +0xfce
  main.main()
      /Users/plasne/Documents/go-batcher/sample/main.go:201 +0xe65

Goroutine 27 (running) created at:
  github.com/plasne/go-batcher.(*Batcher).Start()
      /Users/plasne/Documents/go-batcher/batcher.go:310 +0x49b
  main.main()
      /Users/plasne/Documents/go-batcher/sample/main.go:126 +0xcce
==================
```
