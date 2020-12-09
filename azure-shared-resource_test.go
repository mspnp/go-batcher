package batcher_test

import (
	"context"
	"testing"

	gobatcher "github.com/plasne/go-batcher"
)

func TestProvision(t *testing.T) {
	ctx := context.Background()

	// TODO write tests that fail if not using the new() methods
	/*
		t.Run("max capacity is required", func(t *testing.T) {
			res := gobatcher.NewAzureSharedResourceMock(10000)
			err := res.Provision(ctx)
			if _, ok := err.(gobatcher.UndefinedMaxCapacityError); !ok {
				t.Errorf("expected undefined max capacity")
			}
		})
	*/

	t.Run("too many partitions", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResourceMock(10000)
		err := res.Provision(ctx)
		if e, ok := err.(gobatcher.PartitionsOutOfRangeError); ok {
			if e.MaxCapacity != 10000 {
				t.Errorf("expected max-capacity to be 10,000 (configured value)")
			}
			if e.Factor != 1 {
				t.Errorf("expected factor to be 1 (the default)")
			}
			if e.PartitionCount != 10000 {
				t.Errorf("expected partition-count to be 10,000 (calculated value)")
			}
		} else {
			t.Errorf("expected partitions out of range")
		}
	})

	t.Run("provision is called exactly once", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResourceMock(10000).
			WithFactor(1000)
		count := 0
		res.AddListener(func(event string, val int, msg *string) {
			switch event {
			case "test:provision":
				count++
			}
		})
		err := res.Provision(ctx)
		if err != nil {
			t.Error(err)
		}
		if count != 1 {
			t.Errorf("expected a single call to provision")
		}
	})

	t.Run("correct number of partitions are created", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResourceMock(10000).
			WithFactor(1000)
		count := 0
		res.AddListener(func(event string, val int, msg *string) {
			switch event {
			case "test:create-partition":
				count++
			}
		})
		err := res.Provision(ctx)
		if err != nil {
			t.Error(err)
		}
		if count != 10 {
			t.Errorf("expected 10 partitions to be created")
		}
	})

	t.Run("partial partitions round up", func(t *testing.T) {
		res := gobatcher.NewAzureSharedResourceMock(10050).
			WithFactor(1000)
		count := 0
		res.AddListener(func(event string, val int, msg *string) {
			switch event {
			case "test:create-partition":
				count++
			}
		})
		err := res.Provision(ctx)
		if err != nil {
			t.Error(err)
		}
		if count != 11 {
			t.Errorf("expected 11 partitions to be created")
		}
	})

}
