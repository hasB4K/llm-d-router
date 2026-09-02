/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package disaggregatedsetrollout

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestLocalGetOrSetIsAtomic(t *testing.T) {
	store := localGetOrSet{}
	const callers = 32
	results := make(chan string, callers)
	created := make(chan bool, callers)

	var group sync.WaitGroup
	for i := range callers {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			value, existed := store.GetOrSet("decision-id", fmt.Sprintf("revision-%d", i))
			results <- value
			created <- !existed
		}(i)
	}
	group.Wait()
	close(results)
	close(created)

	var winner string
	for value := range results {
		if winner == "" {
			winner = value
		}
		if value != winner {
			t.Fatalf("local values differ: %q and %q", winner, value)
		}
	}
	createdCount := 0
	for wasCreated := range created {
		if wasCreated {
			createdCount++
		}
	}
	if createdCount != 1 {
		t.Fatalf("created count = %d, want 1", createdCount)
	}
}

func TestLocalGetOrSetExpires(t *testing.T) {
	store := localGetOrSet{
		values: map[string]localGetOrSetValue{
			"decision-id": {
				value:     "old-revision",
				expiresAt: time.Now().Add(-time.Second),
			},
		},
	}

	value, existed := store.GetOrSet("decision-id", "new-revision")
	if existed || value != "new-revision" {
		t.Fatalf("expired local value = (%q, %t), want (new-revision, false)", value, existed)
	}
}

func TestLocalGetOrSetGCDropsExpiredValuesWithoutRequest(t *testing.T) {
	store := localGetOrSet{
		values: map[string]localGetOrSetValue{
			"decision-id": {
				value:     "old-revision",
				expiresAt: time.Now().Add(-time.Second),
			},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		store.runGC(ctx, time.Millisecond)
		close(done)
	}()

	deadline := time.After(250 * time.Millisecond)
	for {
		store.mu.Lock()
		_, found := store.values["decision-id"]
		store.mu.Unlock()
		if !found {
			break
		}
		select {
		case <-deadline:
			t.Fatal("expired value was not removed by the background GC")
		case <-time.After(time.Millisecond):
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("background GC did not stop after context cancellation")
	}
}
