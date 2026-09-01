/*
Copyright 2026 The llm-d Authors.

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

package datalayer

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
)

func TestLocalSyncerGetOrSetIsAtomic(t *testing.T) {
	syncer := NewLocalSyncer("local", "replica")
	const callers = 32
	results := make(chan string, callers)
	created := make(chan bool, callers)

	var group sync.WaitGroup
	for i := range callers {
		group.Add(1)
		go func() {
			defer group.Done()
			actual, existed, err := syncer.GetOrSet(
				context.Background(),
				"revision",
				"request-1",
				fmt.Sprintf("revision-%d", i),
			)
			if err != nil {
				t.Errorf("GetOrSet: %v", err)
				return
			}
			revision, ok := actual.(string)
			if !ok {
				t.Errorf("GetOrSet value type = %T, want string", actual)
				return
			}
			results <- revision
			created <- !existed
		}()
	}
	group.Wait()
	close(results)
	close(created)

	var winner string
	winnerSet := false
	for actual := range results {
		if !winnerSet {
			winner = actual
			winnerSet = true
		}
		if actual != winner {
			t.Fatalf("GetOrSet returned different winners: %v and %v", winner, actual)
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

func TestLocalSyncerGetOrSetStoresAnyValue(t *testing.T) {
	syncer := NewLocalSyncer("local", "replica")
	candidate := map[string]string{"revision": "revision-a"}

	actual, existed, err := syncer.GetOrSet(context.Background(), "values", "key", candidate)
	if err != nil || existed || !reflect.DeepEqual(actual, candidate) {
		t.Fatalf("GetOrSet = (%v, %t, %v)", actual, existed, err)
	}
}
