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
	"sync"
	"time"
)

const (
	localGetOrSetTTL           = 10 * time.Minute
	localGetOrSetSweepInterval = time.Minute
)

type localGetOrSet struct {
	mu     sync.Mutex
	values map[string]localGetOrSetValue
}

type localGetOrSetValue struct {
	value     string
	expiresAt time.Time
}

func (s *localGetOrSet) GetOrSet(key, candidate string) (string, bool) {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.values == nil {
		s.values = make(map[string]localGetOrSetValue)
	}
	if storedValue, found := s.values[key]; found {
		if now.Before(storedValue.expiresAt) {
			return storedValue.value, true
		}
		delete(s.values, key)
	}
	s.values[key] = localGetOrSetValue{
		value:     candidate,
		expiresAt: now.Add(localGetOrSetTTL),
	}
	return candidate, false
}

func (s *localGetOrSet) runGC(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			s.deleteExpired(now)
		}
	}
}

func (s *localGetOrSet) deleteExpired(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for key, value := range s.values {
		if !now.Before(value.expiresAt) {
			delete(s.values, key)
		}
	}
}
