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
	"encoding/json"
	"os"
	"sync"
	"time"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
)

const (
	LocalSyncerType  = "local-syncer"
	localGetOrSetTTL = 10 * time.Minute
)

var _ fwkdl.CrossReplicaSyncer = (*LocalSyncer)(nil)

// LocalSyncer is an in-memory CrossReplicaSyncer for single-replica
// deployments and tests. No cross-replica sync is performed.
type LocalSyncer struct {
	typedName fwkplugin.TypedName
	replicaID string
	data      sync.Map // key: "replicaID:StateKey:endpointID", value: any

	decisionMu sync.Mutex
	decisions  map[decisionKey]decisionValue
	lastSweep  time.Time
}

type decisionKey struct {
	stateKey fwkdl.StateKey
	id       string
}

type decisionValue struct {
	value     any
	expiresAt time.Time
}

func NewLocalSyncer(name, replicaID string) *LocalSyncer {
	return &LocalSyncer{
		typedName: fwkplugin.TypedName{Type: LocalSyncerType, Name: name},
		replicaID: replicaID,
		decisions: make(map[decisionKey]decisionValue),
	}
}

func LocalSyncerFactory(name string, _ *json.Decoder, _ fwkplugin.Handle) (fwkplugin.Plugin, error) {
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "local"
	}
	return NewLocalSyncer(name, hostname), nil
}

func (s *LocalSyncer) TypedName() fwkplugin.TypedName {
	return s.typedName
}

func (s *LocalSyncer) syncKey(key fwkdl.StateKey, endpointID string) string {
	return s.replicaID + ":" + string(key) + ":" + endpointID
}

func (s *LocalSyncer) Set(_ context.Context, key fwkdl.StateKey, endpointID string, value any) error {
	s.data.Store(s.syncKey(key, endpointID), value)
	return nil
}

func (s *LocalSyncer) Get(_ context.Context, key fwkdl.StateKey, endpointID string, _ func([]any) any) (any, bool, error) {
	v, ok := s.data.Load(s.syncKey(key, endpointID))
	return v, ok, nil
}

func (s *LocalSyncer) Delete(_ context.Context, key fwkdl.StateKey, endpointID string) error {
	s.data.Delete(s.syncKey(key, endpointID))
	return nil
}

// GetOrSet coordinates a value within this process. It is suitable for a
// single EPP replica and for tests, but does not synchronize across replicas.
func (s *LocalSyncer) GetOrSet(_ context.Context, key fwkdl.StateKey, id string, candidate any) (any, bool, error) {
	now := time.Now()
	s.decisionMu.Lock()
	defer s.decisionMu.Unlock()

	if s.lastSweep.IsZero() || now.Sub(s.lastSweep) >= time.Minute {
		for storedKey, stored := range s.decisions {
			if !now.Before(stored.expiresAt) {
				delete(s.decisions, storedKey)
			}
		}
		s.lastSweep = now
	}

	decisionKey := decisionKey{stateKey: key, id: id}
	if stored, ok := s.decisions[decisionKey]; ok {
		if now.Before(stored.expiresAt) {
			return stored.value, true, nil
		}
		delete(s.decisions, decisionKey)
	}
	s.decisions[decisionKey] = decisionValue{value: candidate, expiresAt: now.Add(localGetOrSetTTL)}
	return candidate, false, nil
}
