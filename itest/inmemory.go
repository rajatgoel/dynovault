package itest

import (
	"context"
	"github.com/rajatgoel/dynovault/handler"
	"sync"
)

type inmemory struct {
	mu    sync.Mutex
	store map[string]string
}

func newInMemory() *inmemory {
	return &inmemory{
		store: make(map[string]string),
	}
}

func (m *inmemory) Get(_ context.Context, key []byte) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	v, ok := m.store[string(key)]
	if !ok {
		return nil, handler.ErrNotFound
	}

	return []byte(v), nil
}

func (m *inmemory) Put(_ context.Context, key []byte, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.store[string(key)] = string(value)
	return nil
}
