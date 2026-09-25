package ssefanout

import (
	"log/slog"
	"sync"
)

type Sink[T any] interface{ Write(T) error }

type Fanout[T any, K comparable] struct {
	mu    sync.RWMutex
	users map[K]map[Sink[T]]struct{}
}

func NewFanout[T any, K comparable]() *Fanout[T, K] {
	return &Fanout[T, K]{users: make(map[K]map[Sink[T]]struct{})}
}

func (s *Fanout[T, K]) Subscribe(u K, sink Sink[T]) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.users[u] == nil {
		s.users[u] = make(map[Sink[T]]struct{})
	}
	s.users[u][sink] = struct{}{}
}

func (s *Fanout[T, K]) UnSubscribe(u K, sink Sink[T]) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.users[u], sink)
	if len(s.users[u]) == 0 {
		delete(s.users, u)
	}
}

func (s *Fanout[T, K]) IsHolder(u K) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.users[u]) > 0
}

func (s *Fanout[T, K]) PublishTo(u K, v T) {
	s.mu.RLock()
	sinks := make([]Sink[T], 0, len(s.users[u]))
	for sink := range s.users[u] {
		sinks = append(sinks, sink)
	}
	s.mu.RUnlock()

	for _, sink := range sinks {
		if err := sink.Write(v); err != nil {
			slog.Warn("cannot write the event", "error", err, "user", u)
		}
	}
}
