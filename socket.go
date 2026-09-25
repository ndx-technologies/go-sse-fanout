package ssefanout

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"time"
)

var heartbeat = []byte(": heartbeat\n\n")

// eventSink is one event riding on one socket.
type eventSink interface {
	// write hands the socket an event that arrived from another process: the payload is decoded as
	// this event's own type and then encoded into the response like a local one.
	write(envelope Envelope) error
	// detach removes the event from the socket, and the socket from the event.
	detach(ctx context.Context)
}

// frame is one event on its way into the response: its name, and the value of that event's own
// type, kept native until it is encoded into the response.
type frame struct {
	event  string
	encode func(io.Writer) error
}

type SocketClaimDB interface {
	Claim(ctx context.Context, key string, socket SocketID) (SocketClaim, error)
	Refresh(ctx context.Context, key string, socket SocketID) (bool, error)
	Release(ctx context.Context, key string, socket SocketID) error
}

// Socket is one client's SSE response connection: the claim of its user, the frames written into the
// response, and the events riding on it. It ends when the client goes away, when another
// socket takes the user over, or when the claim is lost — and every event ends with it.
type Socket[K Key] struct {
	Config     Config
	Connection PubSub
	Owner      SocketClaimDB

	user   K
	socket SocketID
	out    io.Writer
	listen <-chan Envelope

	frames chan frame
	events map[string]eventSink

	mu     sync.Mutex
	ended  bool
	done   chan struct{}
	cancel context.CancelFunc
}

func NewSocket[K Key](config Config, connection PubSub, owner SocketClaimDB, user K, out io.Writer) *Socket[K] {
	return &Socket[K]{
		Config:     config,
		Connection: connection,
		Owner:      owner,
		user:       user,
		out:        out,
		frames:     make(chan frame, 64),
		events:     make(map[string]eventSink),
		done:       make(chan struct{}),
	}
}

// Subscribe takes the user over, follows the user's channel,
// and streams into the response until the socket ends.
// Every event must Hub.Join() the socket first: an envelope for an event that has not joined is dropped.
func (s *Socket[K]) Subscribe(ctx context.Context) error {
	k := s.user.String()

	channel, err := s.Connection.Listen(ctx, s.Config.keyUserChannel(k))
	if err != nil {
		return err
	}

	socket := NewSocketID()
	displaced, err := s.Owner.Claim(ctx, k, socket)
	if err != nil {
		_ = s.Connection.Unlisten(ctx, s.Config.keyUserChannel(k), channel)
		return err
	}
	s.socket = socket
	s.listen = channel

	if !displaced.Socket.IsZero() && displaced.Socket != socket {
		kick := Envelope{User: k, Socket: socket}
		if err := s.Connection.Publish(ctx, s.Config.keyUserChannel(k), kick); err != nil {
			slog.ErrorContext(ctx, "cannot kick the displaced socket", "error", err, "user", k)
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel

	go s.follow(ctx, channel)
	go s.serve(ctx)
	return nil
}

// Done is closed when the socket ends, so whoever serves the response knows to return.
func (s *Socket[K]) Done() <-chan struct{} { return s.done }

func (s *Socket[K]) attach(event string, sink eventSink) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events[event] = sink
}

// write queues one event. The encoder carries the value natively: no marshalled bytes are made
// here, and the frame is encoded straight into the response when it is written.
func (s *Socket[K]) write(event string, encode func(io.Writer) error) error {
	select {
	case <-s.done:
		return nil
	case s.frames <- frame{event: event, encode: encode}:
		return nil
	default:
		slog.Warn("dropping event for slow socket", "user", s.user.String(), "event", event)
		return nil
	}
}

// serve writes what the queue carries into the response, beats, and keeps the ownership alive.
func (s *Socket[K]) serve(ctx context.Context) {
	k := s.user.String()
	ticker := time.NewTicker(s.Config.Renew)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.end(ctx)
			return
		case f := <-s.frames:
			if err := s.writeFrame(f); err != nil {
				slog.InfoContext(ctx, "cannot write sse frame", "error", err, "user", k)
				s.end(ctx)
				return
			}
		case <-ticker.C:
			held, err := s.Owner.Refresh(ctx, k, s.socket)
			if err != nil {
				if ctx.Err() != nil {
					s.end(ctx)
					return
				}
				slog.ErrorContext(ctx, "cannot refresh sse ownership", "error", err, "user", k)
				continue
			}
			if !held {
				slog.InfoContext(ctx, "sse ownership lost, dropping the socket", "user", k)
				s.end(ctx)
				return
			}
			if err := s.writeHeartbeat(); err != nil {
				s.end(ctx)
				return
			}
		}
	}
}

// follow hands what the user channel carries to the events riding on the socket, and ends it when
// another socket takes the user over.
func (s *Socket[K]) follow(ctx context.Context, channel <-chan Envelope) {
	k := s.user.String()

	for {
		select {
		case <-ctx.Done():
			return
		case envelope, ok := <-channel:
			if !ok {
				return
			}

			if !envelope.Socket.IsZero() {
				if envelope.Socket == s.socket {
					continue
				}
				slog.InfoContext(ctx, "socket displaced, dropping the stream", "user", k)
				s.end(ctx)
				return
			}

			sink := s.event(envelope.Event)
			if sink == nil {
				slog.WarnContext(ctx, "no such event on this socket", "event", envelope.Event, "user", k)
				continue
			}
			if err := sink.write(envelope); err != nil {
				slog.ErrorContext(ctx, "cannot write the sse event", "error", err, "event", envelope.Event, "user", k)
			}
		}
	}
}

func (s *Socket[K]) event(name string) eventSink {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.events[name]
}

func (s *Socket[K]) writeFrame(f frame) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.ended {
		return nil
	}
	if _, err := io.WriteString(s.out, "event: "+f.event+"\ndata: "); err != nil {
		return err
	}
	if err := f.encode(s.out); err != nil {
		return err
	}
	if _, err := io.WriteString(s.out, "\n\n"); err != nil {
		return err
	}
	if flusher, ok := s.out.(interface{ Flush() }); ok {
		flusher.Flush()
	}
	return nil
}

func (s *Socket[K]) writeHeartbeat() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.ended {
		return nil
	}
	if _, err := s.out.Write(heartbeat); err != nil {
		return err
	}
	if flusher, ok := s.out.(interface{ Flush() }); ok {
		flusher.Flush()
	}
	return nil
}

// end releases the ownership, takes every event off the socket, and stops the writers. Writes hold
// the same lock, so none of them outlives this.
func (s *Socket[K]) end(ctx context.Context) {
	k := s.user.String()

	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return
	}
	s.ended = true

	events := make([]eventSink, 0, len(s.events))
	for _, sink := range s.events {
		events = append(events, sink)
	}
	s.events = make(map[string]eventSink)
	s.mu.Unlock()

	ctx = context.WithoutCancel(ctx)
	for _, sink := range events {
		sink.detach(ctx)
	}
	if !s.socket.IsZero() {
		if err := s.Owner.Release(ctx, k, s.socket); err != nil {
			slog.ErrorContext(ctx, "cannot release sse ownership", "error", err, "user", k)
		}
	}
	if err := s.Connection.Unlisten(ctx, s.Config.keyUserChannel(k), s.listen); err != nil {
		slog.ErrorContext(ctx, "cannot stop following the user channel", "error", err, "user", k)
	}

	if s.cancel != nil {
		s.cancel()
	}
	close(s.done)
}
