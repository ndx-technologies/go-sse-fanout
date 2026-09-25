package ssefanout

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"time"
)

type Config struct {
	Channel string        `json:"channel"` // namespace of the channels, "sse"
	TTL     time.Duration `json:"ttl"`     // how long a socket claim outlives its refresher
	Renew   time.Duration `json:"renew"`   // how often the holder refreshes the claim and beats
	Pod     string        `json:"pod"`     // identity of this process

	event string
}

func (s Config) WithDefaults() Config {
	if s.Channel == "" {
		s.Channel = "sse"
	}
	if s.TTL == 0 {
		s.TTL = time.Minute
	}
	if s.Renew == 0 {
		s.Renew = 20 * time.Second
	}
	return s
}

func (s Config) WithEvent(name string) Config {
	s.event = name
	return s
}

func (s Config) eventChannel() string {
	if s.event == "" {
		return s.Channel
	}
	return s.Channel + ":" + s.event
}

func (s Config) keyUserChannel(user string) string { return s.Channel + ":user:" + user }

// Envelope is what crosses processes: the user the event is for, the name of the event, and the
// event itself as bytes. An envelope that carries a socket is a kick, telling the socket it names
// that another socket took the user over.
type Envelope struct {
	User    string         `json:"user"`
	Event   string         `json:"event,omitzero"`
	Socket  SocketID       `json:"socket,omitzero"`
	Payload jsontext.Value `json:"payload,omitzero"`
}

type SocketID uint64

func NewSocketID() SocketID {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return SocketID(binary.BigEndian.Uint64(b[:]))
}

func (s SocketID) String() string { return strconv.FormatUint(uint64(s), 16) }

func (s SocketID) IsZero() bool { return s == 0 }

func (s SocketID) MarshalText() ([]byte, error) { return []byte(s.String()), nil }

func (s *SocketID) UnmarshalText(text []byte) error {
	id, err := strconv.ParseUint(string(text), 16, 64)
	if err != nil {
		return err
	}
	*s = SocketID(id)
	return nil
}

type Key interface {
	comparable
	String() string
}

type SocketClaimReader interface {
	Get(ctx context.Context, key string) (SocketClaim, error)
}

// PubSub carries envelopes between processes: publish one to a channel, and follow a channel of
// this process. RedisPubSub is the one over Redis.
type PubSub interface {
	Publish(ctx context.Context, channel string, envelope Envelope) error
	Listen(ctx context.Context, channel string) (<-chan Envelope, error)
	Unlisten(ctx context.Context, channel string, listen <-chan Envelope) error
}

// Hub is one event: its channel, which every process follows, and the sockets of this process that
// carry it.
//
//   - a socket this process holds is written directly;
//   - a socket another process holds is reached through that user's channel;
//   - a user nobody is known to hold goes to the event channel, where every process filters by the
//     sockets it holds.
//
// The claim is the shortcut, the broadcast is the guarantee.
type Hub[K Key, T any] struct {
	Config    Config
	PubSub    PubSub
	Owner     SocketClaimReader
	Sockets   *Fanout[T, K]
	ParseUser func(s string) (K, error) // inverse of K.String, for the users that arrived from another process
}

func NewHub[K Key, T any](
	config Config,
	connection PubSub,
	owner SocketClaimReader,
	parseUser func(string) (K, error),
) *Hub[K, T] {
	return &Hub[K, T]{
		Config:    config,
		PubSub:    connection,
		Owner:     owner,
		Sockets:   NewFanout[T, K](),
		ParseUser: parseUser,
	}
}

func (s *Hub[K, T]) Start(ctx context.Context) error {
	channel, err := s.PubSub.Listen(ctx, s.Config.eventChannel())
	if err != nil {
		return err
	}
	go s.serve(ctx, channel)
	return nil
}

func (s *Hub[K, T]) Join(user K, sock *Socket[K]) {
	sink := &hubSink[K, T]{hub: s, user: user, event: s.Config.event, socket: sock}
	s.Sockets.Subscribe(user, sink)
	sock.attach(s.Config.event, sink)
}

func (s *Hub[K, T]) Publish(ctx context.Context, users []K, value T) error {
	var errs []error
	var payload jsontext.Value

	for _, user := range users {
		if s.Sockets.IsHolder(user) {
			s.Sockets.PublishTo(user, value)
		}

		k := user.String()

		claim, err := s.Owner.Get(ctx, k)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if !claim.IsZero() && claim.Pod == s.Config.Pod {
			continue // written above, no hop needed
		}

		if payload == nil {
			if payload, err = json.Marshal(value); err != nil {
				return err
			}
		}

		envelope := Envelope{User: k, Event: s.Config.event, Payload: payload}
		channel := s.Config.eventChannel()
		if !claim.IsZero() {
			channel = s.Config.keyUserChannel(k)
		}
		errs = append(errs, s.PubSub.Publish(ctx, channel, envelope))
	}
	return errors.Join(errs...)
}

// serve hands what the event channel carries to the sockets of this process.
func (s *Hub[K, T]) serve(ctx context.Context, channel <-chan Envelope) {
	for {
		select {
		case <-ctx.Done():
			return
		case envelope, ok := <-channel:
			if !ok {
				return
			}

			user, err := s.ParseUser(envelope.User)
			if err != nil {
				slog.ErrorContext(ctx, "cannot parse the user of an sse event", "error", err, "user", envelope.User)
				continue
			}
			if !s.Sockets.IsHolder(user) {
				continue
			}

			var value T
			if err := json.Unmarshal(envelope.Payload, &value); err != nil {
				slog.ErrorContext(ctx, "cannot decode sse event", "error", err, "user", envelope.User)
				continue
			}
			s.Sockets.PublishTo(user, value)
		}
	}
}

// hubSink is one event riding on one socket: it writes what the process holds natively, and what
// arrives from another process after decoding it once.
type hubSink[K Key, T any] struct {
	hub    *Hub[K, T]
	user   K
	event  string
	socket *Socket[K]
}

func (s *hubSink[K, T]) Write(value T) error {
	return s.socket.write(s.event, func(w io.Writer) error { return json.MarshalWrite(w, value) })
}

func (s *hubSink[K, T]) write(envelope Envelope) error {
	var value T
	if err := json.Unmarshal(envelope.Payload, &value); err != nil {
		return err
	}
	return s.Write(value)
}

func (s *hubSink[K, T]) detach(_ context.Context) { s.hub.Sockets.UnSubscribe(s.user, s) }
