package ssefanout

import (
	"context"
	"encoding/json/v2"
	"errors"
	"log/slog"
	"sync"

	"github.com/redis/go-redis/v9"
)

// RedisPubSub publishes to Redis and delivers what it receives to the local subscribers.
type RedisPubSub struct {
	db *redis.Client

	mu          sync.Mutex
	sub         *redis.PubSub
	subscribers map[string][]chan Envelope // channel name to the local channels of this process
}

func NewRedisPubSub(db *redis.Client) *RedisPubSub {
	return &RedisPubSub{db: db, subscribers: make(map[string][]chan Envelope)}
}

func (c *RedisPubSub) Start(ctx context.Context) error {
	sub := c.db.Subscribe(ctx)
	if err := sub.Ping(ctx); err != nil {
		return err
	}

	c.mu.Lock()
	c.sub = sub
	c.mu.Unlock()

	go c.receive(ctx, sub)
	return nil
}

func (c *RedisPubSub) Listen(ctx context.Context, channel string) (<-chan Envelope, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.sub == nil {
		return nil, errors.New("redis pub/sub is not started")
	}
	if err := c.sub.Subscribe(ctx, channel); err != nil {
		return nil, err
	}

	ch := make(chan Envelope, 100)
	c.subscribers[channel] = append(c.subscribers[channel], ch)
	return ch, nil
}

// Unlisten stops this listener, and drops the channel once nobody is left on it: a user channel is
// followed by every socket of that user, and during a take-over two of them are subscribed at once.
func (c *RedisPubSub) Unlisten(ctx context.Context, channel string, listen <-chan Envelope) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	subscribers, ok := c.subscribers[channel]
	if !ok {
		return nil
	}
	for i, ch := range subscribers {
		if ch == listen {
			subscribers = append(subscribers[:i], subscribers[i+1:]...)
			break
		}
	}
	if len(subscribers) > 0 {
		c.subscribers[channel] = subscribers
		return nil
	}

	delete(c.subscribers, channel)
	return c.sub.Unsubscribe(ctx, channel)
}

func (c *RedisPubSub) Publish(ctx context.Context, channel string, envelope Envelope) error {
	payload, err := json.Marshal(envelope)
	if err != nil {
		return err
	}
	return c.db.Publish(ctx, channel, payload).Err()
}

func (c *RedisPubSub) receive(ctx context.Context, sub *redis.PubSub) {
	for {
		select {
		case <-ctx.Done():
			return
		case m, ok := <-sub.Channel():
			if !ok {
				return
			}

			var envelope Envelope
			if err := json.Unmarshal([]byte(m.Payload), &envelope); err != nil {
				slog.ErrorContext(ctx, "cannot decode envelope", "error", err, "channel", m.Channel)
				continue
			}
			c.deliver(m.Channel, envelope)
		}
	}
}

func (c *RedisPubSub) deliver(channel string, envelope Envelope) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, ch := range c.subscribers[channel] {
		select {
		case ch <- envelope:
		default:
			slog.Warn("dropping envelope for slow subscriber", "channel", channel)
		}
	}
}
