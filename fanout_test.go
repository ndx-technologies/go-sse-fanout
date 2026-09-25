package ssefanout

import (
	"errors"
	"sync"
	"testing"
)

// failing is a socket whose write always fails.
type failing struct{}

func (failing) Write(string) error { return errors.New("no such client") }

// collector is a socket standing in for a response.
type collector struct {
	mu     sync.Mutex
	values []string
}

func (c *collector) Write(value string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.values = append(c.values, value)
	return nil
}

func (c *collector) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.values)
}

func TestFanoutPublishTo(t *testing.T) {
	fanout := NewFanout[string, user]()
	alice, bob := &collector{}, &collector{}

	fanout.Subscribe(user("alice"), alice)
	fanout.Subscribe(user("alice"), bob)
	fanout.PublishTo(user("alice"), "hello")

	if alice.count() != 1 || bob.count() != 1 {
		t.Error("got", alice.count(), bob.count(), "want 1, 1")
	}
}

func TestFanoutPublishToKeepsGoingWhenASinkFails(t *testing.T) {
	fanout := NewFanout[string, user]()
	good := &collector{}
	fanout.Subscribe(user("alice"), failing{})
	fanout.Subscribe(user("alice"), good)

	fanout.PublishTo(user("alice"), "hello")

	if good.count() != 1 {
		t.Error("the sink that works got", good.count(), "values, want 1")
	}
}

func TestFanoutKeepsUsersApart(t *testing.T) {
	fanout := NewFanout[string, user]()
	alice, bob := &collector{}, &collector{}

	fanout.Subscribe(user("alice"), alice)
	fanout.Subscribe(user("bob"), bob)
	fanout.PublishTo(user("alice"), "hello")

	if bob.count() != 0 {
		t.Error("another user's socket received the event")
	}
}

func TestFanoutUnSubscribeKeepsTheOtherSockets(t *testing.T) {
	fanout := NewFanout[string, user]()
	first, second := &collector{}, &collector{}

	fanout.Subscribe(user("alice"), first)
	fanout.Subscribe(user("alice"), second)
	fanout.UnSubscribe(user("alice"), first)
	fanout.PublishTo(user("alice"), "hello")

	if first.count() != 0 || second.count() != 1 {
		t.Error("got", first.count(), second.count(), "want 0, 1")
	}
	if !fanout.IsHolder(user("alice")) {
		t.Error("the user was dropped while a socket still held it")
	}
}

func TestFanoutIsHolder(t *testing.T) {
	fanout := NewFanout[string, user]()
	sink := &collector{}

	if fanout.IsHolder(user("alice")) {
		t.Error("a user nobody holds")
	}
	fanout.Subscribe(user("alice"), sink)
	if !fanout.IsHolder(user("alice")) {
		t.Error("a user this process holds")
	}
	fanout.UnSubscribe(user("alice"), sink)
	if fanout.IsHolder(user("alice")) {
		t.Error("a user whose socket went away")
	}
}

func TestFanoutConcurrent(t *testing.T) {
	fanout := NewFanout[string, user]()
	var wg sync.WaitGroup

	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			sink := &collector{}
			for range 100 {
				fanout.Subscribe(user("alice"), sink)
				fanout.IsHolder(user("alice"))
				fanout.PublishTo(user("alice"), "hello")
				fanout.UnSubscribe(user("alice"), sink)
			}
		}()
	}
	wg.Wait()
}
