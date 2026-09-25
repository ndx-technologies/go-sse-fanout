package ssefanout

import (
	"bytes"
	"encoding/json/jsontext"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func receiveMessage(t *testing.T, broker *redis.PubSub) *redis.Message {
	t.Helper()

	select {
	case m := <-broker.Channel():
		return m
	case <-time.After(3 * time.Second):
		t.Fatal("nothing crossed the wire")
		return nil
	}
}

func receiveEnvelope(t *testing.T, listen <-chan Envelope) Envelope {
	t.Helper()

	select {
	case envelope := <-listen:
		return envelope
	case <-time.After(3 * time.Second):
		t.Fatal("the envelope did not arrive")
		return Envelope{}
	}
}

func assertNothing(t *testing.T, listen <-chan Envelope) {
	t.Helper()

	select {
	case envelope := <-listen:
		t.Error("the envelope arrived", envelope)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestRedisPubSubListenBeforeStart(t *testing.T) {
	pubsub := NewRedisPubSub(nil)

	if _, err := pubsub.Listen(t.Context(), "channel"); err == nil {
		t.Error("a pub/sub that was not started followed a channel")
	}
	if err := pubsub.Unlisten(t.Context(), "channel", nil); err != nil {
		t.Error("unlistening a channel nobody follows", err)
	}
}

func TestRedisPubSubDeliverReachesEverySinkOfItsChannel(t *testing.T) {
	pubsub := NewRedisPubSub(nil)
	first, second := make(chan Envelope, 1), make(chan Envelope, 1)
	other := make(chan Envelope, 1)
	pubsub.subscribers["channel"] = []chan Envelope{first, second}
	pubsub.subscribers["other"] = []chan Envelope{other}

	pubsub.deliver("channel", Envelope{User: "alice"})
	pubsub.deliver("nobody", Envelope{User: "alice"})

	for _, listen := range []<-chan Envelope{first, second} {
		if got := receiveEnvelope(t, listen); got.User != "alice" {
			t.Error("got", got, "want alice")
		}
	}
	assertNothing(t, other)
}

func TestRedisPubSubDeliverDropsForASlowSink(t *testing.T) {
	pubsub := NewRedisPubSub(nil)
	slow := make(chan Envelope, 1)
	fast := make(chan Envelope, 10)
	pubsub.subscribers["channel"] = []chan Envelope{slow, fast}

	for range 10 {
		pubsub.deliver("channel", Envelope{User: "alice"})
	}

	if len(slow) != 1 {
		t.Error("the sink nobody reads kept", len(slow), "envelopes, want 1")
	}
	if len(fast) != 10 {
		t.Error("the sink that keeps up got", len(fast), "envelopes, want 10")
	}
}

func TestRedisPubSubPublishAndListen(t *testing.T) {
	p := newPod(t, "pod-a", newNamespace(t), Config{})
	channel := p.config.WithEvent("message").eventChannel()

	listen, err := p.connection.Listen(p.ctx, channel)
	if err != nil {
		t.Fatal(err)
	}

	want := Envelope{User: "alice", Event: "message", Socket: 0xbeef, Payload: jsontext.Value(`{"hello":"world"}`)}
	if err := p.connection.Publish(p.ctx, channel, want); err != nil {
		t.Fatal(err)
	}

	got := receiveEnvelope(t, listen)
	if got.User != want.User || got.Event != want.Event || got.Socket != want.Socket || !bytes.Equal(got.Payload, want.Payload) {
		t.Error("got", got, "want", want)
	}
}

func TestRedisPubSubKeepsChannelsApart(t *testing.T) {
	p := newPod(t, "pod-a", newNamespace(t), Config{})
	messages := p.config.WithEvent("message").eventChannel()
	tasks := p.config.WithEvent("task").eventChannel()

	messageListen, err := p.connection.Listen(p.ctx, messages)
	if err != nil {
		t.Fatal(err)
	}
	taskListen, err := p.connection.Listen(p.ctx, tasks)
	if err != nil {
		t.Fatal(err)
	}

	if err := p.connection.Publish(p.ctx, tasks, Envelope{User: "alice"}); err != nil {
		t.Fatal(err)
	}

	if got := receiveEnvelope(t, taskListen); got.User != "alice" {
		t.Error("got", got, "want alice")
	}
	assertNothing(t, messageListen)
}

func TestRedisPubSubListenTwiceReachesBoth(t *testing.T) {
	p := newPod(t, "pod-a", newNamespace(t), Config{})
	channel := p.config.WithEvent("message").eventChannel()

	first, err := p.connection.Listen(p.ctx, channel)
	if err != nil {
		t.Fatal(err)
	}
	second, err := p.connection.Listen(p.ctx, channel)
	if err != nil {
		t.Fatal(err)
	}

	if err := p.connection.Publish(p.ctx, channel, Envelope{User: "alice"}); err != nil {
		t.Fatal(err)
	}

	for _, listen := range []<-chan Envelope{first, second} {
		if got := receiveEnvelope(t, listen); got.User != "alice" {
			t.Error("got", got, "want alice")
		}
	}
}

func TestRedisPubSubUnlistenStopsTheDelivery(t *testing.T) {
	p := newPod(t, "pod-a", newNamespace(t), Config{})
	channel := p.config.WithEvent("message").eventChannel()

	listen, err := p.connection.Listen(p.ctx, channel)
	if err != nil {
		t.Fatal(err)
	}

	if err := p.connection.Publish(p.ctx, channel, Envelope{User: "alice"}); err != nil {
		t.Fatal(err)
	}
	receiveEnvelope(t, listen)

	if err := p.connection.Unlisten(p.ctx, channel, listen); err != nil {
		t.Fatal(err)
	}
	if err := p.connection.Publish(p.ctx, channel, Envelope{User: "alice"}); err != nil {
		t.Fatal(err)
	}
	assertNothing(t, listen)
}

// Two sockets of one user follow the same channel while one of them is being taken over: the one
// that ends must not take the subscription away from the one that stays.
func TestRedisPubSubUnlistenKeepsTheOtherListener(t *testing.T) {
	p := newPod(t, "pod-a", newNamespace(t), Config{})
	channel := p.config.WithEvent("message").eventChannel()

	first, err := p.connection.Listen(p.ctx, channel)
	if err != nil {
		t.Fatal(err)
	}
	second, err := p.connection.Listen(p.ctx, channel)
	if err != nil {
		t.Fatal(err)
	}

	if err := p.connection.Unlisten(p.ctx, channel, first); err != nil {
		t.Fatal(err)
	}
	if err := p.connection.Publish(p.ctx, channel, Envelope{User: "alice"}); err != nil {
		t.Fatal(err)
	}

	if got := receiveEnvelope(t, second); got.User != "alice" {
		t.Error("got", got, "want alice")
	}
	assertNothing(t, first)
}

// What cannot be decoded is dropped, and the subscription keeps going: a poison message must not
// end the receive loop for every channel of the process.
func TestRedisPubSubIgnoresWhatItCannotDecode(t *testing.T) {
	p := newPod(t, "pod-a", newNamespace(t), Config{})
	channel := p.config.WithEvent("message").eventChannel()

	listen, err := p.connection.Listen(p.ctx, channel)
	if err != nil {
		t.Fatal(err)
	}

	if err := p.rdb.Publish(p.ctx, channel, "not an envelope").Err(); err != nil {
		t.Fatal(err)
	}
	if err := p.connection.Publish(p.ctx, channel, Envelope{User: "alice"}); err != nil {
		t.Fatal(err)
	}

	if got := receiveEnvelope(t, listen); got.User != "alice" {
		t.Error("got", got, "want alice")
	}
}

func TestRedisPubSubPublishWithoutSubscribers(t *testing.T) {
	p := newPod(t, "pod-a", newNamespace(t), Config{})

	if err := p.connection.Publish(p.ctx, p.config.WithEvent("message").eventChannel(), Envelope{User: "alice"}); err != nil {
		t.Error("publishing where nobody listens", err)
	}
}

// Publish knows nothing about the local sinks: it always goes to Redis, and the sinks of this
// process are fed by the subscription. So the publisher sees its own envelope come back, and a
// second process sees it too.
func TestRedisPubSubPublishGoesThroughRedis(t *testing.T) {
	namespace := newNamespace(t)
	first := newPod(t, "pod-a", namespace, Config{})
	second := newPod(t, "pod-b", namespace, Config{})
	channel := first.config.WithEvent("message").eventChannel()

	here, err := first.connection.Listen(first.ctx, channel)
	if err != nil {
		t.Fatal(err)
	}
	there, err := second.connection.Listen(second.ctx, channel)
	if err != nil {
		t.Fatal(err)
	}

	if err := first.connection.Publish(first.ctx, channel, Envelope{User: "alice"}); err != nil {
		t.Fatal(err)
	}

	if got := receiveEnvelope(t, here); got.User != "alice" {
		t.Error("the publisher did not see its own envelope, got", got)
	}
	if got := receiveEnvelope(t, there); got.User != "alice" {
		t.Error("got", got, "want alice")
	}
}

func TestRedisPubSubWireFormat(t *testing.T) {
	p := newPod(t, "pod-a", newNamespace(t), Config{})
	channel := p.config.WithEvent("message").eventChannel()
	broker := p.broker(t, channel)

	envelope := Envelope{User: "alice", Event: "message", Socket: 0xbeef, Payload: jsontext.Value(`{"hello":"world"}`)}
	if err := p.connection.Publish(p.ctx, channel, envelope); err != nil {
		t.Fatal(err)
	}

	event := receiveMessage(t, broker)
	if got, want := string(event.Payload), `{"user":"alice","event":"message","socket":"beef","payload":{"hello":"world"}}`; got != want {
		t.Error("got", got, "want", want)
	}

	kick := Envelope{User: "alice", Socket: 0x7}
	if err := p.connection.Publish(p.ctx, channel, kick); err != nil {
		t.Fatal(err)
	}

	event = receiveMessage(t, broker)
	if got, want := string(event.Payload), `{"user":"alice","socket":"7"}`; got != want {
		t.Error("got", got, "want", want)
	}
}
