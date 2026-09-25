package ssefanout

import (
	"context"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// user stands in for the key of a socket: it only has to be comparable and printable.
type user string

func (s user) String() string { return string(s) }

func parseUser(text string) (user, error) { return user(text), nil }

// recorder is a response: it keeps what a socket writes into it.
type recorder struct {
	mu   sync.Mutex
	data []byte
}

func (r *recorder) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.data = append(r.data, p...)
	return len(p), nil
}

func (r *recorder) String() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return string(r.data)
}

// errWriter is a response whose client is gone: nothing can be written into it.
type errWriter struct{}

func (errWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

// failAfter is a response that takes n writes and then reports the client gone.
type failAfter struct {
	recorder
	n int
}

func (f *failAfter) Write(p []byte) (int, error) {
	if f.n <= 0 {
		return 0, io.ErrClosedPipe
	}
	f.n--
	return f.recorder.Write(p)
}

// flushWriter is a response that can flush, like the one net/http hands a handler.
type flushWriter struct {
	recorder
	flushed atomic.Int64
}

func (f *flushWriter) Flush() { f.flushed.Add(1) }

// pod is one process: its own Redis connection, its own events, and the sockets it holds.
type pod struct {
	config     Config
	connection *RedisPubSub
	owner      OwnerRedis
	rdb        *redis.Client

	ctx     context.Context
	cancel  context.CancelFunc
	sockets []*Socket[user]
}

func newPod(t *testing.T, name, namespace string, config Config) *pod {
	t.Helper()
	if testing.Short() {
		t.Skip("network; redis instance;")
	}

	u := os.Getenv("REDIS_URL")
	if u == "" {
		u = "redis://localhost:6379"
	}
	options, err := redis.ParseURL(u)
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(options)

	config.Pod = name
	config.Channel = namespace
	config = config.WithDefaults()

	ctx, cancel := context.WithCancel(context.Background())
	p := &pod{
		config: config,
		owner:  OwnerRedis{Config: config, DB: rdb},
		rdb:    rdb,
		ctx:    ctx,
		cancel: cancel,
	}

	connection := NewRedisPubSub(rdb)
	if err := connection.Start(ctx); err != nil {
		t.Fatal(err)
	}
	p.connection = connection

	// the sockets end first, then the client goes: nothing is still using it
	t.Cleanup(func() {
		cancel()
		for _, socket := range p.sockets {
			<-socket.Done()
		}
		_ = rdb.Close()
	})
	return p
}

// newNamespace is the channel namespace two processes of one test share.
func newNamespace(_ *testing.T) string {
	return "test:sse:" + strconv.FormatInt(time.Now().UnixNano(), 36)
}

// eventHub is one event of this process, followed.
func eventHub[T any](t *testing.T, p *pod, name string) *Hub[user, T] {
	t.Helper()

	hub := NewHub[user, T](p.config.WithEvent(name), p.connection, p.owner, parseUser)
	if err := hub.Start(p.ctx); err != nil {
		t.Fatal(err)
	}
	return hub
}

// socketOf is one client of this process: a response, the user it holds, and the events it carries.
func socketOf(t *testing.T, p *pod, who user, out io.Writer, hubs ...*Hub[user, string]) *Socket[user] {
	t.Helper()

	socket := NewSocket(p.config, p.connection, p.owner, who, out)
	for _, hub := range hubs {
		hub.Join(who, socket)
	}
	if err := socket.Subscribe(p.ctx); err != nil {
		t.Fatal(err)
	}
	p.sockets = append(p.sockets, socket)
	return socket
}

// broker is a raw Redis subscriber: it sees what actually crosses the wire.
func (p *pod) broker(t *testing.T, channels ...string) *redis.PubSub {
	t.Helper()

	broker := p.rdb.Subscribe(t.Context(), channels...)
	t.Cleanup(func() { _ = broker.Close() })
	if _, err := broker.Receive(t.Context()); err != nil {
		t.Fatal(err)
	}
	return broker
}

func waitFor(t *testing.T, r *recorder, want string) {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if got := r.String(); got == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("got %q, want %q", r.String(), want)
}

func assertQuiet(t *testing.T, broker *redis.PubSub) {
	t.Helper()

	select {
	case m := <-broker.Channel():
		t.Error("something crossed the wire", m.Channel, string(m.Payload))
	case <-time.After(300 * time.Millisecond):
	}
}

func assertEnds(t *testing.T, s *Socket[user]) {
	t.Helper()

	select {
	case <-s.Done():
	case <-time.After(3 * time.Second):
		t.Fatal("the socket did not end")
	}
}

func assertAlive(t *testing.T, s *Socket[user]) {
	t.Helper()

	select {
	case <-s.Done():
		t.Fatal("the socket ended")
	case <-time.After(300 * time.Millisecond):
	}
}

// The process that holds the socket writes into the response by itself: no copy of the event
// crosses the wire.
func TestPublishWritesIntoTheLocalSocket(t *testing.T) {
	p := newPod(t, "pod-a", newNamespace(t), Config{TTL: time.Minute, Renew: time.Second})
	ctx := t.Context()

	messages := eventHub[string](t, p, "message")
	out := &recorder{}
	sock := socketOf(t, p, "alice", out, messages)

	broker := p.broker(t, p.config.WithEvent("message").eventChannel(), p.config.keyUserChannel("alice"))

	if err := messages.Publish(ctx, []user{"alice"}, "hello"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, out, "event: message\ndata: \"hello\"\n\n")
	assertQuiet(t, broker)
	assertAlive(t, sock)
}

// Another process holds the socket: the event goes to that user's channel, where only the holder
// listens, and the event channel stays quiet.
func TestPublishRoutesToTheOwningProcess(t *testing.T) {
	namespace := newNamespace(t)
	holder := newPod(t, "pod-a", namespace, Config{TTL: time.Minute, Renew: time.Second})
	publisher := newPod(t, "pod-b", namespace, Config{TTL: time.Minute, Renew: time.Second})
	ctx := t.Context()

	out := &recorder{}
	socketOf(t, holder, "alice", out, eventHub[string](t, holder, "message"))
	messages := eventHub[string](t, publisher, "message")

	quiet := publisher.broker(t, publisher.config.WithEvent("message").eventChannel())

	if err := messages.Publish(ctx, []user{"alice"}, "hello"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, out, "event: message\ndata: \"hello\"\n\n")
	assertQuiet(t, quiet)
}

// The claim named a process that is gone, so the publisher has no address to use and goes to the
// event channel: the process holding the socket writes it anyway.
func TestPublishFallsBackToTheEventChannel(t *testing.T) {
	namespace := newNamespace(t)
	holder := newPod(t, "pod-a", namespace, Config{TTL: time.Minute, Renew: time.Second})
	publisher := newPod(t, "pod-b", namespace, Config{TTL: time.Minute, Renew: time.Second})
	ctx := t.Context()

	out := &recorder{}
	socketOf(t, holder, "alice", out, eventHub[string](t, holder, "message"))
	messages := eventHub[string](t, publisher, "message")

	if err := holder.rdb.Del(ctx, holder.owner.Key("alice")).Err(); err != nil {
		t.Fatal(err)
	}
	eventChannel := holder.config.WithEvent("message").eventChannel()
	broker := holder.broker(t, eventChannel)

	if err := messages.Publish(ctx, []user{"alice"}, "hello"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, out, "event: message\ndata: \"hello\"\n\n")

	select {
	case m := <-broker.Channel():
		if m.Channel != eventChannel {
			t.Error("the fallback went to", m.Channel)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the fallback did not go to the event channel")
	}
}

// One call, two users: the one this process holds is written straight into its response, the one
// another process holds takes the hop. Both receive the event, and neither receives it twice.
func TestPublishWritesOneUserLocallyAndTheOtherThroughRedis(t *testing.T) {
	namespace := newNamespace(t)
	here := newPod(t, "pod-a", namespace, Config{TTL: time.Minute, Renew: time.Second})
	there := newPod(t, "pod-b", namespace, Config{TTL: time.Minute, Renew: time.Second})
	ctx := t.Context()

	messages := eventHub[string](t, here, "message")
	hereOut := &recorder{}
	socketOf(t, here, "alice", hereOut, messages)

	thereOut := &recorder{}
	socketOf(t, there, "bob", thereOut, eventHub[string](t, there, "message"))

	if err := messages.Publish(ctx, []user{"alice", "bob"}, "hello"); err != nil {
		t.Fatal(err)
	}

	frame := "event: message\ndata: \"hello\"\n\n"
	waitFor(t, hereOut, frame)
	waitFor(t, thereOut, frame)
}

// Nobody is known to hold the user, so the publisher has no address to use: it writes into the
// response it can see and broadcasts as well. The response gets the event twice, because the
// delivery is at least once rather than silently lost.
func TestPublishWithoutTheClaimWritesTwice(t *testing.T) {
	p := newPod(t, "pod-a", newNamespace(t), Config{TTL: time.Minute, Renew: time.Second})
	ctx := t.Context()

	messages := eventHub[string](t, p, "message")
	out := &recorder{}
	socketOf(t, p, "alice", out, messages)

	if err := p.rdb.Del(ctx, p.owner.Key("alice")).Err(); err != nil {
		t.Fatal(err)
	}
	if err := messages.Publish(ctx, []user{"alice"}, "hello"); err != nil {
		t.Fatal(err)
	}

	frame := "event: message\ndata: \"hello\"\n\n"
	waitFor(t, out, frame+frame)

	time.Sleep(300 * time.Millisecond)
	if got := out.String(); got != frame+frame {
		t.Error("got", got, "want", frame+frame)
	}
}

// A client that connects again without closing the first stream: the new socket takes the user
// over, the displaced one is told to stop, and only the new one receives.
func TestTakeOverEndsTheDisplacedSocket(t *testing.T) {
	namespace := newNamespace(t)
	first := newPod(t, "pod-a", namespace, Config{TTL: time.Minute, Renew: time.Second})
	second := newPod(t, "pod-b", namespace, Config{TTL: time.Minute, Renew: time.Second})
	ctx := t.Context()

	displaced := socketOf(t, first, "alice", &recorder{}, eventHub[string](t, first, "message"))

	takenOut := &recorder{}
	taken := eventHub[string](t, second, "message")
	socketOf(t, second, "alice", takenOut, taken)

	assertEnds(t, displaced)

	if err := taken.Publish(ctx, []user{"alice"}, "hello"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, takenOut, "event: message\ndata: \"hello\"\n\n")
}

// The response is one, so one kick ends every event riding on it, not just the event whose claim
// was taken over.
func TestKickEndsEveryEventOfTheSocket(t *testing.T) {
	namespace := newNamespace(t)
	first := newPod(t, "pod-a", namespace, Config{TTL: time.Minute, Renew: time.Second})
	second := newPod(t, "pod-b", namespace, Config{TTL: time.Minute, Renew: time.Second})

	messages := eventHub[string](t, first, "message")
	tasks := eventHub[string](t, first, "task")
	sock := socketOf(t, first, "alice", &recorder{}, messages, tasks)

	socketOf(t, second, "alice", &recorder{}, eventHub[string](t, second, "message"))

	assertEnds(t, sock)
	if messages.Sockets.IsHolder("alice") || tasks.Sockets.IsHolder("alice") {
		t.Error("an event outlived the socket")
	}
}

// A silent response is closed by the proxies in front of it, so the tick that refreshes the claim
// writes a heartbeat into the response as well.
func TestSocketWritesTheHeartbeat(t *testing.T) {
	p := newPod(t, "pod-a", newNamespace(t), Config{TTL: time.Minute, Renew: 50 * time.Millisecond})

	out := &recorder{}
	socketOf(t, p, "alice", out, eventHub[string](t, p, "message"))

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(out.String(), ": heartbeat\n\n") {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Error("no heartbeat was written into the response")
}

// The claim is a lease, so the holder keeps it alive: it survives its own TTL while the response
// is open, and it goes away when the socket ends.
func TestRefreshKeepsTheClaimAlive(t *testing.T) {
	p := newPod(t, "pod-a", newNamespace(t), Config{TTL: 300 * time.Millisecond, Renew: 50 * time.Millisecond})

	sock := socketOf(t, p, "alice", &recorder{}, eventHub[string](t, p, "message"))
	time.Sleep(600 * time.Millisecond)

	claim, err := p.owner.Get(p.ctx, "alice")
	if err != nil || claim.IsZero() {
		t.Fatal("the claim expired while the socket was alive", err, claim)
	}
	assertAlive(t, sock)

	p.cancel()
	assertEnds(t, sock)

	claim, err = p.owner.Get(context.Background(), "alice")
	if err != nil || !claim.IsZero() {
		t.Fatal("the claim outlived the socket", err, claim)
	}
}

// The kick is best effort, so the lease is what decides: a socket that lost the claim without
// being kicked learns it on its next refresh and ends itself, without touching the new claim.
func TestRefreshEndsTheSocketThatLostTheClaim(t *testing.T) {
	p := newPod(t, "pod-a", newNamespace(t), Config{TTL: time.Minute, Renew: 50 * time.Millisecond})
	ctx := t.Context()

	messages := eventHub[string](t, p, "message")
	sock := socketOf(t, p, "alice", &recorder{}, messages)

	if _, err := p.owner.Claim(ctx, "alice", NewSocketID()); err != nil {
		t.Fatal(err)
	}

	assertEnds(t, sock)
	if messages.Sockets.IsHolder("alice") {
		t.Error("the event outlived the socket")
	}
	claim, err := p.owner.Get(ctx, "alice")
	if err != nil || claim.IsZero() {
		t.Error("the socket released the claim of the one that took the user over", err, claim)
	}
}

// The client went away without closing the stream: the first write fails, the socket ends by
// itself, and the claim goes with it, so the next connection does not wait for the lease to expire.
func TestSocketEndsWhenTheClientIsGone(t *testing.T) {
	p := newPod(t, "pod-a", newNamespace(t), Config{TTL: time.Minute, Renew: 50 * time.Millisecond})
	ctx := t.Context()

	messages := eventHub[string](t, p, "message")
	sock := socketOf(t, p, "alice", errWriter{}, messages)

	if err := messages.Publish(ctx, []user{"alice"}, "hello"); err != nil {
		t.Fatal(err)
	}

	assertEnds(t, sock)
	claim, err := p.owner.Get(ctx, "alice")
	if err != nil || !claim.IsZero() {
		t.Error("the claim outlived the client", err, claim)
	}
}

func TestSocketDropsFramesWhenTheClientStalls(t *testing.T) {
	socket := &Socket[user]{frames: make(chan frame, 1), done: make(chan struct{})}
	encode := func(io.Writer) error { return nil }

	if err := socket.write("message", encode); err != nil {
		t.Fatal(err)
	}
	if err := socket.write("message", encode); err != nil {
		t.Error("a full queue returned an error", err)
	}
	if len(socket.frames) != 1 {
		t.Error("the queue holds", len(socket.frames), "frames, want 1")
	}

	close(socket.done)
	if err := socket.write("message", encode); err != nil {
		t.Error("writing to an ended socket returned an error", err)
	}
}

// The user channel carries every event of a user, so an envelope for one this socket does not
// carry is dropped rather than streamed.
func TestSocketDropsAnEventItDoesNotCarry(t *testing.T) {
	p := newPod(t, "pod-a", newNamespace(t), Config{TTL: time.Minute, Renew: time.Second})
	ctx := t.Context()

	out := &recorder{}
	sock := socketOf(t, p, "alice", out, eventHub[string](t, p, "message"))

	receipt := p.config.WithEvent("receipt")
	if err := p.connection.Publish(ctx, receipt.keyUserChannel("alice"), Envelope{User: "alice", Event: "receipt"}); err != nil {
		t.Fatal(err)
	}

	time.Sleep(300 * time.Millisecond)
	if got := out.String(); got != "" {
		t.Error("the response got an event the socket does not carry", got)
	}
	assertAlive(t, sock)
}

// An envelope that cannot be decoded is dropped by the event channel, and the process keeps
// serving: the good event published after it still arrives.
func TestHubDropsAnEnvelopeItCannotDecode(t *testing.T) {
	p := newPod(t, "pod-a", newNamespace(t), Config{TTL: time.Minute, Renew: time.Second})
	ctx := t.Context()

	messages := eventHub[string](t, p, "message")
	out := &recorder{}
	sock := socketOf(t, p, "alice", out, messages)

	if err := p.rdb.Del(ctx, p.owner.Key("alice")).Err(); err != nil {
		t.Fatal(err)
	}
	broken := `{"user":"alice","event":"message","payload":{"a":1}}`
	if err := p.rdb.Publish(ctx, p.config.WithEvent("message").eventChannel(), broken).Err(); err != nil {
		t.Fatal(err)
	}
	if err := messages.Publish(ctx, []user{"alice"}, "hello"); err != nil {
		t.Fatal(err)
	}

	waitFor(t, out, "event: message\ndata: \"hello\"\n\n")
	assertAlive(t, sock)
}

func TestSocketDropsAPayloadItCannotDecode(t *testing.T) {
	p := newPod(t, "pod-a", newNamespace(t), Config{TTL: time.Minute, Renew: time.Second})
	ctx := t.Context()

	out := &recorder{}
	sock := socketOf(t, p, "alice", out, eventHub[string](t, p, "message"))

	broken := `{"user":"alice","event":"message","payload":{"a":1}}`
	if err := p.rdb.Publish(ctx, p.config.keyUserChannel("alice"), broken).Err(); err != nil {
		t.Fatal(err)
	}

	time.Sleep(300 * time.Millisecond)
	if got := out.String(); got != "" {
		t.Error("the response got an event that cannot be decoded", got)
	}
	assertAlive(t, sock)
}

// The response stops taking writes part way through a frame: whether the value or the trailing
// separator is the write that fails, the socket ends and stops holding the user.
func TestSocketEndsWhenTheResponseStopsAcceptingWrites(t *testing.T) {
	p := newPod(t, "pod-a", newNamespace(t), Config{TTL: time.Minute, Renew: time.Second})
	ctx := t.Context()

	messages := eventHub[string](t, p, "message")
	alice := socketOf(t, p, "alice", &failAfter{n: 1}, messages)
	bob := socketOf(t, p, "bob", &failAfter{n: 2}, messages)

	if err := messages.Publish(ctx, []user{"alice", "bob"}, "hello"); err != nil {
		t.Fatal(err)
	}

	assertEnds(t, alice)
	assertEnds(t, bob)
}

func TestSocketEndsWhenTheHeartbeatCannotBeWritten(t *testing.T) {
	p := newPod(t, "pod-a", newNamespace(t), Config{TTL: time.Minute, Renew: 50 * time.Millisecond})

	sock := socketOf(t, p, "alice", errWriter{}, eventHub[string](t, p, "message"))

	assertEnds(t, sock)
}

func TestSocketFlushesWhenTheResponseCan(t *testing.T) {
	p := newPod(t, "pod-a", newNamespace(t), Config{TTL: time.Minute, Renew: 50 * time.Millisecond})
	ctx := t.Context()

	messages := eventHub[string](t, p, "message")
	out := &flushWriter{}
	socketOf(t, p, "alice", out, messages)

	if err := messages.Publish(ctx, []user{"alice"}, "hello"); err != nil {
		t.Fatal(err)
	}

	waitFor(t, &out.recorder, "event: message\ndata: \"hello\"\n\n")

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && !strings.Contains(out.recorder.String(), ": heartbeat\n\n") {
		time.Sleep(10 * time.Millisecond)
	}
	if got := out.flushed.Load(); got < 2 {
		t.Error("got", got, "flushes, want the frame and the heartbeat")
	}
}

func TestConfigDefaults(t *testing.T) {
	config := Config{}.WithDefaults()
	if config.Channel != "sse" || config.TTL != time.Minute || config.Renew != 20*time.Second {
		t.Error("got", config, "want the defaults")
	}
	if got := config.eventChannel(); got != "sse" {
		t.Error("got", got, "want the namespace when no event is set")
	}
}

func TestSocketIDUnmarshalTextRejectsGarbage(t *testing.T) {
	var id SocketID
	if err := id.UnmarshalText([]byte("not hex")); err == nil {
		t.Error("garbage was accepted as a socket id")
	}
}

// A client that connects again without closing the first stream, and both connections land on the
// same pod: the new socket takes the claim over and kicks the old one, and the new one keeps
// receiving the routed events of the user it now holds.
func TestTakeOverOnTheSamePod(t *testing.T) {
	namespace := newNamespace(t)
	holder := newPod(t, "pod-a", namespace, Config{TTL: time.Minute, Renew: time.Second})
	publisher := newPod(t, "pod-b", namespace, Config{TTL: time.Minute, Renew: time.Second})
	ctx := t.Context()

	messages := eventHub[string](t, holder, "message")
	displaced := socketOf(t, holder, "alice", &recorder{}, messages)
	out := &recorder{}
	taken := socketOf(t, holder, "alice", out, messages)

	assertEnds(t, displaced)

	published := eventHub[string](t, publisher, "message")
	if err := published.Publish(ctx, []user{"alice"}, "hello"); err != nil {
		t.Fatal(err)
	}

	waitFor(t, out, "event: message\ndata: \"hello\"\n\n")
	assertAlive(t, taken)
}

// The process that held the user is gone: its claim is still there, and nobody follows the user
// channel any more. A new connection takes the user over anyway, because claiming is unconditional,
// so routing works again at once instead of waiting for the lease to expire.
func TestNewConnectionTakesOverTheClaimOfADeadProcess(t *testing.T) {
	namespace := newNamespace(t)
	dead := newPod(t, "pod-dead", namespace, Config{TTL: time.Minute, Renew: time.Second})
	live := newPod(t, "pod-live", namespace, Config{TTL: time.Minute, Renew: time.Second})
	ctx := t.Context()

	if _, err := dead.owner.Claim(ctx, "alice", NewSocketID()); err != nil {
		t.Fatal(err)
	}

	out := &recorder{}
	sock := socketOf(t, live, "alice", out, eventHub[string](t, live, "message"))

	claim, err := live.owner.Get(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if claim.Pod != "pod-live" {
		t.Error("got", claim.Pod, "want pod-live")
	}

	messages := eventHub[string](t, dead, "message")
	if err := messages.Publish(ctx, []user{"alice"}, "hello"); err != nil {
		t.Fatal(err)
	}

	waitFor(t, out, "event: message\ndata: \"hello\"\n\n")
	assertAlive(t, sock)
}
