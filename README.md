# go-sse-fanout

Server-Sent Events fanout across processes, over Redis Pub/Sub.

A client's connection stream lands on whichever process the load balancer picked.
This makes it reachable from every other one.

- Local first. The direct user channel claim is the shortcut, full broadcast is the guarantee.
- One SSE connection per user. SSE connection ownership with healthcheck.
- No copy. Marshalling JSON into the response wire directly.

```go
func main() {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

	config := ssefanout.Config{Channel: "sse", Pod: os.Getenv("POD_NAME")}.WithDefaults()

	pubsub := ssefanout.NewRedisPubSub(rdb)
	if err := pubsub.Start(ctx); err != nil {
		log.Fatal(err)
	}
	owner := ssefanout.OwnerRedis{Config: config, DB: rdb}

	messages := ssefanout.NewHub[UserID, Message](config.WithEvent("message"), pubsub, owner, ParseUser)
	tasks := ssefanout.NewHub[UserID, Task](config.WithEvent("task"), pubsub, owner, ParseUser)

	events := []event{messages, tasks}
	for _, e := range events {
		if err := e.Start(ctx); err != nil {
			log.Fatal(err)
		}
	}

	http.Handle("/events", SSEHandler{Config: config, PubSub: pubsub, Owner: owner, Events: events})
	log.Fatal(http.ListenAndServe(":8080", nil))
}

type SSEHandler struct {
	Config ssefanout.Config
	PubSub ssefanout.PubSub
	Owner  ssefanout.SocketClaimDB
	Events []event
}

func (s SSEHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	user, ok := UserFromRequest(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if _, ok := w.(http.Flusher); !ok {
		http.Error(w, "sse is not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	socket := ssefanout.NewSocket(s.Config, s.PubSub, s.Owner, user, w)
	for _, e := range s.Events {
		e.Join(user, socket)
	}
	if err := socket.Subscribe(r.Context()); err != nil {
		http.Error(w, "cannot stream", http.StatusInternalServerError)
		return
	}
	<-socket.Done()
}
```

Publish from anywhere. A service that holds no HTTP SSE sockets can still deliver:

```go
if err := messages.Publish(ctx, []UserID{user}, message); err != nil {
	return err
}
```
