# go-sse-fanout

Server-Sent Events fanout across processes over Redis Pub/Sub.

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
	if err := messages.Start(ctx); err != nil {
		log.Fatal(err)
	}

	http.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		user, err := UserFromRequest(r)
		if err != nil {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")

		socket := ssefanout.NewSocket(config, pubsub, owner, user, w)
		messages.Join(user, socket)
		if err := socket.Subscribe(r.Context()); err != nil {
			http.Error(w, "cannot stream", http.StatusInternalServerError)
			return
		}
		<-socket.Done()
	})

	log.Fatal(http.ListenAndServe(":8080", nil))
}
```

Publish from anywhere. A service that holds no HTTP SSE sockets can still deliver:

```go
if err := messages.Publish(ctx, []UserID{user}, message); err != nil {
	return err
}
```
