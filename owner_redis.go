package ssefanout

import (
	"context"
	"encoding/json/v2"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

type SocketClaim struct {
	Pod       string    `json:"pod"`
	Socket    SocketID  `json:"socket"`
	Refreshed time.Time `json:"refreshed_at"`
}

func (s SocketClaim) IsZero() bool { return s.Socket.IsZero() }

// OwnerRedis manages the claim of the socket that holds a user. It is a lease: the holder
// refreshes it, so a process that dies stops refreshing and the claim expires, and a publisher
// that finds no claim broadcasts instead.
type OwnerRedis struct {
	Config Config
	DB     *redis.Client
}

var refreshScript = redis.NewScript(`
local v = redis.call('GET', KEYS[1])
if not v then return 0 end
local e = cjson.decode(v)
if e.pod ~= ARGV[1] or e.socket ~= ARGV[2] then return 0 end
redis.call('SET', KEYS[1], ARGV[3], 'PX', ARGV[4])
return 1
`)

var releaseScript = redis.NewScript(`
local v = redis.call('GET', KEYS[1])
if not v then return 0 end
local e = cjson.decode(v)
if e.pod ~= ARGV[1] or e.socket ~= ARGV[2] then return 0 end
redis.call('DEL', KEYS[1])
return 1
`)

func (s OwnerRedis) Key(key string) string { return s.Config.keyUserChannel(key) }

// Claim takes the claim from whoever holds it and returns what was displaced.
func (s OwnerRedis) Claim(ctx context.Context, key string, socket SocketID) (SocketClaim, error) {
	displaced, err := s.Get(ctx, key)
	if err != nil {
		return SocketClaim{}, err
	}

	payload, err := json.Marshal(SocketClaim{Pod: s.Config.Pod, Socket: socket, Refreshed: time.Now()})
	if err != nil {
		return displaced, err
	}

	return displaced, s.DB.Set(ctx, s.Key(key), payload, s.Config.TTL).Err()
}

func (s OwnerRedis) Get(ctx context.Context, key string) (SocketClaim, error) {
	raw, err := s.DB.Get(ctx, s.Key(key)).Bytes()
	if errors.Is(err, redis.Nil) {
		return SocketClaim{}, nil
	}
	if err != nil {
		return SocketClaim{}, err
	}

	var claim SocketClaim
	if err := json.Unmarshal(raw, &claim); err != nil {
		return SocketClaim{}, err
	}
	return claim, nil
}

// Refresh extends the claim, and reports false when this socket no longer holds it.
func (s OwnerRedis) Refresh(ctx context.Context, key string, socket SocketID) (bool, error) {
	payload, err := json.Marshal(SocketClaim{Pod: s.Config.Pod, Socket: socket, Refreshed: time.Now()})
	if err != nil {
		return false, err
	}

	held, err := refreshScript.Run(
		ctx,
		s.DB,
		[]string{s.Key(key)},
		s.Config.Pod,
		socket.String(),
		payload,
		s.Config.TTL.Milliseconds(),
	).Int()
	if err != nil {
		return false, err
	}
	return held == 1, nil
}

// Release drops the claim, unless a newer socket took it over.
func (s OwnerRedis) Release(ctx context.Context, key string, socket SocketID) error {
	_, err := releaseScript.Run(ctx, s.DB, []string{s.Key(key)}, s.Config.Pod, socket.String()).Int()
	return err
}
