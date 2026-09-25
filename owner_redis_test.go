package ssefanout

import (
	"testing"
	"time"
)

func TestSocketClaimIsZero(t *testing.T) {
	if !(SocketClaim{}).IsZero() {
		t.Error("a claim nobody wrote is not zero")
	}
	if !(SocketClaim{Pod: "pod-a"}).IsZero() {
		t.Error("a claim naming no socket is not zero")
	}
	if (SocketClaim{Pod: "pod-a", Socket: NewSocketID()}).IsZero() {
		t.Error("a claim naming a socket is zero")
	}
}

func TestOwnerClaimAndGet(t *testing.T) {
	p := newPod(t, "pod-a", newNamespace(t), Config{TTL: time.Minute, Renew: time.Second})
	ctx := t.Context()
	socket := NewSocketID()

	if _, err := p.owner.Claim(ctx, "alice", socket); err != nil {
		t.Fatal(err)
	}

	claim, err := p.owner.Get(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if claim.IsZero() {
		t.Fatal("the user is not held after a claim")
	}
	if claim.Pod != "pod-a" || claim.Socket != socket {
		t.Error("got", claim.Pod, claim.Socket, "want pod-a", socket)
	}
	if claim.Refreshed.IsZero() {
		t.Error("the claim carries no refresh time")
	}
}

func TestOwnerGetUnknownUser(t *testing.T) {
	p := newPod(t, "pod-a", newNamespace(t), Config{TTL: time.Minute, Renew: time.Second})

	claim, err := p.owner.Get(t.Context(), "nobody")
	if err != nil {
		t.Fatal(err)
	}
	if !claim.IsZero() {
		t.Fatal("a user nobody claimed is held", claim)
	}
}

func TestOwnerClaimDisplacesTheHolder(t *testing.T) {
	p := newPod(t, "pod-a", newNamespace(t), Config{TTL: time.Minute, Renew: time.Second})
	ctx := t.Context()
	first := NewSocketID()
	second := NewSocketID()

	if _, err := p.owner.Claim(ctx, "alice", first); err != nil {
		t.Fatal(err)
	}
	displaced, err := p.owner.Claim(ctx, "alice", second)
	if err != nil {
		t.Fatal(err)
	}
	if displaced.Socket != first {
		t.Error("got the displaced socket", displaced.Socket, "want", first)
	}

	claim, err := p.owner.Get(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if claim.IsZero() {
		t.Fatal("the user is not held after a take-over")
	}
	if claim.Socket != second {
		t.Error("got", claim.Socket, "want the new socket", second)
	}
}

func TestOwnerRefreshKeepsTheLease(t *testing.T) {
	p := newPod(t, "pod-a", newNamespace(t), Config{TTL: 300 * time.Millisecond, Renew: time.Second})
	ctx := t.Context()
	socket := NewSocketID()

	if _, err := p.owner.Claim(ctx, "alice", socket); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(600 * time.Millisecond)
	for time.Now().Before(deadline) {
		held, err := p.owner.Refresh(ctx, "alice", socket)
		if err != nil {
			t.Fatal(err)
		}
		if !held {
			t.Fatal("the holder lost its own lease")
		}
		time.Sleep(100 * time.Millisecond)
	}

	claim, err := p.owner.Get(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if claim.IsZero() {
		t.Fatal("the lease expired while it was being refreshed")
	}
}

func TestOwnerRefreshByAnotherSocketFails(t *testing.T) {
	p := newPod(t, "pod-a", newNamespace(t), Config{TTL: time.Minute, Renew: time.Second})
	ctx := t.Context()

	if _, err := p.owner.Claim(ctx, "alice", NewSocketID()); err != nil {
		t.Fatal(err)
	}
	held, err := p.owner.Refresh(ctx, "alice", NewSocketID())
	if err != nil {
		t.Fatal(err)
	}
	if held {
		t.Error("a socket that does not hold the user refreshed the lease")
	}
}

func TestOwnerRefreshAfterExpiryFails(t *testing.T) {
	p := newPod(t, "pod-a", newNamespace(t), Config{TTL: 100 * time.Millisecond, Renew: time.Second})
	ctx := t.Context()
	socket := NewSocketID()

	if _, err := p.owner.Claim(ctx, "alice", socket); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)

	held, err := p.owner.Refresh(ctx, "alice", socket)
	if err != nil {
		t.Fatal(err)
	}
	if held {
		t.Error("an expired lease was refreshed")
	}
}

func TestOwnerReleaseDropsOnlyItsOwnLease(t *testing.T) {
	p := newPod(t, "pod-a", newNamespace(t), Config{TTL: time.Minute, Renew: time.Second})
	ctx := t.Context()
	held := NewSocketID()

	if _, err := p.owner.Claim(ctx, "alice", held); err != nil {
		t.Fatal(err)
	}
	if err := p.owner.Release(ctx, "alice", NewSocketID()); err != nil {
		t.Fatal(err)
	}
	claim, err := p.owner.Get(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if claim.IsZero() {
		t.Fatal("another socket released the lease")
	}

	if err := p.owner.Release(ctx, "alice", held); err != nil {
		t.Fatal(err)
	}
	claim, err = p.owner.Get(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if !claim.IsZero() {
		t.Fatal("the lease outlived its release", claim)
	}
}

func TestOwnerReleaseAfterTakeOverDoesNothing(t *testing.T) {
	p := newPod(t, "pod-a", newNamespace(t), Config{TTL: time.Minute, Renew: time.Second})
	ctx := t.Context()
	displaced := NewSocketID()
	taken := NewSocketID()

	if _, err := p.owner.Claim(ctx, "alice", displaced); err != nil {
		t.Fatal(err)
	}
	if _, err := p.owner.Claim(ctx, "alice", taken); err != nil {
		t.Fatal(err)
	}

	if err := p.owner.Release(ctx, "alice", displaced); err != nil {
		t.Fatal(err)
	}
	claim, err := p.owner.Get(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if claim.IsZero() {
		t.Fatal("the displaced socket released the lease of the new one")
	}
	if claim.Socket != taken {
		t.Error("got", claim.Socket, "want", taken)
	}
}

// The refresh is a compare-and-swap: the socket that lost the user cannot take the claim back,
// which a read, compare and write in the client could not prevent.
func TestOwnerRefreshAfterTakeOverDoesNotStealBack(t *testing.T) {
	p := newPod(t, "pod-a", newNamespace(t), Config{TTL: time.Minute, Renew: time.Second})
	ctx := t.Context()
	first := NewSocketID()
	second := NewSocketID()

	if _, err := p.owner.Claim(ctx, "alice", first); err != nil {
		t.Fatal(err)
	}
	if _, err := p.owner.Claim(ctx, "alice", second); err != nil {
		t.Fatal(err)
	}

	held, err := p.owner.Refresh(ctx, "alice", first)
	if err != nil {
		t.Fatal(err)
	}
	if held {
		t.Error("the displaced socket refreshed the claim of the new one")
	}

	claim, err := p.owner.Get(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if claim.Socket != second {
		t.Error("got", claim.Socket, "want the socket of the new holder", second)
	}
}

func TestOwnerScriptsOnNobodyHolding(t *testing.T) {
	p := newPod(t, "pod-a", newNamespace(t), Config{TTL: time.Minute, Renew: time.Second})
	ctx := t.Context()

	if held, err := p.owner.Refresh(ctx, "alice", NewSocketID()); err != nil || held {
		t.Fatal("refreshing a claim nobody holds", err, held)
	}
	if err := p.owner.Release(ctx, "alice", NewSocketID()); err != nil {
		t.Fatal("releasing a claim nobody holds", err)
	}
}

func TestOwnerKeyIsTheUserChannel(t *testing.T) {
	p := newPod(t, "pod-a", newNamespace(t), Config{TTL: time.Minute, Renew: time.Second})

	if got, want := p.owner.Key("alice"), p.config.keyUserChannel("alice"); got != want {
		t.Error("got", got, "want", want)
	}
}
