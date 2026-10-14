package server

import (
	"testing"
	"time"

	"github.com/devlikeapro/gows/gows"
	gowsLog "github.com/devlikeapro/gows/log"
	"github.com/google/uuid"
)

func newTestServer() *Server {
	return &Server{
		log:       gowsLog.Stdout("test", "ERROR", false),
		listeners: map[string]map[uuid.UUID]*eventListener{},
	}
}

func isPoisoned(listener *eventListener) bool {
	select {
	case <-listener.poisoned:
		return true
	default:
		return false
	}
}

func TestSendEventToAllListenersPoisonsStuckListener(t *testing.T) {
	s := newTestServer()
	listener := s.addListener("session", uuid.New())

	// Nobody reads the events, so we fill the buffer and then drop
	for i := 0; i < eventListenerBuffer+maxConsecutiveDrops; i++ {
		s.SendEventToAllListeners("session", i)
	}

	if !isPoisoned(listener) {
		t.Fatal("expected the listener to be poisoned after maxConsecutiveDrops")
	}
}

func TestSendEventToAllListenersResetsDropsOnDelivery(t *testing.T) {
	s := newTestServer()
	listener := s.addListener("session", uuid.New())

	// Fill the buffer, then alternate: drop a few, read one, drop a few...
	for i := 0; i < eventListenerBuffer; i++ {
		s.SendEventToAllListeners("session", i)
	}
	for round := 0; round < 10; round++ {
		for i := 0; i < maxConsecutiveDrops-1; i++ {
			s.SendEventToAllListeners("session", i)
		}
		<-listener.events
		s.SendEventToAllListeners("session", "delivered")
	}

	if isPoisoned(listener) {
		t.Fatal("a listener that keeps up must not be poisoned")
	}
}

func TestSendEventToAllListenersDoesNotBlockOnPoisonedListener(t *testing.T) {
	s := newTestServer()
	listener := s.addListener("session", uuid.New())
	listener.poison()

	done := make(chan struct{})
	go func() {
		s.SendEventToAllListeners("session", "event")
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("SendEventToAllListeners blocked on a poisoned listener")
	}
}

// Pending deleted chats go to a listener that just connected, without ever
// blocking on a full listener.
func TestPushPendingEventsNeverBlocks(t *testing.T) {
	listener := &eventListener{events: make(chan interface{}, 2), poisoned: make(chan struct{})}
	events := []*gows.ChatDeletedEvent{{ID: "a"}, {ID: "b"}, {ID: "c"}}

	pushPendingEvents(listener, events)

	if len(listener.events) != 2 {
		t.Fatalf("expected the first two events, got %d", len(listener.events))
	}
	if first := (<-listener.events).(*gows.ChatDeletedEvent); first.ID != "a" {
		t.Fatalf("expected the oldest first, got %v", first.ID)
	}
}
