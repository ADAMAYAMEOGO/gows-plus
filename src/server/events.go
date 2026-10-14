package server

import (
	"encoding/json"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/devlikeapro/gows/proto"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// eventListenerBuffer is how many events we keep for a listener that doesn't read them fast enough.
const eventListenerBuffer = 1000

// maxConsecutiveDrops is how many events in a row we may drop before we consider
// the listener dead and ask the client to reconnect.
const maxConsecutiveDrops = eventListenerBuffer

type eventListener struct {
	events chan interface{}
	// closed once the listener is considered dead, StreamEvents watches it
	poisoned   chan struct{}
	poisonOnce sync.Once
	drops      atomic.Int64
}

func newEventListener() *eventListener {
	return &eventListener{
		events:   make(chan interface{}, eventListenerBuffer),
		poisoned: make(chan struct{}),
	}
}

// poison marks the listener as dead. It touches neither the listeners map nor
// listenersLock, so it's safe to call while holding listenersLock.RLock().
func (l *eventListener) poison() {
	l.poisonOnce.Do(func() {
		close(l.poisoned)
	})
}

func (s *Server) safeMarshal(v interface{}) (result string) {
	defer func() {
		if err := recover(); err != nil {
			// Print log error and ignore
			s.log.Errorf("Panic happened when marshaling data: %v", err)
			result = ""
		}
	}()
	data, err := json.Marshal(v)
	if err != nil {
		s.log.Errorf("Error when marshaling data: %v", err)
		return ""
	}
	result = string(data)
	return result
}

func (s *Server) StreamEvents(req *__.StreamEventsRequest, stream grpc.ServerStreamingServer[__.EventJson]) error {
	sessionName := req.GetSession().GetId()
	streamId := uuid.New()
	listener := s.addListener(sessionName, streamId)
	defer s.removeListener(sessionName, streamId)
	s.replayChatDeletions(sessionName, listener)
	exclude := make(map[string]struct{}, len(req.GetExclude()))
	for _, eventType := range req.GetExclude() {
		exclude[eventType] = struct{}{}
	}
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-listener.poisoned:
			s.log.Warnf("Listener %s is too slow, session %s, asking the client to reconnect", streamId.String(), sessionName)
			return status.Errorf(
				codes.ResourceExhausted,
				"listener is too slow for session %s, reconnect required",
				sessionName,
			)
		case event, ok := <-listener.events:
			if !ok {
				return nil
			}
			if event == nil {
				continue
			}
			// Remove * at the start if it's *
			eventType := reflect.TypeOf(event).String()
			eventType = strings.TrimPrefix(eventType, "*")
			if _, ok := exclude[eventType]; ok {
				continue
			}

			jsonString := s.safeMarshal(event)
			if jsonString == "" {
				continue
			}

			data := __.EventJson{
				Session: sessionName,
				Event:   eventType,
				Data:    jsonString,
			}
			err := stream.Send(&data)
			if err != nil {
				return err
			}
		}
	}
}

func (s *Server) SendEventToAllListeners(session string, event interface{}) {
	s.listenersLock.RLock()
	defer s.listenersLock.RUnlock()

	sessionListeners := s.listeners[session]
	for id, listener := range sessionListeners {
		// Drop the event if the listener buffer is full to avoid blocking and goroutine leaks.
		select {
		case listener.events <- event:
			listener.drops.Store(0)
		default:
			drops := listener.drops.Add(1)
			s.log.Warnf("Dropping event for slow listener %s, session %s, dropped in a row: %d", id.String(), session, drops)
			if drops >= maxConsecutiveDrops {
				// The listener never catches up - kill the stream so the client
				// reconnects with an empty buffer instead of losing events forever.
				// We hold the read lock here, so we can't call removeListener:
				// StreamEvents removes itself once it sees the poisoned channel.
				listener.poison()
			}
		}
	}
}

func (s *Server) addListener(session string, id uuid.UUID) *eventListener {
	s.listenersLock.Lock()
	defer s.listenersLock.Unlock()

	listener := newEventListener()
	sessionListeners, ok := s.listeners[session]
	if !ok {
		sessionListeners = map[uuid.UUID]*eventListener{}
		s.listeners[session] = sessionListeners
	}
	sessionListeners[id] = listener
	return listener
}

func (s *Server) removeListener(session string, id uuid.UUID) {
	s.listenersLock.Lock()
	defer s.listenersLock.Unlock()
	listener, ok := s.listeners[session][id]
	if !ok {
		return
	}
	delete(s.listeners[session], id)
	// if it's the last listener, remove the session
	if len(s.listeners[session]) == 0 {
		delete(s.listeners, session)
	}
	close(listener.events)
}
