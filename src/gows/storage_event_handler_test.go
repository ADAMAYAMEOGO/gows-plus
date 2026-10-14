package gows

import (
	"testing"

	gowsLog "github.com/devlikeapro/gows/log"
	"github.com/devlikeapro/gows/storage"
	"github.com/devlikeapro/gows/storage/noop"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

// spyMessageStorage records the messages we asked to delete.
type spyMessageStorage struct {
	*noop.MessageStorage
	deleted []types.MessageID
}

func (s *spyMessageStorage) DeleteMessage(id types.MessageID) error {
	s.deleted = append(s.deleted, id)
	return nil
}

// spyLabelStorage records the labels we upserted.
type spyLabelStorage struct {
	*noop.LabelStorage
	upserted []*storage.Label
}

func (s *spyLabelStorage) UpsertLabel(label *storage.Label) error {
	s.upserted = append(s.upserted, label)
	return nil
}

func newTestHandler() (*StorageEventHandler, *spyMessageStorage, *spyLabelStorage) {
	messages := &spyMessageStorage{MessageStorage: noop.NewMessageStorage()}
	labels := &spyLabelStorage{LabelStorage: noop.NewLabelStorage()}
	st := &StorageEventHandler{
		log: gowsLog.Stdout("test", "ERROR", false),
		storage: &storage.Storage{
			Messages:          messages,
			Labels:            labels,
			LabelAssociations: noop.NewLabelAssociationStorage(),
		},
	}
	return st, messages, labels
}

func conversationMessage(text string) *events.Message {
	return &events.Message{
		Info:    types.MessageInfo{ID: "id-1"},
		Message: &waE2E.Message{Conversation: &text},
	}
}

func TestHandleMessageEventNilMessage(t *testing.T) {
	st, messages, _ := newTestHandler()

	// events.Message.Message is nil for messages we could not decrypt
	st.handleMessageEvent(&events.Message{Info: types.MessageInfo{ID: "id-1"}})

	if len(messages.deleted) != 0 {
		t.Fatalf("expected no deletes, got %v", messages.deleted)
	}
}

// ProtocolMessage_REVOKE is the zero value of the enum, so a message without a
// protocol message must never be mistaken for a revoke.
func TestHandleMessageEventPlainMessageIsNotRevoke(t *testing.T) {
	st, messages, _ := newTestHandler()

	st.handleMessageEvent(conversationMessage("hello"))

	if len(messages.deleted) != 0 {
		t.Fatalf("a plain message must not delete anything, got %v", messages.deleted)
	}
}

func TestHandleMessageEventRevokeWithoutKey(t *testing.T) {
	st, messages, _ := newTestHandler()

	revoke := waE2E.ProtocolMessage_REVOKE
	event := &events.Message{
		Info: types.MessageInfo{ID: "id-1"},
		Message: &waE2E.Message{
			ProtocolMessage: &waE2E.ProtocolMessage{Type: &revoke},
		},
	}
	st.handleMessageEvent(event)

	if len(messages.deleted) != 0 {
		t.Fatalf("a revoke without a key must not delete anything, got %v", messages.deleted)
	}
}

func TestHandleMessageEventRevoke(t *testing.T) {
	st, messages, _ := newTestHandler()

	revoke := waE2E.ProtocolMessage_REVOKE
	id := "revoked-id"
	event := &events.Message{
		Info: types.MessageInfo{ID: "id-1"},
		Message: &waE2E.Message{
			ProtocolMessage: &waE2E.ProtocolMessage{
				Type: &revoke,
				Key:  &waCommon.MessageKey{ID: &id},
			},
		},
	}
	st.handleMessageEvent(event)

	if len(messages.deleted) != 1 || messages.deleted[0] != id {
		t.Fatalf("expected to delete %v, got %v", id, messages.deleted)
	}
}

// A protocol message with no type used to dereference a nil *ProtocolMessage_Type.
func TestExtractEphemeralSettingsFromProtocolMessageWithoutType(t *testing.T) {
	setting := ExtractEphemeralSettingsFromProtocolMessage(
		types.MessageInfo{},
		&waE2E.ProtocolMessage{},
	)
	if setting != nil {
		t.Fatalf("expected no ephemeral setting, got %v", setting)
	}
}

// LabelEditAction has optional Name/Color, both used to be dereferenced blindly.
func TestHandleLabelEditWithoutNameAndColor(t *testing.T) {
	st, _, labels := newTestHandler()

	st.handleLabelEdit(&events.LabelEdit{
		LabelID: "label-1",
		Action:  &waSyncAction.LabelEditAction{},
	})

	if len(labels.upserted) != 1 {
		t.Fatalf("expected the label to be upserted, got %v", labels.upserted)
	}
	if labels.upserted[0].Name != "" || labels.upserted[0].Color != 0 {
		t.Fatalf("expected empty name and zero color, got %v", labels.upserted[0])
	}
}

func TestHandleLabelEditDeleted(t *testing.T) {
	st, _, labels := newTestHandler()

	deleted := true
	st.handleLabelEdit(&events.LabelEdit{
		LabelID: "label-1",
		Action:  &waSyncAction.LabelEditAction{Deleted: &deleted},
	})

	if len(labels.upserted) != 0 {
		t.Fatalf("a deleted label must not be upserted, got %v", labels.upserted)
	}
}

func TestHandleHistorySyncWithoutData(t *testing.T) {
	st, _, _ := newTestHandler()

	// Must not dereference a nil event.Data
	st.handleHistorySync(&events.HistorySync{})
}
