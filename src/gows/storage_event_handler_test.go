package gows

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	gowsLog "github.com/devlikeapro/gows/log"
	"github.com/devlikeapro/gows/storage"
	"github.com/devlikeapro/gows/storage/noop"
	"github.com/devlikeapro/gows/storage/sqlstorage"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

// spyMessageStorage records the messages and chats we asked to delete.
type spyMessageStorage struct {
	*noop.MessageStorage
	deleted      []types.MessageID
	deletedChats []deletedChat
	// chatMessages are the ids returned (and forgotten) when deleting the messages of a chat
	chatMessages map[types.JID][]types.MessageID
	// failures are how many times deleting the messages of a chat fails first
	failures map[types.JID]int
	lock     sync.Mutex
}

type deletedChat struct {
	jid    types.JID
	before time.Time
}

func (s *spyMessageStorage) DeleteMessage(id types.MessageID) error {
	s.deleted = append(s.deleted, id)
	return nil
}

func (s *spyMessageStorage) DeleteChatMessages(jid types.JID, deleteBefore time.Time) ([]types.MessageID, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.deletedChats = append(s.deletedChats, deletedChat{jid: jid, before: deleteBefore})
	if s.failures[jid] != 0 {
		s.failures[jid]--
		return nil, errors.New("database is locked")
	}
	ids := s.chatMessages[jid]
	delete(s.chatMessages, jid)
	return ids, nil
}

// spyChatEphemeralSettingStorage records the chat ephemeral settings we asked to delete.
type spyChatEphemeralSettingStorage struct {
	storage.ChatEphemeralSettingStorage
	deleted []deletedChat
	err     error
}

func (s *spyChatEphemeralSettingStorage) DeleteChatEphemeralSetting(jid types.JID, deleteBefore time.Time) error {
	s.deleted = append(s.deleted, deletedChat{jid: jid, before: deleteBefore})
	return s.err
}

// spyLabelStorage records the labels we upserted and deleted.
type spyLabelStorage struct {
	*noop.LabelStorage
	upserted []*storage.Label
	deleted  []string
}

func (s *spyLabelStorage) UpsertLabel(label *storage.Label) error {
	s.upserted = append(s.upserted, label)
	return nil
}

func (s *spyLabelStorage) DeleteLabel(id string) error {
	s.deleted = append(s.deleted, id)
	return nil
}

// spyLabelAssociationStorage records the association calls in order.
type spyLabelAssociationStorage struct {
	*noop.LabelAssociationStorage
	ops []string
}

func (s *spyLabelAssociationStorage) AddAssociation(jid types.JID, labelID string) error {
	s.ops = append(s.ops, "add "+labelID+" "+jid.String())
	return nil
}

func (s *spyLabelAssociationStorage) RemoveAssociation(jid types.JID, labelID string) error {
	s.ops = append(s.ops, "remove "+labelID+" "+jid.String())
	return nil
}

func (s *spyLabelAssociationStorage) RemoveAssociationsByLabelID(labelID string) error {
	s.ops = append(s.ops, "removeByLabel "+labelID)
	return nil
}

func newTestHandler() (*StorageEventHandler, *spyMessageStorage, *spyLabelStorage, *spyLabelAssociationStorage) {
	messages := &spyMessageStorage{MessageStorage: noop.NewMessageStorage()}
	labels := &spyLabelStorage{LabelStorage: noop.NewLabelStorage()}
	associations := &spyLabelAssociationStorage{LabelAssociationStorage: noop.NewLabelAssociationStorage()}
	st := &StorageEventHandler{
		log: gowsLog.Stdout("test", "ERROR", false),
		storage: &storage.Storage{
			Messages:          messages,
			Labels:            labels,
			LabelAssociations: associations,
		},
	}
	return st, messages, labels, associations
}

func labelChatEvent(labelID string, jid types.JID, labeled bool) *events.LabelAssociationChat {
	return &events.LabelAssociationChat{
		JID:     jid,
		LabelID: labelID,
		Action:  &waSyncAction.LabelAssociationAction{Labeled: &labeled},
	}
}

func conversationMessage(text string) *events.Message {
	return &events.Message{
		Info:    types.MessageInfo{ID: "id-1"},
		Message: &waE2E.Message{Conversation: &text},
	}
}

func TestHandleMessageEventNilMessage(t *testing.T) {
	st, messages, _, _ := newTestHandler()

	// events.Message.Message is nil for messages we could not decrypt
	st.handleMessageEvent(&events.Message{Info: types.MessageInfo{ID: "id-1"}})

	if len(messages.deleted) != 0 {
		t.Fatalf("expected no deletes, got %v", messages.deleted)
	}
}

// ProtocolMessage_REVOKE is the zero value of the enum, so a message without a
// protocol message must never be mistaken for a revoke.
func TestHandleMessageEventPlainMessageIsNotRevoke(t *testing.T) {
	st, messages, _, _ := newTestHandler()

	st.handleMessageEvent(conversationMessage("hello"))

	if len(messages.deleted) != 0 {
		t.Fatalf("a plain message must not delete anything, got %v", messages.deleted)
	}
}

func TestHandleMessageEventRevokeWithoutKey(t *testing.T) {
	st, messages, _, _ := newTestHandler()

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
	st, messages, _, _ := newTestHandler()

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

// fastDeleteChatRetries shortens the waits between the attempts of a cleanup
// step for the duration of a test.
func fastDeleteChatRetries(t *testing.T) {
	t.Helper()
	delay := deleteChatRetryDelay
	deleteChatRetryDelay = time.Millisecond
	t.Cleanup(func() { deleteChatRetryDelay = delay })
}

// fakeLIDStore knows one LID / phone number pair.
type fakeLIDStore struct {
	store.LIDStore
	lid, pn types.JID
	err     error
}

func (s *fakeLIDStore) GetPNForLID(ctx context.Context, lid types.JID) (types.JID, error) {
	if s.err != nil || lid != s.lid {
		return types.EmptyJID, s.err
	}
	return s.pn, nil
}

func (s *fakeLIDStore) GetLIDForPN(ctx context.Context, pn types.JID) (types.JID, error) {
	if s.err != nil || pn != s.pn {
		return types.EmptyJID, s.err
	}
	return s.lid, nil
}

var (
	deleteChatPN  = types.NewJID("12345", types.DefaultUserServer)
	deleteChatLID = types.NewJID("67890", types.HiddenUserServer)
)

func newDeleteChatTestHandler() (*StorageEventHandler, *spyMessageStorage, *spyChatEphemeralSettingStorage, *[]interface{}) {
	st, messages, _, _ := newTestHandler()
	ephemeral := &spyChatEphemeralSettingStorage{}
	st.storage.ChatEphemeralSetting = ephemeral
	st.lids = &fakeLIDStore{lid: deleteChatLID, pn: deleteChatPN}
	emitted := make([]interface{}, 0)
	st.emit = func(event interface{}) { emitted = append(emitted, event) }
	return st, messages, ephemeral, &emitted
}

func deleteChatAction(lastMessageTimestamp int64) *waSyncAction.DeleteChatAction {
	return &waSyncAction.DeleteChatAction{
		MessageRange: &waSyncAction.SyncActionMessageRange{
			LastMessageTimestamp: proto.Int64(lastMessageTimestamp),
		},
	}
}

// WhatsApp keeps the messages sent after the message range of the deletion.
func TestHandleDeleteChatUpToTheMessageRange(t *testing.T) {
	st, messages, ephemeral, emitted := newDeleteChatTestHandler()
	group := types.NewJID("120363000000000000", types.GroupServer)
	messages.chatMessages = map[types.JID][]types.MessageID{group: {"a", "b"}}
	deletedAt := time.UnixMilli(1760000100000)

	st.processEvent(&events.DeleteChat{
		JID:         group,
		Timestamp:   deletedAt,
		Action:      deleteChatAction(1760000000),
		DeleteMedia: true,
	})

	expected := []deletedChat{{jid: group, before: time.Unix(1760000001, 0)}}
	if !reflect.DeepEqual(messages.deletedChats, expected) {
		t.Fatalf("expected to delete chat messages %v, got %v", expected, messages.deletedChats)
	}
	if !reflect.DeepEqual(ephemeral.deleted, expected) {
		t.Fatalf("expected to delete chat ephemeral setting %v, got %v", expected, ephemeral.deleted)
	}
	want := &ChatDeletedEvent{
		JID:           group,
		Timestamp:     deletedAt,
		DeletedBefore: time.Unix(1760000001, 0),
		DeleteMedia:   true,
		MessageIDs:    []types.MessageID{"a", "b"},
	}
	if len(*emitted) != 1 || !reflect.DeepEqual((*emitted)[0], want) {
		t.Fatalf("expected to emit %+v, got %+v", want, *emitted)
	}
}

func TestHandleDeleteChatWithoutRangeUsesTheDeletionTime(t *testing.T) {
	st, messages, _, _ := newDeleteChatTestHandler()
	group := types.NewJID("120363000000000000", types.GroupServer)
	deletedAt := time.UnixMilli(1760000100000)

	st.processEvent(&events.DeleteChat{JID: group, Timestamp: deletedAt})

	expected := []deletedChat{{jid: group, before: deletedAt}}
	if !reflect.DeepEqual(messages.deletedChats, expected) {
		t.Fatalf("expected to delete chat messages %v, got %v", expected, messages.deletedChats)
	}
}

// A direct chat is stored under its LID and its phone number, both copies go.
func TestHandleDeleteChatDeletesBothAddresses(t *testing.T) {
	for _, tc := range []struct {
		name     string
		jid, alt types.JID
	}{
		{"phone number", deleteChatPN, deleteChatLID},
		{"LID", deleteChatLID, deleteChatPN},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, messages, ephemeral, emitted := newDeleteChatTestHandler()
			messages.chatMessages = map[types.JID][]types.MessageID{
				tc.jid: {"from-jid"},
				tc.alt: {"from-alt"},
			}
			before := time.Unix(1760000001, 0)

			st.processEvent(&events.DeleteChat{
				JID:          tc.jid,
				Timestamp:    time.UnixMilli(1760000100000),
				Action:       deleteChatAction(1760000000),
				FromFullSync: true,
			})

			expected := []deletedChat{{jid: tc.jid, before: before}, {jid: tc.alt, before: before}}
			if !reflect.DeepEqual(messages.deletedChats, expected) {
				t.Fatalf("expected to delete chat messages %v, got %v", expected, messages.deletedChats)
			}
			if !reflect.DeepEqual(ephemeral.deleted, expected) {
				t.Fatalf("expected to delete chat ephemeral settings %v, got %v", expected, ephemeral.deleted)
			}
			event := (*emitted)[0].(*ChatDeletedEvent)
			if event.AltJID != tc.alt || !event.FromFullSync {
				t.Fatalf("expected the other address %v from a full sync, got %+v", tc.alt, event)
			}
			if !reflect.DeepEqual(event.MessageIDs, []types.MessageID{"from-jid", "from-alt"}) {
				t.Fatalf("expected the ids of both copies, got %v", event.MessageIDs)
			}
		})
	}
}

func TestHandleDeleteChatWithoutKnownOtherAddress(t *testing.T) {
	for _, tc := range []struct {
		name string
		lids store.LIDStore
	}{
		{"no pair", &fakeLIDStore{}},
		{"lookup error", &fakeLIDStore{lid: deleteChatLID, pn: deleteChatPN, err: errors.New("database")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, messages, _, emitted := newDeleteChatTestHandler()
			st.lids = tc.lids

			st.processEvent(&events.DeleteChat{JID: deleteChatPN, Timestamp: time.UnixMilli(1760000100000)})

			if len(messages.deletedChats) != 1 || messages.deletedChats[0].jid != deleteChatPN {
				t.Fatalf("expected to delete the phone number chat only, got %v", messages.deletedChats)
			}
			event := (*emitted)[0].(*ChatDeletedEvent)
			if !event.AltJID.IsEmpty() || event.MessageIDs == nil {
				t.Fatalf("expected no other address and an empty id list, got %+v", event)
			}
		})
	}
}

func TestDeleteChatBeforeUsesTheLatestRangeTimestamp(t *testing.T) {
	event := &events.DeleteChat{
		Timestamp: time.UnixMilli(1760000100000),
		Action: &waSyncAction.DeleteChatAction{
			MessageRange: &waSyncAction.SyncActionMessageRange{
				LastMessageTimestamp:       proto.Int64(1760000000),
				LastSystemMessageTimestamp: proto.Int64(1760000010),
				Messages: []*waSyncAction.SyncActionMessage{
					{Timestamp: proto.Int64(1760000005)},
				},
			},
		},
	}

	if got := deleteChatBefore(event, time.Unix(1760000200, 0)); !got.Equal(time.Unix(1760000011, 0)) {
		t.Fatalf("expected the second after the system message, got %v", got)
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
	st, _, labels, _ := newTestHandler()

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
	st, _, labels, associations := newTestHandler()

	deleted := true
	st.handleLabelEdit(&events.LabelEdit{
		LabelID: "label-1",
		Action:  &waSyncAction.LabelEditAction{Deleted: &deleted},
	})

	if len(labels.upserted) != 0 {
		t.Fatalf("a deleted label must not be upserted, got %v", labels.upserted)
	}
	// No FK cascade in the DB anymore - the associations go first, then the label
	if len(associations.ops) != 1 || associations.ops[0] != "removeByLabel label-1" {
		t.Fatalf("expected the label associations to be removed, got %v", associations.ops)
	}
	if len(labels.deleted) != 1 || labels.deleted[0] != "label-1" {
		t.Fatalf("expected the label to be deleted, got %v", labels.deleted)
	}
}

// Label events run one by one in handleEvent, so the last change per label/chat wins (#2290).
func TestHandleEventLabelAssociationKeepsOrder(t *testing.T) {
	st, _, _, associations := newTestHandler()
	jid := types.NewJID("100000000000001", types.HiddenUserServer)

	st.handleEvent(labelChatEvent("label-1", jid, true))
	st.handleEvent(labelChatEvent("label-1", jid, false))
	st.handleEvent(labelChatEvent("label-1", jid, true))
	st.handleEvent(labelChatEvent("label-1", jid, false))

	expected := []string{
		"add label-1 " + jid.String(),
		"remove label-1 " + jid.String(),
		"add label-1 " + jid.String(),
		"remove label-1 " + jid.String(),
	}
	if len(associations.ops) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, associations.ops)
	}
	for i := range expected {
		if associations.ops[i] != expected[i] {
			t.Fatalf("expected %v, got %v", expected, associations.ops)
		}
	}
}

// The label can arrive after the association in a sync - store the association anyway, no waiting.
func TestHandleLabelAssociationChatWithoutLabel(t *testing.T) {
	st, _, _, associations := newTestHandler()
	jid := types.NewJID("100000000000001", types.HiddenUserServer)

	st.handleLabelAssociationChat(labelChatEvent("label-404", jid, true))

	if len(associations.ops) != 1 || associations.ops[0] != "add label-404 "+jid.String() {
		t.Fatalf("expected the association to be added, got %v", associations.ops)
	}
}

func TestHandleHistorySyncWithoutData(t *testing.T) {
	st, _, _, _ := newTestHandler()

	// Must not dereference a nil event.Data
	st.handleHistorySync(&events.HistorySync{})
}

// ownDeviceGoWS is a session logged in as ownPN / ownLID, enough for validLIDPNPair.
func ownDeviceGoWS(ownPN, ownLID types.JID) *GoWS {
	return &GoWS{
		Client:  &whatsmeow.Client{Store: &store.Device{ID: &ownPN, LID: ownLID}},
		Context: context.Background(),
	}
}

func TestAltChatJIDBothWays(t *testing.T) {
	lids := &fakeLIDStore{lid: deleteChatLID, pn: deleteChatPN}

	alt, err := altChatJID(context.Background(), lids, nil, nil, deleteChatPN)
	if err != nil || alt != deleteChatLID {
		t.Fatalf("expected the LID of the phone number, got %v (%v)", alt, err)
	}
	lidDevice := types.JID{User: deleteChatLID.User, Device: 3, Server: types.HiddenUserServer}
	alt, err = altChatJID(context.Background(), lids, nil, nil, lidDevice)
	if err != nil || alt != deleteChatPN {
		t.Fatalf("expected the phone number of the LID, got %v (%v)", alt, err)
	}
	group := types.NewJID("120363000000000000", types.GroupServer)
	alt, err = altChatJID(context.Background(), lids, nil, nil, group)
	if err != nil || !alt.IsEmpty() {
		t.Fatalf("expected no other address for a group, got %v (%v)", alt, err)
	}
	_, err = altChatJID(context.Background(), &fakeLIDStore{err: errors.New("database")}, nil, nil, deleteChatPN)
	if err == nil {
		t.Fatalf("expected the lookup error")
	}
}

// A stored pair mapping a foreign LID to the own phone number is ignored, as for message alt JIDs.
func TestAltChatJIDIgnoresAForeignLIDOnTheOwnNumber(t *testing.T) {
	ownPN := deleteChatPN
	ownLID := types.NewJID("99999", types.HiddenUserServer)
	gows := ownDeviceGoWS(ownPN, ownLID)

	suspicious := &fakeLIDStore{lid: deleteChatLID, pn: ownPN}
	alt, err := altChatJID(context.Background(), suspicious, gows.validLIDPNPair, nil, ownPN)
	if err != nil || !alt.IsEmpty() {
		t.Fatalf("expected the suspicious pair to be ignored, got %v (%v)", alt, err)
	}
	own := &fakeLIDStore{lid: ownLID, pn: ownPN}
	alt, err = altChatJID(context.Background(), own, gows.validLIDPNPair, nil, ownPN)
	if err != nil || alt != ownLID {
		t.Fatalf("expected the own LID, got %v (%v)", alt, err)
	}
}

// The local cleanup uses the same guard: a suspicious pair only clears the chat itself.
func TestHandleDeleteChatIgnoresASuspiciousPair(t *testing.T) {
	st, messages, _, emitted := newDeleteChatTestHandler()
	ownLID := types.NewJID("99999", types.HiddenUserServer)
	st.gows = ownDeviceGoWS(deleteChatPN, ownLID)
	st.lids = &fakeLIDStore{lid: deleteChatLID, pn: deleteChatPN}

	st.processEvent(&events.DeleteChat{JID: deleteChatPN, Timestamp: time.UnixMilli(1760000100000)})

	if len(messages.deletedChats) != 1 || messages.deletedChats[0].jid != deleteChatPN {
		t.Fatalf("expected to delete the phone number chat only, got %v", messages.deletedChats)
	}
	if event := (*emitted)[0].(*ChatDeletedEvent); !event.AltJID.IsEmpty() {
		t.Fatalf("expected no other address, got %v", event.AltJID)
	}
}

// The bound never goes past the deletion: a range in milliseconds or far in
// the future would delete the messages sent after it.
func TestDeleteChatBeforeIsCapped(t *testing.T) {
	deletedAt := time.UnixMilli(1760000100123)
	now := time.Unix(1760000200, 0)
	for _, tc := range []struct {
		name      string
		timestamp time.Time
		last      int64
		want      time.Time
	}{
		{"range in milliseconds", deletedAt, 1760000000000, deletedAt},
		{"range a day after the deletion", deletedAt, 1760086500, deletedAt},
		{"range a few seconds after the deletion (clock skew)", deletedAt, 1760000105, time.Unix(1760000106, 0)},
		{"range in the deletion second", deletedAt, 1760000100, time.Unix(1760000101, 0)},
		{"no range", deletedAt, 0, deletedAt},
		{"deletion time in the future, no range", time.Unix(1760090000, 0), 0, now},
		{"deletion time in the future, range in milliseconds", time.Unix(1760090000, 0), 1760000000000, now},
		{"no deletion time, no range", time.Time{}, 0, now},
	} {
		t.Run(tc.name, func(t *testing.T) {
			event := &events.DeleteChat{Timestamp: tc.timestamp}
			if tc.last > 0 {
				event.Action = deleteChatAction(tc.last)
			}

			if got := deleteChatBefore(event, now); !got.Equal(tc.want) {
				t.Fatalf("expected %v, got %v", tc.want, got)
			}
		})
	}
}

// A deletion sent for both addresses comes back as two events: the chat is
// deleted once and emitted once, even when they are handled concurrently.
func TestHandleDeleteChatTwiceForBothAddresses(t *testing.T) {
	st, messages, _, emitted := newDeleteChatTestHandler()
	var lock sync.Mutex
	st.emit = func(event interface{}) {
		lock.Lock()
		defer lock.Unlock()
		*emitted = append(*emitted, event)
	}
	messages.chatMessages = map[types.JID][]types.MessageID{
		deleteChatPN:  {"from-pn"},
		deleteChatLID: {"from-lid"},
	}
	deletedAt := time.UnixMilli(1760000100000)

	var wg sync.WaitGroup
	for _, jid := range []types.JID{deleteChatPN, deleteChatLID} {
		wg.Add(1)
		go func(jid types.JID) {
			defer wg.Done()
			st.processEvent(&events.DeleteChat{JID: jid, Timestamp: deletedAt, Action: deleteChatAction(1760000000)})
		}(jid)
	}
	wg.Wait()

	if len(*emitted) != 1 {
		t.Fatalf("expected one event, got %d: %+v", len(*emitted), *emitted)
	}
	event := (*emitted)[0].(*ChatDeletedEvent)
	if len(event.MessageIDs) != 2 || event.Error != "" {
		t.Fatalf("expected both copies in the event, got %+v", event)
	}

	// Another deletion of the same chat later is a new one
	st.processEvent(&events.DeleteChat{JID: deleteChatPN, Timestamp: deletedAt.Add(time.Minute), Action: deleteChatAction(1760000000)})
	if len(*emitted) != 2 {
		t.Fatalf("expected a later deletion to be emitted, got %d events", len(*emitted))
	}
}

// A step failing for a while is retried; a step failing for good is reported
// in Error, the rest of the cleanup still goes.
func TestHandleDeleteChatRetriesAndReportsFailures(t *testing.T) {
	fastDeleteChatRetries(t)
	t.Run("transient", func(t *testing.T) {
		st, messages, _, emitted := newDeleteChatTestHandler()
		messages.chatMessages = map[types.JID][]types.MessageID{deleteChatPN: {"a"}}
		messages.failures = map[types.JID]int{deleteChatPN: 2}

		st.processEvent(&events.DeleteChat{JID: deleteChatPN, Timestamp: time.UnixMilli(1760000100000)})

		event := (*emitted)[0].(*ChatDeletedEvent)
		if event.Error != "" || !reflect.DeepEqual(event.MessageIDs, []types.MessageID{"a"}) {
			t.Fatalf("expected the retried deletion to go through, got %+v", event)
		}
	})
	t.Run("messages", func(t *testing.T) {
		st, messages, _, emitted := newDeleteChatTestHandler()
		messages.chatMessages = map[types.JID][]types.MessageID{deleteChatLID: {"b"}}
		messages.failures = map[types.JID]int{deleteChatPN: 100}

		st.processEvent(&events.DeleteChat{JID: deleteChatPN, Timestamp: time.UnixMilli(1760000100000)})

		event := (*emitted)[0].(*ChatDeletedEvent)
		if !strings.Contains(event.Error, "messages of 12345@s.whatsapp.net") {
			t.Fatalf("expected the failure in Error, got %q", event.Error)
		}
		if !reflect.DeepEqual(event.MessageIDs, []types.MessageID{"b"}) {
			t.Fatalf("expected the other address to be deleted anyway, got %v", event.MessageIDs)
		}
		if attempts := len(messages.deletedChats); attempts != 5 {
			t.Fatalf("expected 4 attempts for the phone number and 1 for the LID, got %d", attempts)
		}
	})
	t.Run("ephemeral setting", func(t *testing.T) {
		st, _, ephemeral, emitted := newDeleteChatTestHandler()
		ephemeral.err = errors.New("database is locked")

		st.processEvent(&events.DeleteChat{JID: deleteChatPN, Timestamp: time.UnixMilli(1760000100000)})

		event := (*emitted)[0].(*ChatDeletedEvent)
		if !strings.Contains(event.Error, "ephemeral setting of") {
			t.Fatalf("expected the failure in Error, got %q", event.Error)
		}
	})
}

// newDeleteChatTestStorage opens a GOWS database: SQLite, or PostgreSQL when
// GOWS_TEST_PG_DSN is set (its tables are emptied first, run with -p 1).
func newDeleteChatTestStorage(t *testing.T) (*storage.Storage, string) {
	t.Helper()
	dialect, address := "sqlite3", "file:"+filepath.Join(t.TempDir(), "gows.db")+"?_foreign_keys=on"
	if dsn := os.Getenv("GOWS_TEST_PG_DSN"); dsn != "" {
		dialect, address = "postgres", dsn
	}
	container, err := sqlstorage.New(dialect, address, waLog.Noop)
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Close() })
	db, err := sqlx.Open(dialect, address)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	for _, table := range []string{"gows_messages", "gows_chat_ephemeral_setting"} {
		_, err = db.Exec("DELETE FROM " + table)
		require.NoError(t, err)
	}
	return &storage.Storage{
		Messages:             container.NewMessageStorage(),
		ChatEphemeralSetting: container.NewChatEphemeralSettingStorage(),
	}, dialect
}

func putDeleteChatTestMessage(t *testing.T, s *storage.Storage, chat types.JID, id string, ts time.Time, real bool) {
	t.Helper()
	msg := &waE2E.Message{Conversation: proto.String(id)}
	if !real {
		msg = &waE2E.Message{ReactionMessage: &waE2E.ReactionMessage{Text: proto.String("x")}}
	}
	require.NoError(t, s.Messages.UpsertOneMessage(&storage.StoredMessage{
		Message: &events.Message{
			Info:       types.MessageInfo{ID: id, Timestamp: ts, MessageSource: types.MessageSource{Chat: chat, Sender: chat}},
			Message:    msg,
			RawMessage: msg,
		},
		IsReal: real,
	}))
}

// End to end on a real database: both copies up to the range, nothing after
// it, nothing of another chat.
func TestHandleDeleteChatOnARealDatabase(t *testing.T) {
	s, dialect := newDeleteChatTestStorage(t)
	other := types.NewJID("99999", types.DefaultUserServer)
	putDeleteChatTestMessage(t, s, deleteChatPN, "PN-OLD", time.Unix(1760000000, 0), true)
	putDeleteChatTestMessage(t, s, deleteChatLID, "LID-LAST", time.Unix(1760000100, 0), true)
	putDeleteChatTestMessage(t, s, deleteChatLID, "LID-SAME-SECOND", time.Unix(1760000100, 900_000_000), true)
	putDeleteChatTestMessage(t, s, deleteChatLID, "LID-NEXT-SECOND", time.Unix(1760000101, 0), true)
	putDeleteChatTestMessage(t, s, deleteChatPN, "PN-REACTION-LATER", time.Unix(1760000200, 0), false)
	putDeleteChatTestMessage(t, s, other, "OTHER", time.Unix(1760000000, 0), true)
	st := &StorageEventHandler{log: gowsLog.Stdout("test", "ERROR", false), storage: s}
	st.lids = &fakeLIDStore{lid: deleteChatLID, pn: deleteChatPN}
	var emitted []interface{}
	st.emit = func(e interface{}) { emitted = append(emitted, e) }

	st.processEvent(&events.DeleteChat{
		JID:       deleteChatPN,
		Timestamp: time.UnixMilli(1760000300000),
		Action:    deleteChatAction(1760000100),
	})

	event := emitted[0].(*ChatDeletedEvent)
	require.Empty(t, event.Error, dialect)
	require.ElementsMatch(t, []types.MessageID{"PN-OLD", "LID-LAST", "LID-SAME-SECOND"}, event.MessageIDs, dialect)
	require.Equal(t, time.Unix(1760000101, 0), event.DeletedBefore, dialect)
	for _, id := range []types.MessageID{"LID-NEXT-SECOND", "PN-REACTION-LATER", "OTHER"} {
		_, err := s.Messages.GetMessage(id)
		require.NoError(t, err, "%s %s", dialect, id)
	}
	for _, id := range event.MessageIDs {
		_, err := s.Messages.GetMessage(id)
		require.ErrorIs(t, err, storage.ErrNotFound, "%s %s", dialect, id)
	}
}

// getEphemeralSettings falls back to the other address, so the setting goes under both.
func TestHandleDeleteChatClearsTheEphemeralSettingOfBothAddresses(t *testing.T) {
	s, dialect := newDeleteChatTestStorage(t)
	setting := storage.NotEphemeral(deleteChatLID)
	setting.IsEphemeral = true
	setting.Setting = &storage.EphemeralSetting{Timestamp: proto.Int64(1760000000), Expiration: 86400}
	require.NoError(t, s.ChatEphemeralSetting.UpdateChatEphemeralSetting(setting), dialect)
	cli := &GoWS{
		Client: &whatsmeow.Client{
			Store: &store.Device{LIDs: &fakeLIDStore{lid: deleteChatLID, pn: deleteChatPN}},
			Log:   waLog.Noop,
		},
		Context: context.Background(),
		Storage: s,
	}
	_, err := cli.getEphemeralSettings(deleteChatPN)
	require.NoError(t, err, dialect)
	st := &StorageEventHandler{gows: cli, log: gowsLog.Stdout("test", "ERROR", false), storage: s}

	st.processEvent(&events.DeleteChat{JID: deleteChatPN, Timestamp: time.UnixMilli(1760000300000)})

	for _, jid := range []types.JID{deleteChatPN, deleteChatLID} {
		_, err = cli.getEphemeralSettings(jid)
		require.ErrorIs(t, err, storage.ErrNotFound, "%s %s", dialect, jid)
	}
}

// spyChatDeletionStorage keeps the deleted chats in memory.
type spyChatDeletionStorage struct {
	kept []*storage.PendingChatDeletion
	err  error
}

func (s *spyChatDeletionStorage) AddChatDeletion(deletion *storage.PendingChatDeletion) error {
	if s.err != nil {
		return s.err
	}
	s.kept = append(s.kept, deletion)
	return nil
}

func (s *spyChatDeletionStorage) GetChatDeletions() ([]*storage.PendingChatDeletion, error) {
	return s.kept, s.err
}

func (s *spyChatDeletionStorage) DeleteChatDeletion(id string) error {
	for i, deletion := range s.kept {
		if deletion.ID == id {
			s.kept = append(s.kept[:i], s.kept[i+1:]...)
			break
		}
	}
	return nil
}

func (s *spyChatDeletionStorage) DeleteChatDeletionsBefore(before time.Time) (int64, error) {
	return 0, nil
}

func TestChatDeletedNeedsConfirmation(t *testing.T) {
	for _, tc := range []struct {
		name  string
		event ChatDeletedEvent
		want  bool
	}{
		{"deleted on the phone, to notify", ChatDeletedEvent{}, true},
		{"full sync without media", ChatDeletedEvent{FromFullSync: true}, false},
		{"full sync with messages, their files to remove", ChatDeletedEvent{FromFullSync: true, MessageIDs: []types.MessageID{"a"}}, true},
		{"full sync with messages, gallery too", ChatDeletedEvent{FromFullSync: true, DeleteMedia: true, MessageIDs: []types.MessageID{"a"}}, true},
		{"full sync, nothing stored", ChatDeletedEvent{FromFullSync: true, DeleteMedia: true}, false},
		{"failed cleanup, nothing deleted", ChatDeletedEvent{Error: "database is locked"}, false},
		{"failed cleanup with messages deleted", ChatDeletedEvent{Error: "x", MessageIDs: []types.MessageID{"a"}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.event.needsConfirmation(); got != tc.want {
				t.Fatalf("expected %v, got %v", tc.want, got)
			}
		})
	}
}

// A deleted chat is kept until the client confirms it, so it is not lost if
// nobody listens when it is emitted.
func TestHandleDeleteChatKeepsTheDeletionUntilConfirmed(t *testing.T) {
	fastDeleteChatRetries(t)
	st, messages, _, emitted := newDeleteChatTestHandler()
	kept := &spyChatDeletionStorage{}
	st.storage.ChatDeletions = kept
	messages.chatMessages = map[types.JID][]types.MessageID{deleteChatPN: {"a"}}

	st.processEvent(&events.DeleteChat{JID: deleteChatPN, Timestamp: time.UnixMilli(1760000100000), DeleteMedia: true})

	event := (*emitted)[0].(*ChatDeletedEvent)
	if event.ID == "" || len(kept.kept) != 1 || kept.kept[0].ID != event.ID {
		t.Fatalf("expected the emitted deletion to be kept with its id, got %+v / %+v", event, kept.kept)
	}
	stored := &ChatDeletedEvent{}
	require.NoError(t, json.Unmarshal(kept.kept[0].Data, stored))
	if stored.ID != event.ID || !reflect.DeepEqual(stored.MessageIDs, event.MessageIDs) || stored.JID != deleteChatPN {
		t.Fatalf("expected the event itself to be kept, got %+v", stored)
	}

	// A full sync replay without media to remove is not kept
	st.processEvent(&events.DeleteChat{JID: deleteChatLID, Timestamp: time.UnixMilli(1760000200000), FromFullSync: true})
	if last := (*emitted)[1].(*ChatDeletedEvent); last.ID != "" || len(kept.kept) != 1 {
		t.Fatalf("expected a full sync replay not to be kept, got %+v", last)
	}

	// Keeping it failing still emits it
	kept.err = errors.New("database is locked")
	st.processEvent(&events.DeleteChat{JID: deleteChatPN, Timestamp: time.UnixMilli(1760000300000)})
	if len(*emitted) != 3 {
		t.Fatalf("expected the deletion to be emitted anyway, got %d events", len(*emitted))
	}
}

func TestPendingChatDeletionsRoundTrip(t *testing.T) {
	s, _ := newDeleteChatTestStorage(t)
	path := filepath.Join(t.TempDir(), "deletions.db")
	container, err := sqlstorage.New("sqlite3", "file:"+path+"?_foreign_keys=on", waLog.Noop)
	require.NoError(t, err)
	defer container.Close()
	s.ChatDeletions = container.NewChatDeletionStorage()
	st := &StorageEventHandler{log: gowsLog.Stdout("test", "ERROR", false), storage: s}
	st.lids = &fakeLIDStore{lid: deleteChatLID, pn: deleteChatPN}
	st.emit = func(e interface{}) {}
	putDeleteChatTestMessage(t, s, deleteChatLID, "LID-1", time.Unix(1760000000, 0), true)
	st.processEvent(&events.DeleteChat{JID: deleteChatPN, Timestamp: time.UnixMilli(1760000100000), DeleteMedia: true})
	gows := &GoWS{Client: &whatsmeow.Client{Log: waLog.Noop}, Storage: s}

	pending, err := gows.PendingChatDeletions()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.NotEmpty(t, pending[0].ID)
	require.Equal(t, deleteChatPN, pending[0].JID)
	require.Equal(t, deleteChatLID, pending[0].AltJID)
	require.Equal(t, []types.MessageID{"LID-1"}, pending[0].MessageIDs)
	require.True(t, pending[0].DeleteMedia)

	require.NoError(t, gows.ConfirmChatDeletion(pending[0].ID))
	pending, err = gows.PendingChatDeletions()
	require.NoError(t, err)
	require.Empty(t, pending)
}

// A deleted chat of an ignored kind (groups here) is not handled: its messages
// were never stored, and no event is emitted for it.
func TestHandleDeleteChatOfAnIgnoredChat(t *testing.T) {
	st, messages, _, emitted := newDeleteChatTestHandler()
	st.ignoreJids = &IgnoreJidsConfig{Groups: true}
	group := types.NewJID("120363000000000000", types.GroupServer)

	st.processEvent(&events.DeleteChat{JID: group, Timestamp: time.UnixMilli(1760000100000)})

	if len(messages.deletedChats) != 0 || len(*emitted) != 0 {
		t.Fatalf("expected an ignored group to be skipped, got %v / %v", messages.deletedChats, *emitted)
	}

	st.processEvent(&events.DeleteChat{JID: deleteChatPN, Timestamp: time.UnixMilli(1760000100000)})

	if len(*emitted) != 1 {
		t.Fatalf("expected a direct chat to be handled, got %d events", len(*emitted))
	}
}

// putFromMe stores a message sent by this account (from the phone or the API).
func putFromMe(t *testing.T, s *storage.Storage, chat, sender types.JID, id string, ts time.Time) {
	t.Helper()
	msg := &waE2E.Message{Conversation: proto.String(id)}
	require.NoError(t, s.Messages.UpsertOneMessage(&storage.StoredMessage{
		Message: &events.Message{
			Info: types.MessageInfo{
				ID:            id,
				Timestamp:     ts,
				MessageSource: types.MessageSource{Chat: chat, Sender: sender, IsFromMe: true},
			},
			Message:    msg,
			RawMessage: msg,
		},
		IsReal: true,
	}))
}

// Broadcast list messages are stored under the list: deleting the chat with yourself leaves them.
func TestDeletingTheOwnChatKeepsBroadcastListMessages(t *testing.T) {
	s, dialect := newDeleteChatTestStorage(t)
	ownPN := types.NewJID("22670000009", types.DefaultUserServer)
	ownLID := types.NewJID("99999", types.HiddenUserServer)
	list := types.NewJID("1760000000", types.BroadcastServer)
	putFromMe(t, s, ownPN, ownPN, "SELF-PN", time.Unix(1760000000, 0))
	putFromMe(t, s, ownLID, ownLID, "SELF-LID", time.Unix(1760000010, 0))
	putFromMe(t, s, list, ownLID, "LIST-FROM-PHONE", time.Unix(1760000005, 0))
	putFromMe(t, s, list, ownPN, "LIST-FROM-API", time.Unix(1760000006, 0))
	st := &StorageEventHandler{log: gowsLog.Stdout("test", "ERROR", false), storage: s}
	st.gows = ownDeviceGoWS(ownPN, ownLID)
	st.lids = &fakeLIDStore{lid: ownLID, pn: ownPN}
	var emitted []interface{}
	st.emit = func(e interface{}) { emitted = append(emitted, e) }

	st.processEvent(&events.DeleteChat{
		JID:         ownPN,
		Timestamp:   time.UnixMilli(1760000100000),
		DeleteMedia: true,
	})

	event := emitted[0].(*ChatDeletedEvent)
	require.Equal(t, ownLID, event.AltJID, dialect)
	require.ElementsMatch(t, []types.MessageID{"SELF-PN", "SELF-LID"}, event.MessageIDs, dialect)
	for _, id := range []types.MessageID{"LIST-FROM-PHONE", "LIST-FROM-API"} {
		stored, err := s.Messages.GetMessage(id)
		require.NoError(t, err, "%s %s", dialect, id)
		require.Equal(t, list, stored.Info.Chat, dialect)
	}
}

// A broadcast list deleted on the phone (the API refuses it) is cleared under
// the list itself, nothing else.
func TestHandleABroadcastListDeletedOnThePhone(t *testing.T) {
	s, dialect := newDeleteChatTestStorage(t)
	ownPN := types.NewJID("22670000009", types.DefaultUserServer)
	ownLID := types.NewJID("99999", types.HiddenUserServer)
	list := types.NewJID("1760000000", types.BroadcastServer)
	putFromMe(t, s, list, ownLID, "LIST-1", time.Unix(1760000005, 0))
	putFromMe(t, s, list, ownPN, "LIST-2", time.Unix(1760000006, 0))
	putFromMe(t, s, ownLID, ownLID, "SELF-LID", time.Unix(1760000001, 0))
	putDeleteChatTestMessage(t, s, deleteChatPN, "DM", time.Unix(1760000002, 0), true)
	st := &StorageEventHandler{log: gowsLog.Stdout("test", "ERROR", false), storage: s}
	st.gows = ownDeviceGoWS(ownPN, ownLID)
	st.lids = &fakeLIDStore{lid: ownLID, pn: ownPN}
	var emitted []interface{}
	st.emit = func(e interface{}) { emitted = append(emitted, e) }

	st.processEvent(&events.DeleteChat{
		JID:         list,
		Timestamp:   time.UnixMilli(1760000100000),
		Action:      deleteChatAction(1760000006),
		DeleteMedia: true,
	})

	event := emitted[0].(*ChatDeletedEvent)
	require.Equal(t, list, event.JID, dialect)
	require.True(t, event.AltJID.IsEmpty(), dialect)
	require.ElementsMatch(t, []types.MessageID{"LIST-1", "LIST-2"}, event.MessageIDs, dialect)
	for _, id := range []types.MessageID{"SELF-LID", "DM"} {
		_, err := s.Messages.GetMessage(id)
		require.NoError(t, err, "%s %s", dialect, id)
	}

	// With broadcast lists ignored, nothing of the list is stored nor cleared
	ignoring, _, _, ignored := newDeleteChatTestHandler()
	ignoring.ignoreJids = &IgnoreJidsConfig{Broadcast: true}
	ignoring.processEvent(&events.DeleteChat{JID: list, Timestamp: time.UnixMilli(1760000100000)})
	require.Empty(t, *ignored)
}
