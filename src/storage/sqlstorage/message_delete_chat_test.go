package sqlstorage

import (
	"testing"
	"time"

	"github.com/devlikeapro/gows/storage"
	"github.com/stretchr/testify/require"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

// newDeleteChatTestStore opens a message store on SQLite, or on PostgreSQL when
// GOWS_TEST_PG_DSN is set.
func newDeleteChatTestStore(t *testing.T) *SqlMessageStore {
	t.Helper()
	return newTestContainer(t, "gows_messages").NewMessageStorage()
}

func putDeleteChatTestMessage(t *testing.T, store *SqlMessageStore, chat types.JID, id string, timestamp time.Time, real bool) {
	t.Helper()
	msg := &waE2E.Message{Conversation: proto.String(id)}
	require.NoError(t, store.UpsertOneMessage(&storage.StoredMessage{
		Message: &events.Message{
			Info: types.MessageInfo{
				ID:            id,
				Timestamp:     timestamp,
				MessageSource: types.MessageSource{Chat: chat, Sender: chat},
			},
			Message:    msg,
			RawMessage: msg,
		},
		IsReal: real,
	}))
}

func TestDeleteChatMessagesReturnsTheDeletedIds(t *testing.T) {
	store := newDeleteChatTestStore(t)
	chat := types.NewJID("12345", types.DefaultUserServer)
	other := types.NewJID("67890", types.HiddenUserServer)
	putDeleteChatTestMessage(t, store, chat, "old", time.Unix(1760000000, 0), true)
	putDeleteChatTestMessage(t, store, chat, "reaction", time.Unix(1760000050, 0), false)
	putDeleteChatTestMessage(t, store, chat, "same-second", time.Unix(1760000100, 500_000_000), true)
	putDeleteChatTestMessage(t, store, chat, "after", time.Unix(1760000101, 0), true)
	putDeleteChatTestMessage(t, store, other, "other-chat", time.Unix(1760000000, 0), true)

	ids, err := store.DeleteChatMessages(chat, time.Unix(1760000101, 0))

	require.NoError(t, err)
	require.ElementsMatch(t, []types.MessageID{"old", "reaction", "same-second"}, ids)
	for _, id := range []types.MessageID{"old", "reaction", "same-second"} {
		_, err = store.GetMessage(id)
		require.ErrorIs(t, err, storage.ErrNotFound, id)
	}
	for _, id := range []types.MessageID{"after", "other-chat"} {
		_, err = store.GetMessage(id)
		require.NoError(t, err, id)
	}

	ids, err = store.DeleteChatMessages(chat, time.Unix(1760000101, 0))

	require.NoError(t, err)
	require.Empty(t, ids)
}

// A message a fraction of a second after the bound stays.
func TestDeleteChatMessagesKeepsAFractionAfterTheBound(t *testing.T) {
	store := newDeleteChatTestStore(t)
	chat := types.NewJID("12345", types.DefaultUserServer)
	putDeleteChatTestMessage(t, store, chat, "whole-before", time.Unix(1760000100, 0), true)
	putDeleteChatTestMessage(t, store, chat, "frac-after-bound", time.Unix(1760000101, 500_000_000), true)
	putDeleteChatTestMessage(t, store, chat, "whole-after-bound", time.Unix(1760000102, 0), true)

	ids, err := store.DeleteChatMessages(chat, time.Unix(1760000101, 0))

	require.NoError(t, err)
	require.Equal(t, []types.MessageID{"whole-before"}, ids)
}
