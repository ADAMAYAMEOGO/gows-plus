package server

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/devlikeapro/gows/storage"
	"github.com/devlikeapro/gows/storage/noop"
	"github.com/devlikeapro/gows/storage/sqlstorage"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

var (
	deleteChatPN       = types.NewJID("22670000000", types.DefaultUserServer)
	deleteChatLID      = types.NewJID("123456789012345", types.HiddenUserServer)
	deleteChatGroupJID = types.NewJID("120363000000000000", types.GroupServer)
	deleteChatNow      = time.Unix(1760000900, 0)
)

func storedDeleteChatMessage(source types.MessageSource, id string, timestamp time.Time) *storage.StoredMessage {
	msg := &waE2E.Message{Conversation: proto.String(id)}
	return &storage.StoredMessage{
		Message: &events.Message{
			Info: types.MessageInfo{
				MessageSource: source,
				ID:            id,
				Timestamp:     timestamp,
			},
			Message:    msg,
			RawMessage: msg,
		},
		IsReal: true,
	}
}

// lastMessagesStorage answers the last message stored under each chat.
type lastMessagesStorage struct {
	*noop.MessageStorage
	last  map[types.JID]*storage.StoredMessage
	err   error
	asked []types.JID
}

func (s *lastMessagesStorage) GetChatMessages(jid types.JID, filters storage.MessageFilter, pagination storage.Pagination, merge bool) ([]*storage.StoredMessage, error) {
	s.asked = append(s.asked, jid)
	if merge {
		return nil, errors.New("the last message of an address must not merge LID and phone number")
	}
	if s.err != nil {
		return nil, s.err
	}
	if msg, ok := s.last[jid]; ok {
		return []*storage.StoredMessage{msg}, nil
	}
	return []*storage.StoredMessage{}, nil
}

type testChatDeletion struct {
	*chatDeletion
	messages   *lastMessagesStorage
	sent       []appstate.PatchInfo
	refetched  int
	requests   []deleteChatRequest
	requestErr error
}

type deleteChatRequest struct {
	jid   types.JID
	at    time.Time
	purge bool
}

func newTestChatDeletion(alt types.JID) *testChatDeletion {
	td := &testChatDeletion{messages: &lastMessagesStorage{last: map[types.JID]*storage.StoredMessage{}}}
	td.chatDeletion = &chatDeletion{
		messages: td.messages,
		altJID: func(ctx context.Context, jid types.JID) (types.JID, error) {
			return alt, nil
		},
		inGroup: func(ctx context.Context, jid types.JID) (bool, error) {
			return false, nil
		},
		send: func(ctx context.Context, patch appstate.PatchInfo) error {
			td.sent = append(td.sent, patch)
			return nil
		},
		refetch: func() { td.refetched++ },
		now:     func() time.Time { return deleteChatNow },
		request: func(jids []types.JID, deletedAt time.Time, purge bool) error {
			for _, jid := range jids {
				td.requests = append(td.requests, deleteChatRequest{jid: jid, at: deletedAt, purge: purge})
			}
			return td.requestErr
		},
		purgeByDefault: true,
	}
	return td
}

// deleteChatMutation is what a test checks in a deleteChat mutation.
type deleteChatMutation struct {
	index    []string
	end      int64
	keyChat  string
	keyID    string
	keyTime  int64
	keyCount int
}

func mutationsOf(t *testing.T, patch appstate.PatchInfo) []deleteChatMutation {
	t.Helper()
	assert.Equal(t, appstate.WAPatchRegularHigh, patch.Type)
	result := make([]deleteChatMutation, 0, len(patch.Mutations))
	for _, mutation := range patch.Mutations {
		assert.Equal(t, int32(6), mutation.Version)
		messageRange := mutation.Value.GetDeleteChatAction().GetMessageRange()
		m := deleteChatMutation{
			index:    mutation.Index,
			end:      messageRange.GetLastMessageTimestamp(),
			keyCount: len(messageRange.GetMessages()),
		}
		if m.keyCount > 0 {
			m.keyChat = messageRange.GetMessages()[0].GetKey().GetRemoteJID()
			m.keyID = messageRange.GetMessages()[0].GetKey().GetID()
			m.keyTime = messageRange.GetMessages()[0].GetTimestamp()
		}
		result = append(result, m)
	}
	return result
}

func TestParseDeleteChatJIDAcceptsChats(t *testing.T) {
	for raw, expected := range map[string]string{
		"12345@s.whatsapp.net":        "12345@s.whatsapp.net",
		"12345:7@s.whatsapp.net":      "12345@s.whatsapp.net",
		"67890@lid":                   "67890@lid",
		"67890:3@lid":                 "67890@lid",
		"120363000000000000@g.us":     "120363000000000000@g.us",
		"22670000000-1500000000@g.us": "22670000000-1500000000@g.us",
	} {
		jid, err := parseDeleteChatJID(raw)

		assert.NoError(t, err, raw)
		assert.Equal(t, expected, jid.String(), raw)
	}
}

// A deletion is a permanent app state mutation: ids that are not exactly a
// direct chat or a group are rejected, never normalized into one.
func TestParseDeleteChatJIDRejectsOtherChats(t *testing.T) {
	for _, raw := range []string{
		"",
		"12345",
		"@s.whatsapp.net",
		"1.2.3@s.whatsapp.net",
		"12345.1:2@s.whatsapp.net",
		"abc@s.whatsapp.net",
		"abc@lid",
		"abc@g.us",
		"+22670000000@s.whatsapp.net",
		" 12345@s.whatsapp.net",
		"12345@s.whatsapp.net@garbage",
		"12345@@s.whatsapp.net",
		"12345@c.us",
		"12345@hosted",
		"12345@hosted.lid",
		"12345@bot",
		"12345:7@g.us",
		"1-2-3@g.us",
		"-1@g.us",
		"status@broadcast",
		"12345@broadcast",
		"120363000000000000@newsletter",
	} {
		_, err := parseDeleteChatJID(raw)

		assert.Equal(t, codes.InvalidArgument, status.Code(err), raw)
	}
}

func TestDeleteChatMessageKeyDirectChat(t *testing.T) {
	last := storedDeleteChatMessage(types.MessageSource{
		Chat:     deleteChatPN,
		Sender:   types.NewJID("11111", types.DefaultUserServer),
		IsFromMe: true,
	}, "last-id", time.Unix(1750000000, 0))

	key := deleteChatMessageKey(last)

	assert.Equal(t, "22670000000@s.whatsapp.net", key.GetRemoteJID())
	assert.True(t, key.GetFromMe())
	assert.Equal(t, "last-id", key.GetID())
	assert.Nil(t, key.Participant)
}

func TestDeleteChatMessageKeyGroupChat(t *testing.T) {
	fromOther := storedDeleteChatMessage(types.MessageSource{
		Chat:    deleteChatGroupJID,
		Sender:  types.NewADJID("22222", 0, 3),
		IsGroup: true,
	}, "group-id", time.Unix(1750000000, 0))
	fromMe := storedDeleteChatMessage(types.MessageSource{
		Chat:     deleteChatGroupJID,
		Sender:   types.NewJID("11111", types.DefaultUserServer),
		IsFromMe: true,
		IsGroup:  true,
	}, "mine", time.Unix(1750000000, 0))

	key := deleteChatMessageKey(fromOther)
	assert.Equal(t, "120363000000000000@g.us", key.GetRemoteJID())
	assert.False(t, key.GetFromMe())
	assert.Equal(t, "22222@s.whatsapp.net", key.GetParticipant())

	// As in Baileys, an own message has no participant
	key = deleteChatMessageKey(fromMe)
	assert.True(t, key.GetFromMe())
	assert.Nil(t, key.Participant)

	assert.Nil(t, deleteChatMessageKey(nil))
}

// Nothing stored: one mutation per address, the range ends now, without a key.
func TestDeleteChatWithAnEmptyStore(t *testing.T) {
	td := newTestChatDeletion(deleteChatLID)

	result, err := td.run(context.Background(), deleteChatPN, false, nil)

	require.NoError(t, err)
	require.Len(t, td.sent, 1)
	assert.Equal(t, []deleteChatMutation{
		{index: []string{appstate.IndexDeleteChat, "22670000000@s.whatsapp.net", "0"}, end: deleteChatNow.Unix()},
		{index: []string{appstate.IndexDeleteChat, "123456789012345@lid", "0"}, end: deleteChatNow.Unix()},
	}, mutationsOf(t, td.sent[0]))
	assert.Equal(t, deleteChatNow, result.End)
	assert.Len(t, result.Addresses, 2)
	assert.ElementsMatch(t, []types.JID{deleteChatPN, deleteChatLID}, td.messages.asked)
}

// Each mutation carries the key of its own address, and both ranges cover the whole chat.
func TestDeleteChatSendsEachAddressItsOwnKey(t *testing.T) {
	td := newTestChatDeletion(deleteChatLID)
	old := time.Unix(1760000000, 0)
	newer := time.Unix(1760000500, 0)
	td.messages.last[deleteChatPN] = storedDeleteChatMessage(types.MessageSource{Chat: deleteChatPN, Sender: deleteChatPN}, "OLD-PN", old)
	td.messages.last[deleteChatLID] = storedDeleteChatMessage(types.MessageSource{Chat: deleteChatLID, Sender: deleteChatLID}, "NEW-LID", newer)

	result, err := td.run(context.Background(), deleteChatPN, true, nil)

	require.NoError(t, err)
	require.Len(t, td.sent, 1)
	assert.Equal(t, []deleteChatMutation{
		{
			index:    []string{appstate.IndexDeleteChat, "22670000000@s.whatsapp.net", "1"},
			end:      newer.Unix(),
			keyChat:  "22670000000@s.whatsapp.net",
			keyID:    "OLD-PN",
			keyTime:  old.Unix(),
			keyCount: 1,
		},
		{
			index:    []string{appstate.IndexDeleteChat, "123456789012345@lid", "1"},
			end:      newer.Unix(),
			keyChat:  "123456789012345@lid",
			keyID:    "NEW-LID",
			keyTime:  newer.Unix(),
			keyCount: 1,
		},
	}, mutationsOf(t, td.sent[0]))
	assert.Equal(t, newer, result.End)
}

// The phone keeps direct chats under their LID once its chat database is
// migrated: the LID mutation goes first, whatever address was asked.
func TestDeleteChatPutsThePhoneAddressFirst(t *testing.T) {
	for _, tc := range []struct {
		name     string
		lidFirst bool
		asked    types.JID
		alt      types.JID
		first    string
	}{
		{"migrated, phone number asked", true, deleteChatPN, deleteChatLID, "123456789012345@lid"},
		{"migrated, LID asked", true, deleteChatLID, deleteChatPN, "123456789012345@lid"},
		{"not migrated, LID asked", false, deleteChatLID, deleteChatPN, "22670000000@s.whatsapp.net"},
		{"not migrated, phone number asked", false, deleteChatPN, deleteChatLID, "22670000000@s.whatsapp.net"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			td := newTestChatDeletion(tc.alt)
			td.lidFirst = tc.lidFirst

			result, err := td.run(context.Background(), tc.asked, false, nil)

			require.NoError(t, err)
			assert.Equal(t, tc.first, td.sent[0].Mutations[0].Index[1])
			assert.Equal(t, tc.first, result.Addresses[0].JID.String())
		})
	}
}

func TestDeleteChatWithoutKnownOtherAddress(t *testing.T) {
	td := newTestChatDeletion(types.EmptyJID)

	_, err := td.run(context.Background(), deleteChatLID, false, nil)

	require.NoError(t, err)
	mutations := mutationsOf(t, td.sent[0])
	require.Len(t, mutations, 1)
	assert.Equal(t, "123456789012345@lid", mutations[0].index[1])
}

func TestDeleteChatGroupHasOneMutation(t *testing.T) {
	td := newTestChatDeletion(types.EmptyJID)
	td.altJID = func(ctx context.Context, jid types.JID) (types.JID, error) {
		t.Fatalf("a group has no other address")
		return types.EmptyJID, nil
	}
	sent := time.Unix(1750000000, 0)
	td.messages.last[deleteChatGroupJID] = storedDeleteChatMessage(types.MessageSource{
		Chat:    deleteChatGroupJID,
		Sender:  types.NewJID("22222", types.HiddenUserServer),
		IsGroup: true,
	}, "group-id", sent)

	_, err := td.run(context.Background(), deleteChatGroupJID, false, nil)

	require.NoError(t, err)
	mutations := mutationsOf(t, td.sent[0])
	require.Len(t, mutations, 1)
	assert.Equal(t, "120363000000000000@g.us", mutations[0].keyChat)
	assert.Equal(t, sent.Unix(), mutations[0].end)
	key := td.sent[0].Mutations[0].Value.GetDeleteChatAction().GetMessageRange().GetMessages()[0].GetKey()
	assert.Equal(t, "22222@lid", key.GetParticipant())
}

func TestDeleteChatStorageErrorSendsNothing(t *testing.T) {
	td := newTestChatDeletion(deleteChatLID)
	td.messages.err = errors.New("database is locked")

	_, err := td.run(context.Background(), deleteChatPN, false, nil)

	assert.Equal(t, codes.Internal, status.Code(err))
	assert.Empty(t, td.sent)
}

func TestDeleteChatOtherAddressErrorSendsNothing(t *testing.T) {
	td := newTestChatDeletion(deleteChatLID)
	td.altJID = func(ctx context.Context, jid types.JID) (types.JID, error) {
		return types.EmptyJID, errors.New("lid map unavailable")
	}

	_, err := td.run(context.Background(), deleteChatPN, false, nil)

	assert.Equal(t, codes.Internal, status.Code(err))
	assert.Empty(t, td.sent)
	assert.Empty(t, td.messages.asked)
}

func TestDeleteChatSendErrorIsReturned(t *testing.T) {
	td := newTestChatDeletion(deleteChatLID)
	sendErr := errors.New("websocket not connected")
	td.send = func(ctx context.Context, patch appstate.PatchInfo) error { return sendErr }

	_, err := td.run(context.Background(), deleteChatPN, false, nil)

	assert.ErrorIs(t, err, sendErr)
}

func TestDeleteChatMutationKeepsTheUpstreamFormat(t *testing.T) {
	td := newTestChatDeletion(types.EmptyJID)
	sent := time.Unix(1750000000, 0)
	td.messages.last[deleteChatLID] = storedDeleteChatMessage(types.MessageSource{Chat: deleteChatLID, Sender: deleteChatLID}, "last-id", sent)

	_, err := td.run(context.Background(), deleteChatLID, true, nil)

	require.NoError(t, err)
	got := td.sent[0].Mutations[0]
	want := appstate.BuildDeleteChat(deleteChatLID, sent, deleteChatMessageKey(td.messages.last[deleteChatLID]), true).Mutations[0]
	assert.Equal(t, want.Index, got.Index)
	assert.Equal(t, want.Version, got.Version)
	assert.True(t, proto.Equal(want.Value, got.Value), "%v != %v", want.Value, got.Value)
	assert.IsType(t, &waSyncAction.DeleteChatAction{}, got.Value.GetDeleteChatAction())
}

// newDeleteChatTestContainer opens a GOWS database: SQLite, or PostgreSQL when
// GOWS_TEST_PG_DSN is set (its tables are emptied first, run with -p 1).
func newDeleteChatTestContainer(t *testing.T) (*sqlstorage.GContainer, *sqlx.DB) {
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
	for _, table := range []string{"gows_messages", "whatsmeow_lid_map"} {
		_, err = db.Exec("DELETE FROM " + table)
		require.NoError(t, err)
	}
	return container, db
}

// On a real database, each address gets the last message stored under it,
// even when the LID-PN map would merge them.
func TestDeleteChatRangesFromARealStore(t *testing.T) {
	container, db := newDeleteChatTestContainer(t)
	_, err := db.Exec(db.Rebind("INSERT INTO whatsmeow_lid_map (lid, pn) VALUES (?, ?)"), deleteChatLID.User, deleteChatPN.User)
	require.NoError(t, err)
	messages := container.NewMessageStorage()
	old := time.Unix(1760000000, 0)
	newer := time.Unix(1760000500, 0)
	require.NoError(t, messages.UpsertOneMessage(storedDeleteChatMessage(types.MessageSource{Chat: deleteChatPN, Sender: deleteChatPN}, "OLD-PN", old)))
	require.NoError(t, messages.UpsertOneMessage(storedDeleteChatMessage(types.MessageSource{Chat: deleteChatLID, Sender: deleteChatLID}, "NEW-LID", newer)))
	td := newTestChatDeletion(deleteChatLID)
	td.chatDeletion.messages = messages

	_, err = td.run(context.Background(), deleteChatPN, false, nil)

	require.NoError(t, err)
	mutations := mutationsOf(t, td.sent[0])
	require.Len(t, mutations, 2)
	assert.Equal(t, "OLD-PN", mutations[0].keyID)
	assert.Equal(t, "22670000000@s.whatsapp.net", mutations[0].keyChat)
	assert.Equal(t, "NEW-LID", mutations[1].keyID)
	assert.Equal(t, "123456789012345@lid", mutations[1].keyChat)
	assert.Equal(t, newer.Unix(), mutations[0].end)
	assert.Equal(t, newer.Unix(), mutations[1].end)
}

// The phone only offers to delete a group once it is left.
func TestDeleteChatRefusesAGroupStillJoined(t *testing.T) {
	td := newTestChatDeletion(types.EmptyJID)
	td.inGroup = func(ctx context.Context, jid types.JID) (bool, error) {
		return inGroup(&types.GroupInfo{JID: jid}, nil)
	}

	_, err := td.run(context.Background(), deleteChatGroupJID, false, nil)

	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Empty(t, td.sent)
}

func TestDeleteChatAcceptsAGroupLeftOrGone(t *testing.T) {
	for _, groupErr := range []error{whatsmeow.ErrNotInGroup, whatsmeow.ErrGroupNotFound} {
		td := newTestChatDeletion(types.EmptyJID)
		td.inGroup = func(ctx context.Context, jid types.JID) (bool, error) {
			return inGroup(nil, fmt.Errorf("%w: iq error", groupErr))
		}

		_, err := td.run(context.Background(), deleteChatGroupJID, false, nil)

		require.NoError(t, err, groupErr)
		assert.Len(t, td.sent, 1, groupErr)
	}
}

func TestDeleteChatGroupCheckErrorSendsNothing(t *testing.T) {
	td := newTestChatDeletion(types.EmptyJID)
	td.inGroup = func(ctx context.Context, jid types.JID) (bool, error) {
		return inGroup(nil, errors.New("info query timed out"))
	}

	_, err := td.run(context.Background(), deleteChatGroupJID, false, nil)

	assert.Equal(t, codes.Unavailable, status.Code(err))
	assert.Empty(t, td.sent)
}

func TestDeleteChatDoesNotQueryGroupsForDirectChats(t *testing.T) {
	td := newTestChatDeletion(deleteChatLID)
	td.inGroup = func(ctx context.Context, jid types.JID) (bool, error) {
		t.Fatalf("no group query for a direct chat")
		return false, nil
	}

	_, err := td.run(context.Background(), deleteChatPN, false, nil)

	require.NoError(t, err)
}

// The response tells what each mutation covered.
func TestDeleteChatResponse(t *testing.T) {
	td := newTestChatDeletion(deleteChatLID)
	sentAt := time.Unix(1760000500, 0)
	td.messages.last[deleteChatLID] = storedDeleteChatMessage(types.MessageSource{
		Chat:     deleteChatLID,
		Sender:   types.JID{User: "11111", Device: 2, Server: types.DefaultUserServer},
		IsFromMe: true,
	}, "NEW-LID", sentAt)

	result, err := td.run(context.Background(), deleteChatPN, false, nil)
	require.NoError(t, err)
	response := result.toResponse()

	assert.True(t, response.GetConfirmed())
	require.Len(t, response.GetRanges(), 2)
	pn, lid := response.GetRanges()[0], response.GetRanges()[1]
	assert.Equal(t, "22670000000@s.whatsapp.net", pn.GetJid())
	assert.Equal(t, sentAt.Unix(), pn.GetLastMessageTimestamp())
	assert.Empty(t, pn.GetLastMessageId())
	assert.Equal(t, "123456789012345@lid", lid.GetJid())
	assert.Equal(t, sentAt.Unix(), lid.GetLastMessageTimestamp())
	assert.Equal(t, "NEW-LID", lid.GetLastMessageId())
	assert.True(t, lid.GetLastMessageFromMe())
	assert.Equal(t, "11111@s.whatsapp.net", lid.GetLastMessageSender())
	assert.Equal(t, sentAt.Unix(), lid.GetLastMessageSentAt())
	assert.Zero(t, td.refetched)
}

// WhatsApp accepted the patch but reading it back failed: not an error, the
// deletion is done; the app state is read again in the background.
func TestDeleteChatSentButNotReadBack(t *testing.T) {
	td := newTestChatDeletion(deleteChatLID)
	td.send = func(ctx context.Context, patch appstate.PatchInfo) error {
		return fmt.Errorf("%s: %w", appStateFetchAfterSendError, errors.New("context deadline exceeded"))
	}

	result, err := td.run(context.Background(), deleteChatPN, false, nil)

	require.NoError(t, err)
	assert.False(t, result.Confirmed)
	assert.False(t, result.toResponse().GetConfirmed())
	assert.Equal(t, 1, td.refetched)
}

// The chat with yourself: its range never ends at a broadcast list message,
// which is stored under the list, not under the own number or LID.
func TestDeleteTheOwnChatRangeIgnoresBroadcastListMessages(t *testing.T) {
	container, _ := newDeleteChatTestContainer(t)
	messages := container.NewMessageStorage()
	ownPN := types.NewJID("22670000009", types.DefaultUserServer)
	ownLID := types.NewJID("99999", types.HiddenUserServer)
	list := types.NewJID("1760000000", types.BroadcastServer)
	self := time.Unix(1760000000, 0)
	put := func(chat, sender types.JID, id string, ts time.Time) {
		msg := storedDeleteChatMessage(types.MessageSource{Chat: chat, Sender: sender, IsFromMe: true}, id, ts)
		require.NoError(t, messages.UpsertOneMessage(msg))
	}
	put(ownLID, ownLID, "SELF", self)
	put(list, ownLID, "LIST-FROM-PHONE", time.Unix(1760000500, 0))
	put(list, ownPN, "LIST-FROM-API", time.Unix(1760000600, 0))
	td := newTestChatDeletion(ownLID)
	td.chatDeletion.messages = messages

	result, err := td.run(context.Background(), ownPN, false, nil)

	require.NoError(t, err)
	assert.True(t, self.Equal(result.End), "range end %v", result.End)
	mutations := mutationsOf(t, td.sent[0])
	require.Len(t, mutations, 2)
	for _, mutation := range mutations {
		assert.Equal(t, self.Unix(), mutation.end)
		assert.NotContains(t, mutation.index[1], "@broadcast")
		assert.NotContains(t, mutation.keyID, "LIST")
	}
}

// The purge is recorded for each address and the patch timestamp before anything is sent.
func TestDeleteChatRecordsThePurgeAsked(t *testing.T) {
	for _, purge := range []bool{false, true} {
		td := newTestChatDeletion(deleteChatLID)
		td.purgeByDefault = !purge
		td.send = func(ctx context.Context, patch appstate.PatchInfo) error {
			require.Len(t, td.requests, 2, "recorded before sending")
			td.sent = append(td.sent, patch)
			return nil
		}

		result, err := td.run(context.Background(), deleteChatPN, false, proto.Bool(purge))

		require.NoError(t, err)
		assert.Equal(t, purge, result.Purge)
		assert.Equal(t, purge, result.toResponse().GetPurge())
		assert.Equal(t, []deleteChatRequest{
			{jid: deleteChatPN, at: deleteChatNow, purge: purge},
			{jid: deleteChatLID, at: deleteChatNow, purge: purge},
		}, td.requests)
		assert.Equal(t, deleteChatNow, td.sent[0].Timestamp)
	}
}

func TestDeleteChatWithoutPurgeUsesTheSessionSetting(t *testing.T) {
	td := newTestChatDeletion(deleteChatLID)
	td.purgeByDefault = false

	result, err := td.run(context.Background(), deleteChatPN, false, nil)

	require.NoError(t, err)
	assert.False(t, result.Purge)
	assert.Empty(t, td.requests)
}

func TestDeleteChatPurgeNotRecordedSendsNothing(t *testing.T) {
	td := newTestChatDeletion(deleteChatLID)
	td.requestErr = errors.New("database is locked")

	_, err := td.run(context.Background(), deleteChatPN, false, proto.Bool(false))

	assert.Equal(t, codes.Internal, status.Code(err))
	assert.Empty(t, td.sent)
}
