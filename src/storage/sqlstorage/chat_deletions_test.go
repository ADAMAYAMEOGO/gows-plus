package sqlstorage

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/devlikeapro/gows/storage"
	"github.com/stretchr/testify/require"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// newTestContainer opens a GOWS database: SQLite, or PostgreSQL when
// GOWS_TEST_PG_DSN is set (the tables used are emptied first, run with -p 1).
func newTestContainer(t *testing.T, tables ...string) *GContainer {
	t.Helper()
	dialect, address := "sqlite3", "file:"+filepath.Join(t.TempDir(), "gows.db")+"?_foreign_keys=on"
	if dsn := os.Getenv("GOWS_TEST_PG_DSN"); dsn != "" {
		dialect, address = "postgres", dsn
	}
	container, err := New(dialect, address, waLog.Noop)
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Close() })
	for _, table := range tables {
		_, err = container.db.Exec("DELETE FROM " + table)
		require.NoError(t, err)
	}
	return container
}

func TestChatDeletionsAreKeptUntilConfirmed(t *testing.T) {
	store := newTestContainer(t, ChatDeletionsTable).NewChatDeletionStorage()
	older := &storage.PendingChatDeletion{ID: "b", CreatedAt: time.Unix(1760000000, 0), Data: []byte(`{"JID":"1@s.whatsapp.net"}`)}
	newer := &storage.PendingChatDeletion{ID: "a", CreatedAt: time.Unix(1760000100, 0), Data: []byte(`{"JID":"2@lid"}`)}
	require.NoError(t, store.AddChatDeletion(newer))
	require.NoError(t, store.AddChatDeletion(older))
	require.Error(t, store.AddChatDeletion(older), "the id is unique")

	pending, err := store.GetChatDeletions()
	require.NoError(t, err)
	require.Len(t, pending, 2)
	require.Equal(t, "b", pending[0].ID)
	require.Equal(t, `{"JID":"1@s.whatsapp.net"}`, string(pending[0].Data))
	require.True(t, pending[0].CreatedAt.Equal(older.CreatedAt), pending[0].CreatedAt)
	require.Equal(t, "a", pending[1].ID)

	require.NoError(t, store.DeleteChatDeletion("b"))
	require.NoError(t, store.DeleteChatDeletion("unknown"))
	pending, err = store.GetChatDeletions()
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, "a", pending[0].ID)

	pruned, err := store.DeleteChatDeletionsBefore(time.Unix(1760000050, 0))
	require.NoError(t, err)
	require.Zero(t, pruned)
	pruned, err = store.DeleteChatDeletionsBefore(time.Unix(1760000200, 0))
	require.NoError(t, err)
	require.Equal(t, int64(1), pruned)
	pending, err = store.GetChatDeletions()
	require.NoError(t, err)
	require.Empty(t, pending)
}

// The pending deletions survive a reopen of the database.
func TestChatDeletionsTableSurvivesAReopen(t *testing.T) {
	path := "file:" + filepath.Join(t.TempDir(), "gows.db") + "?_foreign_keys=on"
	first, err := New("sqlite3", path, waLog.Noop)
	require.NoError(t, err)
	require.NoError(t, first.NewChatDeletionStorage().AddChatDeletion(&storage.PendingChatDeletion{ID: "a", CreatedAt: time.Now(), Data: []byte("{}")}))
	require.NoError(t, first.Close())

	second, err := New("sqlite3", path, waLog.Noop)
	require.NoError(t, err)
	defer second.Close()
	pending, err := second.NewChatDeletionStorage().GetChatDeletions()
	require.NoError(t, err)
	require.Len(t, pending, 1)
}

// A request is found for its own chat and deletion time only, and stays
// until pruned.
func TestChatDeletionRequestsMatchTheirOwnDeletion(t *testing.T) {
	store := newTestContainer(t, ChatDeletionRequestsTable).NewChatDeletionStorage()
	pn := types.NewJID("22670000000", types.DefaultUserServer)
	lid := types.NewJID("123456789012345", types.HiddenUserServer)
	at := time.UnixMilli(1760000100123)
	require.NoError(t, store.AddChatDeletionRequest(pn, at, false))
	require.NoError(t, store.AddChatDeletionRequest(lid, at, true))

	_, found, err := store.GetChatDeletionRequest(pn, at.Add(time.Millisecond))
	require.NoError(t, err)
	require.False(t, found, "another deletion time")
	_, found, err = store.GetChatDeletionRequest(types.NewJID("1", types.DefaultUserServer), at)
	require.NoError(t, err)
	require.False(t, found, "another chat")

	for i := 0; i < 2; i++ {
		purge, found, err := store.GetChatDeletionRequest(pn, at)
		require.NoError(t, err)
		require.True(t, found)
		require.False(t, purge)
	}
	purge, found, err := store.GetChatDeletionRequest(lid, at)
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, purge)

	pruned, err := store.DeleteChatDeletionRequestsBefore(time.Now().Add(time.Hour))
	require.NoError(t, err)
	require.Equal(t, int64(2), pruned)
	_, found, err = store.GetChatDeletionRequest(pn, at)
	require.NoError(t, err)
	require.False(t, found)
}
