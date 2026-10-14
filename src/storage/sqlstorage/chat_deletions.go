package sqlstorage

import (
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/devlikeapro/gows/storage"
	"github.com/jmoiron/sqlx"
)

// ChatDeletionsTable keeps the deleted chats the client has not confirmed yet.
const ChatDeletionsTable = "gows_chat_deletions"

var _ storage.ChatDeletionStorage = (*SqlChatDeletionStore)(nil)

type SqlChatDeletionStore struct {
	db *sqlx.DB
}

func (gc *GContainer) NewChatDeletionStorage() *SqlChatDeletionStore {
	return &SqlChatDeletionStore{db: gc.db}
}

func (s SqlChatDeletionStore) AddChatDeletion(deletion *storage.PendingChatDeletion) error {
	query, args, err := sq.Insert(ChatDeletionsTable).
		Columns("id", "created_at", "data").
		Values(deletion.ID, deletion.CreatedAt.UTC(), string(deletion.Data)).
		ToSql()
	if err != nil {
		return err
	}
	_, err = s.db.Exec(query, args...)
	return err
}

func (s SqlChatDeletionStore) GetChatDeletions() ([]*storage.PendingChatDeletion, error) {
	query, args, err := sq.Select("id", "created_at", "data").
		From(ChatDeletionsTable).
		OrderBy("created_at ASC", "id ASC").
		ToSql()
	if err != nil {
		return nil, err
	}
	rows, err := s.db.Queryx(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]*storage.PendingChatDeletion, 0)
	for rows.Next() {
		var id, data string
		var createdAt time.Time
		if err = rows.Scan(&id, &createdAt, &data); err != nil {
			return nil, err
		}
		result = append(result, &storage.PendingChatDeletion{ID: id, CreatedAt: createdAt, Data: []byte(data)})
	}
	return result, rows.Err()
}

func (s SqlChatDeletionStore) DeleteChatDeletion(id string) error {
	query, args, err := sq.Delete(ChatDeletionsTable).Where(sq.Eq{"id": id}).ToSql()
	if err != nil {
		return err
	}
	_, err = s.db.Exec(query, args...)
	return err
}

func (s SqlChatDeletionStore) DeleteChatDeletionsBefore(before time.Time) (int64, error) {
	query, args, err := sq.Delete(ChatDeletionsTable).Where(sq.Lt{"created_at": before.UTC()}).ToSql()
	if err != nil {
		return 0, err
	}
	result, err := s.db.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}
