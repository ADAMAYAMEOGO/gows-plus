package storage

import (
	"time"

	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

type Storage struct {
	Messages             MessageStorage
	Contacts             ContactStorage
	Chats                ChatStorage
	Groups               GroupStorage
	ChatEphemeralSetting ChatEphemeralSettingStorage
	Labels               LabelStorage
	LabelAssociations    LabelAssociationStorage
	Lidmap               LidmapStorage
	ChatDeletions        ChatDeletionStorage
}

type MessageStorage interface {
	UpsertOneMessage(msg *StoredMessage) error
	GetLastMessagesInChats(filter ChatFilter, sortBy Sort, pagination Pagination, merge bool) ([]*StoredMessage, error)
	GetAllMessages(filters MessageFilter, sortBy Sort, pagination Pagination, merge bool) ([]*StoredMessage, error)
	GetChatMessages(jid types.JID, filters MessageFilter, pagination Pagination, merge bool) ([]*StoredMessage, error)
	// GetMessage fetches a message by ID without retries.
	GetMessage(id types.MessageID) (*StoredMessage, error)
	// GetMessageWithRetries fetches a message by ID with retries.
	GetMessageWithRetries(id types.MessageID) (*StoredMessage, error)
	// DeleteChatMessages deletes the chat messages sent before deleteBefore, returns their ids
	DeleteChatMessages(jid types.JID, deleteBefore time.Time) ([]types.MessageID, error)
	DeleteMessage(id types.MessageID) error
}

type GroupStorage interface {
	FetchGroups(force bool) error
	UpdateGroup(update *events.GroupInfo) error
	UpsertOneGroup(group *types.GroupInfo) error
	GetAllGroups(sort Sort, pagination Pagination) ([]*types.GroupInfo, error)
	GetGroup(jid types.JID) (*types.GroupInfo, error)
	DeleteGroup(jid types.JID) error
	DeleteGroups() error
}

type ContactStorage interface {
	GetContact(user types.JID) (*StoredContact, error)
	GetAllContacts(sortBy Sort, pagination Pagination) ([]*StoredContact, error)
}

type ChatStorage interface {
	GetChats(filter ChatFilter, sortBy Sort, pagination Pagination, merge bool) ([]*StoredChat, error)
}

type ChatEphemeralSettingStorage interface {
	GetChatEphemeralSetting(id types.JID) (*StoredChatEphemeralSetting, error)
	UpdateChatEphemeralSetting(setting *StoredChatEphemeralSetting) error
	DeleteChatEphemeralSetting(id types.JID, deleteBefore time.Time) error
}

type LabelStorage interface {
	GetAllLabels() ([]*Label, error)
	GetLabelById(id string) (*Label, error)
	UpsertLabel(label *Label) error
	DeleteLabel(id string) error
}

type LabelAssociationStorage interface {
	GetJIDsByLabelID(labelID string) ([]types.JID, error)
	GetLabelIDsByJID(jid types.JID) ([]string, error)
	AddAssociation(jid types.JID, labelID string) error
	RemoveAssociation(jid types.JID, labelID string) error
	RemoveAssociationsByLabelID(labelID string) error
}

type LidmapStorage interface {
	// GetAllLidMap returns all lid/pn pairs from the database as an array of LidmapEntry
	GetAllLidMap() ([]LidmapEntry, error)
	// GetLidCount returns the count of lids in the database
	GetLidCount() (int, error)
}

// PendingChatDeletion is a deleted chat the client has not confirmed yet.
type PendingChatDeletion struct {
	ID        string
	CreatedAt time.Time
	// Data is the event as sent to the client (JSON)
	Data []byte
}

// ChatDeletionStorage keeps the deleted chats until the client confirms them
type ChatDeletionStorage interface {
	AddChatDeletion(deletion *PendingChatDeletion) error
	// GetChatDeletions returns the pending deletions, oldest first
	GetChatDeletions() ([]*PendingChatDeletion, error)
	DeleteChatDeletion(id string) error
	// DeleteChatDeletionsBefore forgets the deletions added before the time
	DeleteChatDeletionsBefore(before time.Time) (int64, error)

	// AddChatDeletionRequest records the purge asked for the deletion of jid stamped deletedAt
	AddChatDeletionRequest(jid types.JID, deletedAt time.Time, purge bool) error
	// GetChatDeletionRequest returns that request, found is false for a deletion made elsewhere
	GetChatDeletionRequest(jid types.JID, deletedAt time.Time) (purge bool, found bool, err error)
	// DeleteChatDeletionRequestsBefore forgets the requests added before the time
	DeleteChatDeletionRequestsBefore(before time.Time) (int64, error)
}
