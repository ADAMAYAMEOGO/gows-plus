package gows

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/devlikeapro/gows/storage"
	"github.com/google/uuid"
	"go.mau.fi/whatsmeow/types"
)

// chatDeletionLifetime is how long an unconfirmed deleted chat is kept
const chatDeletionLifetime = 30 * 24 * time.Hour

// needsConfirmation tells whether the client has files to remove or a deletion to notify
func (event *ChatDeletedEvent) needsConfirmation() bool {
	if len(event.MessageIDs) > 0 {
		return true
	}
	return !event.FromFullSync && event.Error == ""
}

// keepChatDeletion stores a deleted chat until the client confirms it, to resend it if nobody listened
func (st *StorageEventHandler) keepChatDeletion(event *ChatDeletedEvent) {
	if st.storage.ChatDeletions == nil || !event.needsConfirmation() {
		return
	}
	event.ID = uuid.NewString()
	data, err := json.Marshal(event)
	if err != nil {
		st.log.Errorf("Failed to keep deleted chat %v: %v", event.JID, err)
		return
	}
	pending := &storage.PendingChatDeletion{ID: event.ID, CreatedAt: time.Now(), Data: data}
	err = retryDeleteChat(func() error {
		return st.storage.ChatDeletions.AddChatDeletion(pending)
	})
	if err != nil {
		st.log.Errorf("Failed to keep deleted chat %v, it is only sent once: %v", event.JID, err)
	}
}

// PendingChatDeletions returns the unconfirmed deleted chats, oldest first
func (gows *GoWS) PendingChatDeletions() ([]*ChatDeletedEvent, error) {
	if gows.Storage == nil || gows.Storage.ChatDeletions == nil {
		return nil, nil
	}
	pending, err := gows.Storage.ChatDeletions.GetChatDeletions()
	if err != nil {
		return nil, err
	}
	result := make([]*ChatDeletedEvent, 0, len(pending))
	for _, item := range pending {
		event := &ChatDeletedEvent{}
		if err = json.Unmarshal(item.Data, event); err != nil {
			gows.Log.Errorf("Dropping unreadable deleted chat %v: %v", item.ID, err)
			_ = gows.Storage.ChatDeletions.DeleteChatDeletion(item.ID)
			continue
		}
		event.ID = item.ID
		result = append(result, event)
	}
	return result, nil
}

// ConfirmChatDeletion forgets a deleted chat the client handled.
func (gows *GoWS) ConfirmChatDeletion(id string) error {
	if gows.Storage == nil || gows.Storage.ChatDeletions == nil {
		return nil
	}
	return gows.Storage.ChatDeletions.DeleteChatDeletion(id)
}

// PruneChatDeletions forgets the deleted chats still unconfirmed after chatDeletionLifetime
func (gows *GoWS) PruneChatDeletions() {
	if gows.Storage == nil || gows.Storage.ChatDeletions == nil {
		return
	}
	pruned, err := gows.Storage.ChatDeletions.DeleteChatDeletionsBefore(time.Now().Add(-chatDeletionLifetime))
	if err != nil {
		gows.Log.Warnf("Failed to prune the deleted chats: %v", err)
	} else if pruned > 0 {
		gows.Log.Warnf("Forgot %d deleted chat(s) never confirmed by the client", pruned)
	}
	_, err = gows.Storage.ChatDeletions.DeleteChatDeletionRequestsBefore(time.Now().Add(-chatDeletionLifetime))
	if err != nil {
		gows.Log.Warnf("Failed to prune the chat deletion requests: %v", err)
	}
}

// RequestChatDeletion records the purge asked for the deletion of jids stamped deletedAt
func (gows *GoWS) RequestChatDeletion(jids []types.JID, deletedAt time.Time, purge bool) error {
	if gows.Storage == nil || gows.Storage.ChatDeletions == nil {
		return errors.New("chat deletion requests are not stored")
	}
	for _, jid := range jids {
		if err := gows.Storage.ChatDeletions.AddChatDeletionRequest(jid, deletedAt, purge); err != nil {
			return err
		}
	}
	return nil
}

// PurgesDeletedChats tells whether deleted chats are cleared from storage by default
func (gows *GoWS) PurgesDeletedChats() bool {
	return gows.storageEventHandler == nil || !gows.storageEventHandler.keepDeletedChats
}
