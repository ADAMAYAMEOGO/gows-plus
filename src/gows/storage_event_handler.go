package gows

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/avast/retry-go"
	"github.com/devlikeapro/gows/storage"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waHistorySync"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// StorageEventHandler handles events from WhatsApp and stores them in the database.
// It can be configured to ignore events from certain types of JIDs based on their server type.
type StorageEventHandler struct {
	gows    *GoWS
	log     waLog.Logger
	storage *storage.Storage
	// ignoreJids specifies which types of JIDs should be ignored when processing events.
	ignoreJids *IgnoreJidsConfig
	// lids replaces the client LID store (tests).
	lids store.LIDStore
	// emit sends an event to the event stream.
	emit func(event interface{})
	// deleteChatLock handles the deleted chats one at a time
	deleteChatLock sync.Mutex
	// recentDeletedChats are the deletions emitted lately
	recentDeletedChats recentKeys
	// keepDeletedChats is StorageConfig.PurgeDeletedChats false
	keepDeletedChats bool
}

func (st *StorageEventHandler) shouldIgnoreJID(jid types.JID) bool {
	if jid.IsEmpty() {
		return false
	}

	if st.ignoreJids == nil {
		return false
	}

	// Check if the JID should be ignored based on its server type
	switch jid.Server {
	case types.BroadcastServer:
		// Do not filter status@broadcast when broadcast-ignore is enabled:
		// it is controlled by the dedicated Status flag.
		if jid.User == "status" {
			return st.ignoreJids.Status
		}
		return st.ignoreJids.Broadcast
	case types.GroupServer:
		return st.ignoreJids.Groups
	case types.NewsletterServer:
		return st.ignoreJids.Newsletters
	default:
		return false
	}
}

func (st *StorageEventHandler) GetMessageForRetry(requester, to types.JID, id types.MessageID) *waE2E.Message {
	msg, err := st.storage.Messages.GetMessage(id)
	if err != nil {
		st.log.Errorf("Error getting message for retry - requester %v, to %v, id %v: %v", requester, to, id, err)
		return nil
	}
	if msg == nil || msg.Message == nil {
		st.log.Warnf("No message for retry - requester %v, to %v, id %v", requester, to, id)
		return nil
	}
	return msg.Message.RawMessage
}

func isRealMessage(event *events.Message) bool {
	if event.Message == nil {
		return false
	}
	msg := event.Message
	switch {
	case msg.Conversation != nil:
		return true
	case msg.ExtendedTextMessage != nil:
		return true
	case msg.ImageMessage != nil:
		return true
	case msg.ContactMessage != nil:
		return true
	case msg.LocationMessage != nil:
		return true
	case msg.VideoMessage != nil:
		return true
	case msg.PtvMessage != nil:
		return true
	case msg.AudioMessage != nil:
		return true
	case msg.DocumentMessage != nil:
		return true
	case msg.DocumentWithCaptionMessage != nil:
		return true
	case msg.StickerMessage != nil:
		return true
	case msg.ContactsArrayMessage != nil:
		return true
	case msg.TemplateMessage != nil:
		return true
	case msg.ListMessage != nil:
		return true
	case msg.RichResponseMessage != nil:
		return true
	case msg.PollCreationMessage != nil:
		return true
	case msg.PollCreationMessageV2 != nil:
		return true
	case msg.PollCreationMessageV3 != nil:
		return true
	case msg.PollCreationMessageV4 != nil:
		return true
	case msg.PollCreationMessageV5 != nil:
		return true
	case msg.ButtonsResponseMessage != nil:
		return true
	default:
		return false
	}
}

func (st *StorageEventHandler) handleEvent(event interface{}) {
	switch event.(type) {
	case *events.LabelEdit, *events.LabelAssociationChat:
		// Keep whatsmeow's order - the last change per label/chat wins
		// In case of "add" then "delete" and similar events for 1 entity
		st.processEvent(event)
	default:
		go st.processEvent(event)
	}
}

func (st *StorageEventHandler) processEvent(event interface{}) {
	// Handle all panic and log error + stack
	defer func() {
		if err := recover(); err != nil {
			stack := debug.Stack()
			st.log.Errorf("Panic happened in handle event: %v. Stack: %s. Event: %v", err, stack, event)
		}
	}()

	switch event.(type) {
	case *events.Message:
		msg := event.(*events.Message)
		if st.shouldIgnoreJID(msg.Info.Chat) {
			return
		}
		var status storage.Status
		if msg.Info.IsFromMe {
			status = storage.StatusServerAck
		} else {
			status = storage.StatusDeliveryAck
		}
		st.handleSaveMessage(msg, &status)
		st.handleMessageEvent(msg)
	case *events.Receipt:
		receipt := event.(*events.Receipt)
		if st.shouldIgnoreJID(receipt.Chat) {
			return
		}
		st.handleReceipt(receipt)
	case *events.UndecryptableMessage:
		msg := event.(*events.UndecryptableMessage)
		if st.shouldIgnoreJID(msg.Info.Chat) {
			return
		}
		st.log.Errorf(
			"Undecryptable message %s in %s from %s (unavailable: %v, type: %q, failMode: %q) - content lost unless the sender answers the retry receipt",
			msg.Info.ID, msg.Info.Chat, msg.Info.Sender,
			msg.IsUnavailable, msg.UnavailableType, msg.DecryptFailMode,
		)
	case *events.HistorySync:
		st.handleHistorySync(event.(*events.HistorySync))
	// Groups
	case *events.JoinedGroup:
		group := event.(*events.JoinedGroup)
		if st.shouldIgnoreJID(group.JID) {
			return
		}
		st.handleMeJoinedGroup(group)
	case *events.GroupInfo:
		info := event.(*events.GroupInfo)
		if st.shouldIgnoreJID(info.JID) {
			return
		}
		left := st.handleMeLeftGroup(info)
		if left {
			return
		}
		st.handleGroupInfo(info)
	case *events.DeleteChat:
		deleteChat := event.(*events.DeleteChat)
		if st.shouldIgnoreJID(deleteChat.JID) {
			return
		}
		st.handleDeleteChat(deleteChat)
	case *events.Contact:
		contact := event.(*events.Contact)
		if st.shouldIgnoreJID(contact.JID) {
			return
		}
		st.handleContact(contact)
	// Labels
	case *events.LabelEdit:
		st.handleLabelEdit(event.(*events.LabelEdit))
	case *events.LabelAssociationChat:
		labelAssoc := event.(*events.LabelAssociationChat)
		if st.shouldIgnoreJID(labelAssoc.JID) {
			return
		}
		st.handleLabelAssociationChat(labelAssoc)
	}
}

func (st *StorageEventHandler) handleSaveMessage(event *events.Message, status *storage.Status) {
	messageToStore := &storage.StoredMessage{
		Message: event,
		Status:  status,
		IsReal:  isRealMessage(event),
	}

	err := st.storage.Messages.UpsertOneMessage(messageToStore)
	if err != nil {
		st.log.Errorf("Error storing message %v(%v): %v", event.Info.Chat, event.Info.ID, err)
	}
}

func (st *StorageEventHandler) handleMessageEvent(event *events.Message) {
	if event.Message == nil {
		return
	}
	protocolMessage := event.Message.GetProtocolMessage()

	// Revoked message
	// NOTE: ProtocolMessage_REVOKE is the zero value, always check the message itself first
	isRevoked := protocolMessage != nil && protocolMessage.GetType() == waE2E.ProtocolMessage_REVOKE
	if isRevoked {
		id := protocolMessage.GetKey().GetID()
		if id == "" {
			st.log.Warnf("Revoke message %v without a message id, ignoring", event.Info.ID)
			return
		}
		err := st.storage.Messages.DeleteMessage(id)
		if err != nil {
			st.log.Errorf("Error deleting message %v: %v", id, err)
		}
		return
	}

	// Chat ephemeral settings - changed
	if protocolMessage != nil {
		setting := ExtractEphemeralSettingsFromProtocolMessage(event.Info, protocolMessage)
		if setting != nil {
			err := st.storage.ChatEphemeralSetting.UpdateChatEphemeralSetting(setting)
			if err != nil {
				st.log.Errorf("Error updating chat ephemeral setting %v: %v", setting.ID, err)
			}
			st.log.Debugf("Changed chat ephemeral setting %v (enabled: %v)", setting.ID, setting.IsEphemeral)
			return
		}
	}

	// Chat ephemeral settings - from message
	setting := ExtractEphemeralSettingsFromMsg(event)
	if setting != nil {
		err := st.storage.ChatEphemeralSetting.UpdateChatEphemeralSetting(setting)
		if err != nil {
			st.log.Errorf("Error updating chat ephemeral setting %v: %v", setting.ID, err)
		}
		st.log.Debugf("Initial chat ephemeral setting %v (enabled: %v)", setting.ID, setting.IsEphemeral)
		// Do not return - we still need to handle the message
		// return
	}
}

func (st *StorageEventHandler) handleReceipt(event *events.Receipt) {
	var status storage.Status
	switch event.Type {
	case types.ReceiptTypeDelivered:
		status = storage.StatusDeliveryAck
	case types.ReceiptTypeRead:
		status = storage.StatusRead
	case types.ReceiptTypePlayed:
		status = storage.StatusPlayed
	default:
		st.log.Debugf("Unknown receipt type: %v", event.Type)
		return
	}
	if event.Chat.Server == types.StatusBroadcastJID.Server && event.Chat.User == types.StatusBroadcastJID.User {
		// Ignore
		st.log.Debugf("Ignoring receipt for '%v(%v)'", event.Chat, event.MessageIDs)
	}
	for _, id := range event.MessageIDs {
		st.log.Debugf("Updating status for message %v(%v) to %v (receipt type: '%v')", event.Chat, id, status, event.Type.GoString())
		msg, err := st.storage.Messages.GetMessageWithRetries(id)
		if errors.Is(err, storage.ErrNotFound) {
			st.log.Debugf("Message %v(%v) not found", event.Chat, id)
			continue
		}
		if err != nil {
			st.log.Debugf("Error getting message - storage handle receipt %v(%v): %v", event.Chat, id, err)
			continue
		}
		if msg.Status != nil && *msg.Status >= status {
			continue
		}
		msg.Status = &status
		err = st.storage.Messages.UpsertOneMessage(msg)
		if err != nil {
			st.log.Errorf("Error updating status for message %v(%v): %v", event.Chat, id, err)
			continue
		}
		st.log.Debugf("Updated status for message %v(%v) to %v", event.Chat, id, status)
	}
}

func (st *StorageEventHandler) handleHistorySync(event *events.HistorySync) {
	conversations := event.Data.GetConversations()
	for _, conv := range conversations {
		jid, err := types.ParseJID(conv.GetID())
		if err != nil {
			st.log.Errorf("Error parsing JID: %v", err)
			continue
		}

		// Skip this conversation if the JID should be ignored
		if st.shouldIgnoreJID(jid) {
			continue
		}

		go st.saveHistoryForOneChat(conv, jid)
	}
	st.log.Debugf("Saved history for %v chats", len(conversations))
}

func (st *StorageEventHandler) saveHistoryForOneChat(conv *waHistorySync.Conversation, chatJID types.JID) {
	historyMessages := conv.GetMessages()
	for _, historyMsg := range historyMessages {
		message := historyMsg.GetMessage()
		msg, err := st.gows.ParseWebMessage(chatJID, message)
		if err != nil {
			st.log.Errorf("Error parsing message: %v", err)
			continue
		}

		var status storage.Status
		if msg.SourceWebMsg != nil && msg.SourceWebMsg.Status != nil {
			status = storage.Status(*msg.SourceWebMsg.Status)
		}

		st.handleSaveMessage(msg, &status)
	}
	st.log.Debugf("Saved %v messages in %v", len(conv.GetMessages()), chatJID)

	setting := ExtractEphemeralSettingsFromConversation(conv, chatJID)
	if setting != nil {
		err := st.storage.ChatEphemeralSetting.UpdateChatEphemeralSetting(setting)
		if err != nil {
			st.log.Errorf("Error updating chat ephemeral setting %v: %v", setting.ID, err)
		}
		st.log.Debugf("Initial chat ephemeral setting %v (enabled: %v)", setting.ID, setting.IsEphemeral)
	}
}

func (st *StorageEventHandler) handleMeJoinedGroup(group *events.JoinedGroup) {
	err := st.storage.Groups.UpsertOneGroup(&group.GroupInfo)
	if err != nil {
		st.log.Errorf("Error storing group %v: %v", group.JID, err)
	}
	st.log.Debugf("I joined group %v", group.JID)
}

func (st *StorageEventHandler) handleMeLeftGroup(info *events.GroupInfo) bool {
	jid := st.gows.Store.ID
	if jid == nil {
		// Not logged in, we can't tell if it's us who left
		return false
	}
	for _, leave := range info.Leave {
		if leave == jid.ToNonAD() {
			st.log.Debugf("I left group %v", info.JID)
			err := st.storage.Groups.DeleteGroup(info.JID)
			if err != nil {
				st.log.Errorf("Error deleting group %v: %v", info.JID, err)
			}
			return true
		}
	}
	return false
}

func (st *StorageEventHandler) handleGroupInfo(info *events.GroupInfo) {
	err := retry.Do(func() error {
		return st.storage.Groups.UpdateGroup(info)
	})
	if err != nil {
		st.log.Errorf("Error updating group %v: %v", info.JID, err)
	}
	return
}

// handleDeleteChat clears a deleted chat (messages up to the deletion, ephemeral setting) under both
// addresses of a direct chat unless it is kept (purgeDeletedChat), then emits ChatDeletedEvent once.
func (st *StorageEventHandler) handleDeleteChat(event *events.DeleteChat) {
	if event.JID.IsEmpty() {
		return
	}
	st.deleteChatLock.Lock()
	defer st.deleteChatLock.Unlock()

	deleteBefore := deleteChatBefore(event, time.Now())
	altJID := st.altChatJID(event.JID)
	deleted := make([]types.MessageID, 0)
	failures := make([]string, 0)
	purge, err := st.purgeDeletedChat(event)
	if err != nil {
		st.log.Errorf("Error reading the deletion request of chat %v, keeping it: %v", event.JID, err)
		failures = append(failures, fmt.Sprintf("deletion request of %v: %v", event.JID, err))
	}
	for _, jid := range []types.JID{event.JID, altJID} {
		if jid.IsEmpty() || !purge {
			continue
		}
		var ids []types.MessageID
		err := retryDeleteChat(func() (err error) {
			ids, err = st.storage.Messages.DeleteChatMessages(jid, deleteBefore)
			return err
		})
		if err != nil {
			st.log.Errorf("Error deleting chat messages %v: %v", jid, err)
			failures = append(failures, fmt.Sprintf("messages of %v: %v", jid, err))
		}
		deleted = append(deleted, ids...)
		err = retryDeleteChat(func() error {
			return st.storage.ChatEphemeralSetting.DeleteChatEphemeralSetting(jid, deleteBefore)
		})
		if err != nil {
			st.log.Errorf("Error deleting chat ephemeral setting %v: %v", jid, err)
			failures = append(failures, fmt.Sprintf("ephemeral setting of %v: %v", jid, err))
		}
	}
	st.log.Debugf("Deleted chat %v (%v): purge %v, %d messages before %v", event.JID, altJID, purge, len(deleted), deleteBefore)

	key := deletedChatKey(event, altJID, deleteBefore)
	if len(deleted) == 0 && len(failures) == 0 && st.recentDeletedChats.seen(key) {
		st.log.Debugf("Chat %v already deleted with %v, not emitted again", event.JID, altJID)
		return
	}
	st.recentDeletedChats.add(key)
	chatDeleted := &ChatDeletedEvent{
		JID:           event.JID,
		AltJID:        altJID,
		Timestamp:     event.Timestamp,
		DeletedBefore: deleteBefore,
		DeleteMedia:   event.DeleteMedia,
		FromFullSync:  event.FromFullSync,
		Purged:        purge,
		MessageIDs:    deleted,
		Error:         strings.Join(failures, "; "),
	}
	st.keepChatDeletion(chatDeleted)
	st.emitEvent(chatDeleted)
}

// purgeDeletedChat follows the DeleteChat request of this deletion, else the session setting.
// A request that can't be read keeps the chat.
func (st *StorageEventHandler) purgeDeletedChat(event *events.DeleteChat) (bool, error) {
	if st.storage.ChatDeletions == nil {
		return !st.keepDeletedChats, nil
	}
	var purge, found bool
	err := retryDeleteChat(func() (err error) {
		purge, found, err = st.storage.ChatDeletions.GetChatDeletionRequest(event.JID, event.Timestamp)
		return err
	})
	if err != nil {
		return false, err
	}
	if found {
		return purge, nil
	}
	return !st.keepDeletedChats, nil
}

// retryDeleteChat retries a cleanup step: the mutation is not delivered again
func retryDeleteChat(step func() error) error {
	return retry.Do(
		step,
		retry.Attempts(4),
		retry.Delay(deleteChatRetryDelay),
		retry.LastErrorOnly(true),
	)
}

// deleteChatRetryDelay is the base wait between two attempts of a cleanup step.
var deleteChatRetryDelay = 250 * time.Millisecond

// deletedChatKey identifies one deletion of a chat, whichever address it came for
func deletedChatKey(event *events.DeleteChat, altJID types.JID, deleteBefore time.Time) string {
	jids := []string{event.JID.String(), altJID.String()}
	sort.Strings(jids)
	return fmt.Sprintf("%s|%s|%d|%d", jids[0], jids[1], event.Timestamp.UnixMilli(), deleteBefore.UnixNano())
}

// recentKeys remembers keys for a while.
type recentKeys struct {
	lock sync.Mutex
	keys map[string]time.Time
}

const recentDeletedChatTTL = 10 * time.Minute

func (r *recentKeys) seen(key string) bool {
	r.lock.Lock()
	defer r.lock.Unlock()
	at, ok := r.keys[key]
	return ok && time.Since(at) < recentDeletedChatTTL
}

func (r *recentKeys) add(key string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.keys == nil {
		r.keys = make(map[string]time.Time)
	}
	for k, at := range r.keys {
		if time.Since(at) >= recentDeletedChatTTL {
			delete(r.keys, k)
		}
	}
	r.keys[key] = time.Now()
}

// deleteChatClockSkew is how far after the deletion its range may end (device and server clocks differ)
const deleteChatClockSkew = time.Minute

// deleteChatBefore returns the bound of the deleted messages: just after the last second of the range,
// or the deletion time without a range or when the range ends after it (ms timestamp, clock ahead)
func deleteChatBefore(event *events.DeleteChat, now time.Time) time.Time {
	deletedAt := event.Timestamp
	if deletedAt.IsZero() || deletedAt.After(now) {
		deletedAt = now
	}
	messageRange := event.Action.GetMessageRange()
	last := max(messageRange.GetLastMessageTimestamp(), messageRange.GetLastSystemMessageTimestamp())
	for _, message := range messageRange.GetMessages() {
		last = max(last, message.GetTimestamp())
	}
	if last <= 0 {
		return deletedAt
	}
	if last >= deletedAt.Add(deleteChatClockSkew).Unix() {
		return deletedAt
	}
	return time.Unix(last+1, 0)
}

// altChatJID returns the other address of a direct chat, empty on lookup error
func (st *StorageEventHandler) altChatJID(jid types.JID) types.JID {
	ctx := context.Background()
	if st.gows != nil {
		ctx = st.gows.Context
	}
	var valid func(lid, pn types.JID) bool
	if st.gows != nil {
		valid = st.gows.validLIDPNPair
	}
	alt, err := altChatJID(ctx, st.lidStore(), valid, st.log, jid)
	if err != nil {
		st.log.Warnf("Failed to get the other address of chat %v: %v", jid, err)
		return types.EmptyJID
	}
	return alt
}

func (st *StorageEventHandler) lidStore() store.LIDStore {
	if st.lids != nil {
		return st.lids
	}
	if st.gows == nil || st.gows.Client == nil || st.gows.Store == nil {
		return nil
	}
	return st.gows.Store.LIDs
}

func (st *StorageEventHandler) emitEvent(event interface{}) {
	if st.emit != nil {
		st.emit(event)
	}
}

func (st *StorageEventHandler) handleLabelEdit(event *events.LabelEdit) {
	if event.Action == nil {
		return
	}
	action := event.Action
	// Delete
	if action.GetDeleted() {
		// No FK cascade anymore, drop the associations here
		err := st.storage.LabelAssociations.RemoveAssociationsByLabelID(event.LabelID)
		if err != nil {
			st.log.Errorf("Error removing label associations for label %v: %v", event.LabelID, err)
			return
		}
		err = st.storage.Labels.DeleteLabel(event.LabelID)
		if err != nil {
			st.log.Errorf("Error deleting label %v: %v", event.LabelID, err)
			return
		}
		st.log.Debugf("Deleted label %v", event.LabelID)
		return
	}

	// Create
	label := &storage.Label{
		ID:    event.LabelID,
		Name:  action.GetName(),
		Color: int(action.GetColor()),
	}

	err := st.storage.Labels.UpsertLabel(label)
	if err != nil {
		st.log.Errorf("Error upserting label %v: %v", label.ID, err)
		return
	}

	st.log.Debugf("Upserted label %v (%v)", label.ID, label.Name)
}

func (st *StorageEventHandler) handleLabelAssociationChat(event *events.LabelAssociationChat) {
	// Get the association data directly from the event
	jid := event.JID
	labelID := event.LabelID
	labeled := event.Action.GetLabeled()

	if labeled {
		// The label may not be stored yet (it can come later in the sync), store the association anyway
		err := st.storage.LabelAssociations.AddAssociation(jid, labelID)
		if err != nil {
			st.log.Errorf("Error adding label association for JID %v and label %v: %v", jid, labelID, err)
			return
		}
		st.log.Debugf("Added label association for JID %v and label %v", jid, labelID)
	} else {
		err := st.storage.LabelAssociations.RemoveAssociation(jid, labelID)
		if err != nil {
			st.log.Errorf("Error removing label association for JID %v and label %v: %v", jid, labelID, err)
			return
		}
		st.log.Debugf("Removed label association for JID %v and label %v", jid, labelID)
	}
}

func (st *StorageEventHandler) handleContact(contact *events.Contact) {
	st.handleContactLidJidMapping(contact)
}

func (st *StorageEventHandler) handleContactLidJidMapping(contact *events.Contact) {
	ctx := st.gows.Context
	cli := st.gows.Client

	// Save lid to jid mapping
	if cli == nil || cli.Store == nil || cli.Store.LIDs == nil {
		return
	}

	var err error
	var lid = types.EmptyJID
	var jid = types.EmptyJID

	act := contact.Action
	switch contact.JID.Server {
	// jid => lid
	case types.DefaultUserServer:
		jid = contact.JID
		if act.GetLidJID() != "" {
			lid, err = types.ParseJID(act.GetLidJID())
			if err != nil {
				st.log.Errorf("Failed to parse LID JID: %v", err)
				return
			}
		}

	// lid => jid
	case types.HiddenUserServer:
		lid = contact.JID
		if act.GetPnJID() != "" {
			jid, err = types.ParseJID(act.GetPnJID())
			if err != nil {
				st.log.Errorf("Failed to parse PN JID: %v", err)
				return
			}
		}
	}

	// Save lid to jid mapping
	if lid == types.EmptyJID || jid == types.EmptyJID {
		return
	}
	err = cli.Store.LIDs.PutLIDMapping(ctx, lid, jid)
	if err != nil {
		st.log.Errorf("Failed to update LID mapping (%v => %v): %v", lid, jid, err)
		return
	}
}
