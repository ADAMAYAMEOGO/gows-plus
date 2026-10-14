package gows

import (
	"time"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

type ConnectedEventData struct {
	ID       *types.JID
	LID      *types.JID
	PushName string
}

type EventMessageResponse struct {
	*events.Message
	EventResponse *waE2E.EventResponseMessage
}

type PollVoteEvent struct {
	*events.Message
	Votes *[]string
}

// ChatDeletedEvent is emitted once a deleted chat (events.DeleteChat) is cleared from storage
type ChatDeletedEvent struct {
	ID            string            // set when the client must confirm it (AckChatDeleted)
	JID           types.JID         // the chat of the app state mutation
	AltJID        types.JID         // the LID or phone number of a direct chat, empty if unknown
	Timestamp     time.Time         // when the chat was deleted
	DeletedBefore time.Time         // the stored messages sent before this time were deleted
	DeleteMedia   bool              // the media flag of the mutation (phone gallery)
	FromFullSync  bool              // whether the deletion comes from an app state full sync
	MessageIDs    []types.MessageID // the deleted stored messages
	Error         string            // what failed in the cleanup, empty if it all went through
}

type GroupJoinRequest struct {
	JID           types.JID
	PhoneNumber   types.JID // zero value if not provided
	RequestMethod string    // invite_link | linked_group_join | non_admin_add
}

type GroupJoinRequestEvent struct {
	JID       types.JID // the group JID
	Sender    *types.JID
	SenderPN  *types.JID
	Timestamp time.Time
	Action    string // created | revoked
	Requests  []GroupJoinRequest
}
