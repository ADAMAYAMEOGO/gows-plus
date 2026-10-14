package server

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"time"

	"github.com/devlikeapro/gows/gows"
	"github.com/devlikeapro/gows/proto"
	"github.com/devlikeapro/gows/storage"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// DeleteChat deletes a chat for the account with an app state patch, like "Delete chat" on the phone
func (s *Server) DeleteChat(ctx context.Context, req *__.DeleteChatRequest) (*__.DeleteChatResponse, error) {
	cli, err := s.Sm.Get(req.GetSession().GetId())
	if err != nil {
		return nil, err
	}
	jid, err := parseDeleteChatJID(req.GetJid())
	if err != nil {
		return nil, err
	}
	var purge *bool
	if req.Purge != nil {
		purge = proto.Bool(req.GetPurge())
	}
	result, err := newChatDeletion(cli).run(ctx, jid, req.GetDeleteMedia(), purge)
	if err != nil {
		return nil, err
	}
	return result.toResponse(), nil
}

// AckChatDeleted confirms a gows.ChatDeletedEvent was handled, so it is not sent again
func (s *Server) AckChatDeleted(ctx context.Context, req *__.AckChatDeletedRequest) (*__.Empty, error) {
	cli, err := s.Sm.Get(req.GetSession().GetId())
	if err != nil {
		return nil, err
	}
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "missing deleted chat id")
	}
	if err = cli.ConfirmChatDeletion(req.GetId()); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to confirm deleted chat %s: %v", req.GetId(), err)
	}
	return &__.Empty{}, nil
}

// pendingChatDeletions returns the unconfirmed deleted chats of a running session
func (s *Server) pendingChatDeletions(session string) []*gows.ChatDeletedEvent {
	cli, err := s.Sm.Get(session)
	if err != nil {
		return nil
	}
	pending, err := cli.PendingChatDeletions()
	if err != nil {
		s.log.Errorf("Failed to read the deleted chats of session %s: %v", session, err)
		return nil
	}
	return pending
}

// replayChatDeletions resends the unconfirmed deleted chats to a listener that just connected
func (s *Server) replayChatDeletions(session string, listener *eventListener) {
	pushPendingEvents(listener, s.pendingChatDeletions(session))
}

func pushPendingEvents(listener *eventListener, events []*gows.ChatDeletedEvent) {
	for _, event := range events {
		select {
		case listener.events <- event:
		default:
			return
		}
	}
}

func (result *deleteChatResult) toResponse() *__.DeleteChatResponse {
	response := &__.DeleteChatResponse{Confirmed: result.Confirmed, Purge: result.Purge}
	for _, address := range result.Addresses {
		item := &__.DeleteChatRange{
			Jid:                  address.JID.String(),
			LastMessageTimestamp: result.End.Unix(),
		}
		if address.Last != nil {
			item.LastMessageId = address.Last.Info.ID
			item.LastMessageFromMe = address.Last.Info.IsFromMe
			item.LastMessageSender = address.Last.Info.Sender.ToNonAD().String()
			item.LastMessageSentAt = address.Last.Info.Timestamp.Unix()
		}
		response.Ranges = append(response.Ranges, item)
	}
	return response
}

// chatDeletion is what deleting a chat needs from a session.
type chatDeletion struct {
	messages storage.MessageStorage
	// altJID gives the other address of a direct chat (gows.AltChatJID)
	altJID func(ctx context.Context, jid types.JID) (types.JID, error)
	// lidFirst tells that the phone keeps direct chats under their LID
	lidFirst bool
	// inGroup tells whether this account is still a member of the group
	inGroup func(ctx context.Context, jid types.JID) (bool, error)
	send    func(ctx context.Context, patch appstate.PatchInfo) error
	// refetch reads the app state again in the background
	refetch func()
	now     func() time.Time
	// request records the purge asked for the deletion (gows.RequestChatDeletion)
	request func(jids []types.JID, deletedAt time.Time, purge bool) error
	// purgeByDefault tells whether deleted chats are purged without a request
	purgeByDefault bool
}

func newChatDeletion(cli *gows.GoWS) *chatDeletion {
	return &chatDeletion{
		messages: cli.Storage.Messages,
		altJID:   cli.AltChatJID,
		lidFirst: cli.Store != nil && cli.Store.LIDMigrationTimestamp > 0,
		inGroup: func(ctx context.Context, jid types.JID) (bool, error) {
			return inGroup(cli.GetGroupInfo(ctx, jid))
		},
		send:           cli.SendAppState,
		refetch:        func() { go refetchAppState(cli) },
		now:            time.Now,
		request:        cli.RequestChatDeletion,
		purgeByDefault: cli.PurgesDeletedChats(),
	}
}

// deleteChatAddress is an address the deletion of a chat is sent to.
type deleteChatAddress struct {
	JID types.JID
	// Last is the last message stored under JID itself, nil if there is none
	Last *storage.StoredMessage
}

// deleteChatResult is what a deletion covered.
type deleteChatResult struct {
	// Addresses get one mutation each, the address the phone uses first
	Addresses []deleteChatAddress
	// End is the end of the message range of every mutation
	End time.Time
	// Confirmed is false when WhatsApp accepted the patch but reading it back failed
	Confirmed bool
	// Purge tells whether the chat is cleared from storage when the deletion comes back
	Purge bool
}

// run sends one mutation per address of the chat (phone number and LID), in one patch.
// Every range ends at the last message of the whole chat, with the key of the last message under its own address.
// purge, when set, is recorded before sending and overrides the session setting for this deletion.
func (d *chatDeletion) run(ctx context.Context, jid types.JID, deleteMedia bool, purge *bool) (*deleteChatResult, error) {
	if jid.Server == types.GroupServer {
		// The phone only offers "Delete group" once the group is left
		member, err := d.inGroup(ctx, jid)
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "failed to check whether this account is still in group %s: %v", jid, err)
		}
		if member {
			return nil, status.Errorf(codes.FailedPrecondition, "this account is still in group %s, leave the group before deleting it", jid)
		}
	}
	jids := []types.JID{jid}
	if jid.Server == types.DefaultUserServer || jid.Server == types.HiddenUserServer {
		alt, err := d.altJID(ctx, jid)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to get the other address of chat %s: %v", jid, err)
		}
		if !alt.IsEmpty() {
			jids = append(jids, alt)
		}
	}
	if len(jids) == 2 && (jids[1].Server == types.HiddenUserServer) == d.lidFirst {
		jids[0], jids[1] = jids[1], jids[0]
	}

	result := &deleteChatResult{}
	for _, address := range jids {
		last, err := d.lastMessage(address)
		if err != nil {
			return nil, err
		}
		result.Addresses = append(result.Addresses, deleteChatAddress{JID: address, Last: last})
		if last != nil && last.Info.Timestamp.After(result.End) {
			result.End = last.Info.Timestamp
		}
	}
	if result.End.IsZero() {
		// Nothing stored: the range ends now, without a key
		result.End = d.now()
	}

	patch := buildDeleteChatPatch(result, deleteMedia)
	patch.Timestamp = d.now()
	result.Purge = d.purgeByDefault
	if purge != nil {
		jids := make([]types.JID, 0, len(result.Addresses))
		for _, address := range result.Addresses {
			jids = append(jids, address.JID)
		}
		// Kept even if sending fails: an unused request never matches another deletion
		if err := d.request(jids, patch.Timestamp, *purge); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to record the purge of chat %s: %v", jid, err)
		}
		result.Purge = *purge
	}

	err := d.send(ctx, patch)
	switch {
	case err == nil:
		result.Confirmed = true
	case sentButNotFetched(err):
		// Already deleted on WhatsApp, retrying would send it twice
		d.refetch()
	default:
		return nil, err
	}
	return result, nil
}

// appStateFetchAfterSendError is the SendAppState error for a patch accepted but not read back
const appStateFetchAfterSendError = "failed to fetch app state after sending update"

func sentButNotFetched(err error) bool {
	return err != nil && strings.Contains(err.Error(), appStateFetchAfterSendError)
}

// refetchAppStateDelays are the waits before each new read of the app state
var refetchAppStateDelays = []time.Duration{10 * time.Second, time.Minute, 5 * time.Minute}

// refetchAppState reads regular_high again until it works, so the sent mutation comes back
func refetchAppState(cli *gows.GoWS) {
	for _, delay := range refetchAppStateDelays {
		select {
		case <-cli.Context.Done():
			return
		case <-time.After(delay):
		}
		err := cli.FetchAppState(cli.Context, appstate.WAPatchRegularHigh, false, false)
		if err == nil {
			cli.Log.Infof("Read the app state again after a deleted chat")
			return
		}
		cli.Log.Warnf("Failed to read the app state again after a deleted chat: %v", err)
	}
	cli.Log.Errorf("Gave up reading the app state after a deleted chat, the local copy is cleared on the next app state sync")
}

// inGroup tells from a group info query whether the account is still in the group (403 and 404: no)
func inGroup(info *types.GroupInfo, err error) (bool, error) {
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, whatsmeow.ErrNotInGroup), errors.Is(err, whatsmeow.ErrGroupNotFound):
		return false, nil
	default:
		return false, err
	}
}

// lastMessage returns the last message stored under jid itself, nil if none
func (d *chatDeletion) lastMessage(jid types.JID) (*storage.StoredMessage, error) {
	messages, err := d.messages.GetChatMessages(
		jid,
		storage.MessageFilter{},
		storage.Pagination{Limit: 1, Offset: 0},
		false,
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get the last message of chat %s: %v", jid, err)
	}
	if len(messages) == 0 || messages[0] == nil || messages[0].Message == nil {
		return nil, nil
	}
	return messages[0], nil
}

// buildDeleteChatPatch builds one deleteChat mutation per address, in one patch
func buildDeleteChatPatch(result *deleteChatResult, deleteMedia bool) appstate.PatchInfo {
	patch := appstate.PatchInfo{Type: appstate.WAPatchRegularHigh}
	for _, address := range result.Addresses {
		one := appstate.BuildDeleteChat(address.JID, result.End, nil, deleteMedia)
		if key := deleteChatMessageKey(address.Last); key != nil {
			messageRange := one.Mutations[0].Value.GetDeleteChatAction().GetMessageRange()
			messageRange.Messages = []*waSyncAction.SyncActionMessage{{
				Key:       key,
				Timestamp: proto.Int64(address.Last.Info.Timestamp.Unix()),
			}}
		}
		patch.Mutations = append(patch.Mutations, one.Mutations...)
	}
	return patch
}

// deleteChatMessageKey returns the key of a stored message, with the sender of others' group messages (as Baileys)
func deleteChatMessageKey(last *storage.StoredMessage) *waCommon.MessageKey {
	if last == nil || last.Message == nil {
		return nil
	}
	key := &waCommon.MessageKey{
		RemoteJID: proto.String(last.Info.Chat.ToNonAD().String()),
		FromMe:    proto.Bool(last.Info.IsFromMe),
		ID:        proto.String(last.Info.ID),
	}
	if last.Info.IsGroup && !last.Info.IsFromMe && !last.Info.Sender.IsEmpty() {
		key.Participant = proto.String(last.Info.Sender.ToNonAD().String())
	}
	return key
}

var (
	// deleteChatUser is the user part of a direct chat: digits, with an optional device
	deleteChatUser = regexp.MustCompile(`^\d+(:\d+)?$`)
	// deleteChatGroup is the user part of a group: digits, or "creator-timestamp" for old groups
	deleteChatGroup = regexp.MustCompile(`^\d+(-\d+)?$`)
)

// parseDeleteChatJID accepts direct chats and groups only. A deletion stays in the app state,
// so a malformed id is rejected rather than normalized
func parseDeleteChatJID(raw string) (types.JID, error) {
	user, server, ok := strings.Cut(raw, "@")
	if !ok || strings.Contains(server, "@") {
		return types.JID{}, status.Errorf(codes.InvalidArgument, "invalid chat jid %q: expected user@server", raw)
	}
	var userPattern *regexp.Regexp
	switch server {
	case types.DefaultUserServer, types.HiddenUserServer:
		userPattern = deleteChatUser
	case types.GroupServer:
		userPattern = deleteChatGroup
	default:
		return types.JID{}, status.Errorf(codes.InvalidArgument, "deleting chat %q is not supported, only direct chats and groups can be deleted", raw)
	}
	if !userPattern.MatchString(user) {
		return types.JID{}, status.Errorf(codes.InvalidArgument, "invalid chat jid %q", raw)
	}
	jid, err := types.ParseJID(raw)
	if err != nil {
		return types.JID{}, status.Errorf(codes.InvalidArgument, "invalid chat jid %q: %v", raw, err)
	}
	return jid.ToNonAD(), nil
}
