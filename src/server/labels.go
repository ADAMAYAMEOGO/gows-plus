package server

import (
	"context"
	"fmt"
	"github.com/devlikeapro/gows/storage"
	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/types"

	__ "github.com/devlikeapro/gows/proto"
)

// GetLabels implements the gRPC method to retrieve all labels for a session
func (s *Server) GetLabels(ctx context.Context, req *__.GetLabelsRequest) (*__.JsonList, error) {
	cli, err := s.Sm.Get(req.Session.Id)
	if err != nil {
		return nil, err
	}

	labels, err := cli.Storage.Labels.GetAllLabels()
	if err != nil {
		return nil, fmt.Errorf("error getting labels: %w", err)
	}

	response, err := toJsonList(labels)
	if err != nil {
		return nil, fmt.Errorf("error marshaling labels: %w", err)
	}

	return response, nil
}

// UpsertLabel implements the gRPC method to create or update a label
func (s *Server) UpsertLabel(ctx context.Context, req *__.UpsertLabelRequest) (*__.Empty, error) {
	cli, err := s.Sm.Get(req.Session.Id)
	if err != nil {
		return nil, err
	}

	// Send app state update for label edit
	label := req.Label
	patch := appstate.BuildLabelEdit(label.Id, label.Name, label.Color, false)
	err = cli.SendAppState(ctx, patch)
	if err != nil {
		return nil, fmt.Errorf("error upserting label: %w", err)
	}
	return &__.Empty{}, nil
}

// DeleteLabel implements the gRPC method to delete a label
func (s *Server) DeleteLabel(ctx context.Context, req *__.DeleteLabelRequest) (*__.Empty, error) {
	cli, err := s.Sm.Get(req.Session.Id)
	if err != nil {
		return nil, err
	}

	// Send app state update for label delete
	label := req.Label
	patch := appstate.BuildLabelEdit(label.Id, label.Name, label.Color, true)
	err = cli.SendAppState(ctx, patch)
	if err != nil {
		return nil, fmt.Errorf("error deleting label: %w", err)
	}
	return &__.Empty{}, nil
}

func (s *Server) GetLabelsByJid(ctx context.Context, req *__.EntityByIdRequest) (*__.JsonList, error) {
	cli, err := s.Sm.Get(req.Session.Id)
	if err != nil {
		return nil, err
	}
	jid, err := types.ParseJID(req.Id)
	if err != nil {
		return nil, fmt.Errorf("error parsing jid: %w", err)
	}
	labelIds, err := cli.Storage.LabelAssociations.GetLabelIDsByJID(jid)
	if err != nil {
		return nil, fmt.Errorf("error getting labels by jid: %w", err)
	}
	labels, err := cli.Storage.Labels.GetAllLabels()
	if err != nil {
		return nil, fmt.Errorf("error getting labels: %w", err)
	}
	labelById := make(map[string]*storage.Label)
	for _, label := range labels {
		labelById[label.ID] = label
	}
	labelsForJid := make([]*storage.Label, 0)
	for _, labelId := range labelIds {
		// Skip associations whose label is not synced yet
		label, ok := labelById[labelId]
		if !ok {
			continue
		}
		labelsForJid = append(labelsForJid, label)
	}
	response, err := toJsonList(labelsForJid)
	if err != nil {
		return nil, fmt.Errorf("error marshaling labels: %w", err)
	}
	return response, nil
}
func (s *Server) GetChatsByLabelId(ctx context.Context, req *__.EntityByIdRequest) (*__.JsonList, error) {
	cli, err := s.Sm.Get(req.Session.Id)
	if err != nil {
		return nil, err
	}
	labelId := req.Id
	chats, err := cli.Storage.LabelAssociations.GetJIDsByLabelID(labelId)
	if err != nil {
		return nil, fmt.Errorf("error getting chats by label id: %w", err)
	}
	response, err := toJsonList(chats)
	if err != nil {
		return nil, fmt.Errorf("error marshaling chats: %w", err)
	}
	return response, nil

}

func (s *Server) AddChatLabel(ctx context.Context, req *__.ChatLabelRequest) (*__.Empty, error) {
	cli, err := s.Sm.Get(req.Session.Id)
	if err != nil {
		return nil, err
	}
	jid, err := types.ParseJID(req.ChatId)
	patch := appstate.BuildLabelChat(jid, req.LabelId, true)
	err = cli.SendAppState(ctx, patch)
	if err != nil {
		return nil, fmt.Errorf("error adding label to chat: %w", err)
	}
	return &__.Empty{}, nil
}

func (s *Server) RemoveChatLabel(ctx context.Context, req *__.ChatLabelRequest) (*__.Empty, error) {
	cli, err := s.Sm.Get(req.Session.Id)
	if err != nil {
		return nil, err
	}
	jid, err := types.ParseJID(req.ChatId)
	patch := appstate.BuildLabelChat(jid, req.LabelId, false)
	err = cli.SendAppState(ctx, patch)
	if err != nil {
		return nil, fmt.Errorf("error removing label from chat: %w", err)
	}
	return &__.Empty{}, nil
}
