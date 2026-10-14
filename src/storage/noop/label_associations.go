package noop

import (
	"github.com/devlikeapro/gows/storage"
	"go.mau.fi/whatsmeow/types"
)

type LabelAssociationStorage struct{}

var _ storage.LabelAssociationStorage = (*LabelAssociationStorage)(nil)

func NewLabelAssociationStorage() *LabelAssociationStorage {
	return &LabelAssociationStorage{}
}

func (s LabelAssociationStorage) GetJIDsByLabelID(labelID string) ([]types.JID, error) {
	return []types.JID{}, nil
}

func (s LabelAssociationStorage) GetLabelIDsByJID(jid types.JID) ([]string, error) {
	return []string{}, nil
}

func (s LabelAssociationStorage) AddAssociation(jid types.JID, labelID string) error {
	return nil
}

func (s LabelAssociationStorage) RemoveAssociation(jid types.JID, labelID string) error {
	return nil
}

func (s LabelAssociationStorage) RemoveAssociationsByLabelID(labelID string) error {
	return nil
}
