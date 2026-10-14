package sqlstorage

import (
	sq "github.com/Masterminds/squirrel"
	"github.com/devlikeapro/gows/storage"
	"go.mau.fi/whatsmeow/types"
)

type SqlLabelAssociationStore struct {
	*EntityRepository[storage.LabelAssociation]
}

var _ storage.LabelAssociationStorage = (*SqlLabelAssociationStore)(nil)

func (gc *GContainer) NewLabelAssociationStorage() *SqlLabelAssociationStore {
	repo := NewEntityRepository[storage.LabelAssociation](
		gc.db,
		LabelAssociationsTable,
		labelAssociationMapper,
	)
	return &SqlLabelAssociationStore{
		repo,
	}
}

func (s SqlLabelAssociationStore) GetJIDsByLabelID(labelID string) ([]types.JID, error) {
	conditions := []sq.Sqlizer{
		sq.Eq{"label_id": labelID},
	}

	associations, err := s.FilterBy(conditions, nil, storage.Pagination{})
	if err != nil {
		return nil, err
	}

	jids := make([]types.JID, 0, len(associations))
	for _, association := range associations {
		jids = append(jids, association.JID)
	}

	return jids, nil
}

func (s SqlLabelAssociationStore) GetLabelIDsByJID(jid types.JID) ([]string, error) {
	conditions := []sq.Sqlizer{
		sq.Eq{"jid": jid},
	}

	associations, err := s.FilterBy(conditions, nil, storage.Pagination{})
	if err != nil {
		return nil, err
	}

	labelIDs := make([]string, 0, len(associations))
	for _, association := range associations {
		labelIDs = append(labelIDs, association.LabelID)
	}

	return labelIDs, nil
}

func (s SqlLabelAssociationStore) AddAssociation(jid types.JID, labelID string) error {
	association := &storage.LabelAssociation{
		JID:     jid,
		LabelID: labelID,
	}

	return s.UpsertOne(association)
}

func (s SqlLabelAssociationStore) RemoveAssociation(jid types.JID, labelID string) error {
	conditions := []sq.Sqlizer{
		sq.Eq{"jid": jid},
		sq.Eq{"label_id": labelID},
	}

	return s.DeleteBy(conditions)
}

func (s SqlLabelAssociationStore) RemoveAssociationsByLabelID(labelID string) error {
	conditions := []sq.Sqlizer{
		sq.Eq{"label_id": labelID},
	}

	return s.DeleteBy(conditions)
}
