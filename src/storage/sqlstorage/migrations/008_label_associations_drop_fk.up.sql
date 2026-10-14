-- Drop the foreign key: an association can arrive before its label, like in the web client (#2290)
CREATE TABLE gows_label_associations_v2
(
    -- JID (chat or contact)
    jid VARCHAR(100) NOT NULL,
    -- Label ID
    label_id VARCHAR(100) NOT NULL,
    -- Association data (JSON)
    data TEXT NOT NULL,
    -- Primary key constraint on jid and label_id
    PRIMARY KEY (jid, label_id)
);

INSERT INTO gows_label_associations_v2 (jid, label_id, data)
SELECT
    jid,
    label_id,
    data
FROM gows_label_associations;

DROP TABLE gows_label_associations;

ALTER TABLE gows_label_associations_v2 RENAME TO gows_label_associations;

-- Index for jid (useful for retrieving labels for a chat)
CREATE INDEX gows_label_associations_jid_idx ON gows_label_associations (jid);

-- Index for label_id (useful for retrieving chats with a specific label)
CREATE INDEX gows_label_associations_label_id_idx ON gows_label_associations (label_id);
