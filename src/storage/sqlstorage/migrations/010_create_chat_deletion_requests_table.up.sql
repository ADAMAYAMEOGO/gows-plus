-- Whether to clear a chat this client is deleting (DeleteChatRequest.purge),
-- until the deletion comes back from the server
CREATE TABLE IF NOT EXISTS gows_chat_deletion_requests
(
    jid VARCHAR(100) NOT NULL,
    -- Timestamp of the app state mutation (unix milliseconds)
    deleted_at BIGINT NOT NULL,
    purge BOOLEAN NOT NULL,
    created_at TIMESTAMP NOT NULL,
    PRIMARY KEY (jid, deleted_at)
);
