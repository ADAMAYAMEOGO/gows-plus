-- Deleted chats the client has not confirmed yet (AckChatDeleted)
CREATE TABLE IF NOT EXISTS gows_chat_deletions
(
    id VARCHAR(100) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    -- The event as sent to the client (JSON)
    data TEXT NOT NULL,
    PRIMARY KEY (id)
);
