-- Schema des batch-writer (Spec 4.1, Spalten aus PLANUNG.md §3.7).
--
-- Das offizielle Postgres-Image führt diese Datei beim ERSTEN Start mit leerem
-- Datenverzeichnis aus. Dieselbe Datei benutzen die Tests über Testcontainers,
-- es gibt also nur eine Fassung des Schemas.

CREATE TABLE IF NOT EXISTS message (
    id          uuid        PRIMARY KEY,   -- vom chat-service vergeben, trägt ON CONFLICT
    room_id     uuid        NOT NULL,      -- bewusst ohne Fremdschlüssel, siehe Spec 4.1
    sender_id   varchar     NOT NULL,      -- die sub-Kennung aus Keycloak
    sender_name varchar     NOT NULL,      -- denormalisiert, damit die Historie lesbar bleibt
    content     text        NOT NULL,
    sent_at     timestamptz NOT NULL       -- Serverzeit des chat-service
);

-- Die einzige geplante Leseabfrage: "die letzten 50 Nachrichten eines Raums".
CREATE INDEX IF NOT EXISTS idx_message_room_sent
    ON message (room_id, sent_at DESC);
