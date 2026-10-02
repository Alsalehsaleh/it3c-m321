package ch.benedict.m321.batchwriter.repository;

import ch.benedict.m321.batchwriter.IntegrationTestBase;
import ch.benedict.m321.batchwriter.dto.ChatMessage;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Prüft das Schreiben in die Datenbank, noch ohne Queue: ein Stapel wird vollständig
 * geschrieben, und ON CONFLICT macht Duplikate harmlos.
 */
class MessageRepositoryIntegrationTest extends IntegrationTestBase {

    @Autowired
    private MessageRepository messageRepository;

    /** Ein voller Stapel von 500 Nachrichten ergibt 500 neue Zeilen. */
    @Test
    void writesAllMessagesOfABatch() {
        UUID roomId = UUID.randomUUID();
        List<ChatMessage> batch = createMessages(roomId, 500);

        int newRows = messageRepository.insertAll(batch);

        long rowsInRoom = countRowsInRoom(roomId);
        assertEquals(500, newRows);
        assertEquals(500, rowsInRoom);
    }

    /** Jedes Feld landet in seiner Spalte, der Zeitpunkt ohne Verschiebung durch Zeitzonen. */
    @Test
    void storesEveryFieldOfAMessage() {
        UUID messageId = UUID.randomUUID();
        UUID roomId = UUID.randomUUID();
        Instant sentAt = Instant.parse("2026-09-30T16:31:38.474905Z");
        ChatMessage message = new ChatMessage(messageId, roomId, "anna", "Anna Muster", "Hallo Datenbank", sentAt);

        List<ChatMessage> batch = List.of(message);
        messageRepository.insertAll(batch);

        Map<String, Object> row = jdbcTemplate.queryForMap(
                "select room_id, sender_id, sender_name, content from message where id = ?", messageId);
        OffsetDateTime storedSentAt = jdbcTemplate.queryForObject(
                "select sent_at from message where id = ?", OffsetDateTime.class, messageId);
        assertEquals(roomId, row.get("room_id"));
        assertEquals("anna", row.get("sender_id"));
        assertEquals("Anna Muster", row.get("sender_name"));
        assertEquals("Hallo Datenbank", row.get("content"));
        assertEquals(sentAt, storedSentAt.toInstant());
    }

    /**
     * Die Messung, die Spec 3.2.2 ankündigt: Zwei gleiche id im selben Stapel ergeben eine
     * Zeile und keinen Fehler — auch ohne die Entdopplung im Listener.
     */
    @Test
    void sameIdTwiceInOneBatchGivesOneRow() {
        UUID roomId = UUID.randomUUID();
        ChatMessage original = createMessage(roomId, "Original");
        ChatMessage copy = new ChatMessage(original.id(), roomId, "anna", "Anna Muster", "Kopie", original.sentAt());
        List<ChatMessage> batch = List.of(original, copy);

        int newRows = messageRepository.insertAll(batch);

        long rowsInRoom = countRowsInRoom(roomId);
        assertEquals(1, newRows);
        assertEquals(1, rowsInRoom);
    }

    /** Eine id, die schon in der Tabelle steht, wird übergangen; gezählt werden nur neue Zeilen. */
    @Test
    void knownIdInLaterBatchIsSkipped() {
        UUID roomId = UUID.randomUUID();
        ChatMessage first = createMessage(roomId, "erste");
        ChatMessage second = createMessage(roomId, "zweite");
        List<ChatMessage> firstBatch = List.of(first);
        List<ChatMessage> laterBatch = List.of(first, second);
        messageRepository.insertAll(firstBatch);

        int newRows = messageRepository.insertAll(laterBatch);

        long rowsInRoom = countRowsInRoom(roomId);
        assertEquals(1, newRows);
        assertEquals(2, rowsInRoom);
    }
}
