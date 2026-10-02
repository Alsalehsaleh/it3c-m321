package ch.benedict.m321.batchwriter.listener;

import ch.benedict.m321.batchwriter.IntegrationTestBase;
import ch.benedict.m321.batchwriter.config.QueueNames;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Szenario S5 im Test: Dieselbe Nachricht zweimal ergibt genau eine Zeile, und nichts
 * landet in der DLQ.
 *
 * Die Nachricht kommt wie beim Lehrer als rohes JSON, nur mit content_type. Damit prüft
 * der Test zugleich, dass der Dienst den Header __TypeId__ nicht braucht (Spec 2.4).
 */
class DuplicateMessageIntegrationTest extends IntegrationTestBase {

    /** Zweimal dieselbe id: eine Zeile, keine Ablehnung. */
    @Test
    void sameMessageTwiceGivesOneRow() throws InterruptedException {
        UUID messageId = UUID.randomUUID();
        UUID roomId = UUID.randomUUID();
        String json = """
                {"id":"%s",
                 "roomId":"%s",
                 "senderId":"anna",
                 "senderName":"Anna Muster",
                 "content":"Doppelt",
                 "sentAt":"2026-09-25T09:14:02.471Z"}
                """.formatted(messageId, roomId);

        sendJson(json);
        sendJson(json);
        // Eine Markierung hinterher: Die Queue liefert der Reihe nach aus. Steht die
        // Markierung in der Tabelle, sind beide Kopien sicher schon verarbeitet.
        sendMessages(roomId, 1);

        long rowsInRoom = waitForRowsInRoom(roomId, 2);
        Long rowsWithId = jdbcTemplate.queryForObject(
                "select count(*) from message where id = ?", Long.class, messageId);
        assertEquals(2, rowsInRoom);
        assertEquals(1, rowsWithId);
        assertEquals(0, messagesWaitingIn(QueueNames.DEAD_LETTER_QUEUE));
    }
}
