package ch.benedict.m321.batchwriter.listener;

import ch.benedict.m321.batchwriter.IntegrationTestBase;
import ch.benedict.m321.batchwriter.config.QueueNames;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Spec 3.2.3 im Test: Eine kaputte Nachricht landet einzeln in chat.dlq, die gültigen
 * Nachrichten desselben Stapels werden normal geschrieben.
 *
 * "Kaputt" gibt es in zwei Formen: kein gültiges JSON, oder gültiges JSON, dem ein Feld
 * fehlt. Beide scheitern schon beim Einlesen, weil ChatMessage im Konstruktor prüft.
 */
class BrokenMessageIntegrationTest extends IntegrationTestBase {

    /** So lange darf der Broker brauchen, um abgelehnte Nachrichten in die DLQ umzuleiten. */
    private static final int MAX_WAIT_SECONDS = 10;

    /** Vier Nachrichten im selben Stapel: zwei gültige in die Tabelle, zwei kaputte in die DLQ. */
    @Test
    void brokenMessagesGoToTheDeadLetterQueueOneByOne() throws InterruptedException {
        UUID roomId = UUID.randomUUID();
        UUID brokenMessageId = UUID.randomUUID();
        String jsonWithoutContent = """
                {"id":"%s",
                 "roomId":"%s",
                 "senderId":"anna",
                 "senderName":"Anna Muster",
                 "sentAt":"2026-09-25T09:14:02.471Z"}
                """.formatted(brokenMessageId, roomId);

        sendMessages(roomId, 1);
        sendJson(jsonWithoutContent);
        sendJson("das ist kein JSON");
        sendMessages(roomId, 1);

        long rows = waitForRowsInRoom(roomId, 2);
        int deadLetters = waitForDeadLetters(2);
        assertEquals(2, rows);
        assertEquals(2, deadLetters);
    }

    /**
     * Wartet, bis die DLQ die erwartete Zahl Nachrichten enthält, höchstens 10 s.
     *
     * Abgelehnte Nachrichten leitet der Broker selbst in die DLQ um. Das geschieht neben
     * dem Schreiben her und kann einen Moment dauern.
     */
    private int waitForDeadLetters(int expectedDeadLetters) throws InterruptedException {
        for (int second = 0; second < MAX_WAIT_SECONDS; second++) {
            int deadLetters = messagesWaitingIn(QueueNames.DEAD_LETTER_QUEUE);
            if (deadLetters >= expectedDeadLetters) {
                return deadLetters;
            }
            Thread.sleep(1000);
        }
        return messagesWaitingIn(QueueNames.DEAD_LETTER_QUEUE);
    }
}
