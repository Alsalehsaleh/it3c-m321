package ch.benedict.m321.batchwriter.listener;

import ch.benedict.m321.batchwriter.IntegrationTestBase;
import ch.benedict.m321.batchwriter.config.QueueNames;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Szenario S3 im Test: Was in chat.persist liegt, steht kurz danach in der Tabelle.
 *
 * Das ist der Hauptweg des Dienstes — Queue, Stapel, INSERT, COMMIT, Bestätigung —
 * einmal ganz durch, mit echtem RabbitMQ und echtem Postgres.
 */
class PersistQueueIntegrationTest extends IntegrationTestBase {

    /** 1000 Nachrichten stehen nach spätestens 60 s in der Tabelle, und die Queue ist leer. */
    @Test
    void thousandMessagesEndUpInTheTable() throws InterruptedException {
        UUID roomId = UUID.randomUUID();

        sendMessages(roomId, 1000);

        long rows = waitForRowsInRoom(roomId, 1000);
        int waitingMessages = messagesWaitingIn(QueueNames.PERSIST_QUEUE);
        assertEquals(1000, rows);
        assertEquals(0, waitingMessages);
    }
}
