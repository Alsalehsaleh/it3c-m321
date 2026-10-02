package ch.benedict.m321.batchwriter;

import ch.benedict.m321.batchwriter.config.QueueNames;
import ch.benedict.m321.batchwriter.dto.ChatMessage;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.QueueInformation;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Gemeinsame Grundlage aller Integrationstests.
 *
 * Alle Tests, die von dieser Klasse erben, teilen sich EINEN Spring-Kontext und damit
 * dieselben Container. Das spart pro Testklasse das Hochfahren von RabbitMQ und Postgres.
 * Weil der Kontext geteilt ist, arbeitet jeder Test in einem eigenen, zufälligen Raum
 * und zählt nur dessen Zeilen.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
public abstract class IntegrationTestBase {

    /** So lange wartet ein Test höchstens auf seine Zeilen — dieselbe Grenze wie in S3. */
    private static final int MAX_WAIT_SECONDS = 60;

    /** Direkter Zugang zur Datenbank, um nachzuzählen, was der Dienst geschrieben hat. */
    @Autowired
    protected JdbcTemplate jdbcTemplate;

    /** Legt Nachrichten in die Queue, so wie der chat-service es tut. */
    @Autowired
    protected RabbitTemplate rabbitTemplate;

    /** Fragt den Broker, wie viele Nachrichten in einer Queue liegen. */
    @Autowired
    protected AmqpAdmin amqpAdmin;

    /** Legt Nachrichten für einen Raum als JSON in chat.persist. */
    protected void sendMessages(UUID roomId, int count) {
        List<ChatMessage> messages = createMessages(roomId, count);
        for (ChatMessage message : messages) {
            rabbitTemplate.convertAndSend(QueueNames.PERSIST_QUEUE, message);
        }
    }

    /**
     * Wartet, bis im Raum mindestens so viele Zeilen stehen wie erwartet, höchstens 60 s.
     *
     * Der batch-writer schreibt in seinem eigenen Thread. Der Test kann deshalb nicht
     * sofort nachzählen, sondern fragt einmal pro Sekunde nach.
     */
    protected long waitForRowsInRoom(UUID roomId, long expectedRows) throws InterruptedException {
        for (int second = 0; second < MAX_WAIT_SECONDS; second++) {
            long rows = countRowsInRoom(roomId);
            if (rows >= expectedRows) {
                return rows;
            }
            Thread.sleep(1000);
        }
        return countRowsInRoom(roomId);
    }

    /** Wie viele Nachrichten in einer Queue auf Abholung warten. */
    protected int messagesWaitingIn(String queueName) {
        QueueInformation queueInformation = amqpAdmin.getQueueInfo(queueName);
        return queueInformation.getMessageCount();
    }

    /** Zählt die Zeilen eines Raums. Jeder Test benutzt seinen eigenen Raum. */
    protected long countRowsInRoom(UUID roomId) {
        Long rows = jdbcTemplate.queryForObject(
                "select count(*) from message where room_id = ?", Long.class, roomId);
        return rows;
    }

    /** Baut eine vollständige Nachricht, so wie der chat-service sie erzeugt. */
    protected ChatMessage createMessage(UUID roomId, String content) {
        UUID messageId = UUID.randomUUID();
        Instant sentAt = Instant.now();
        return new ChatMessage(messageId, roomId, "anna", "Anna Muster", content, sentAt);
    }

    /** Baut so viele Nachrichten für einen Raum, wie ein Test braucht. */
    protected List<ChatMessage> createMessages(UUID roomId, int count) {
        List<ChatMessage> messages = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            ChatMessage message = createMessage(roomId, "Nachricht " + i);
            messages.add(message);
        }
        return messages;
    }
}
