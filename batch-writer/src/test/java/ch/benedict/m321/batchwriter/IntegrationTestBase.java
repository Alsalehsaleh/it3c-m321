package ch.benedict.m321.batchwriter;

import ch.benedict.m321.batchwriter.dto.ChatMessage;
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

    /** Direkter Zugang zur Datenbank, um nachzuzählen, was der Dienst geschrieben hat. */
    @Autowired
    protected JdbcTemplate jdbcTemplate;

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
