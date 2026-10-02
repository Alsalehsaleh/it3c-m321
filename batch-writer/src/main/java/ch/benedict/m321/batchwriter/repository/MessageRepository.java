package ch.benedict.m321.batchwriter.repository;

import ch.benedict.m321.batchwriter.dto.ChatMessage;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

/**
 * Die einzige Stelle im ganzen System, die in die Tabelle message schreibt.
 *
 * Ein Stapel wird in EINER Transaktion geschrieben: ein vorbereitetes INSERT, das
 * JdbcTemplate für jede Nachricht einmal ausführt und gebündelt abschickt. Eine
 * Transaktion pro Stapel statt einer pro Nachricht — dafür gibt es den batch-writer
 * (Spec 1.1).
 */
@Repository
@RequiredArgsConstructor
public class MessageRepository {

    /**
     * ON CONFLICT (id) DO NOTHING macht ein Duplikat harmlos: Steht die id schon in der
     * Tabelle, passiert nichts — kein Fehler und keine zweite Zeile (Spec 3.2.2).
     */
    private static final String INSERT_MESSAGE = """
            INSERT INTO message (id, room_id, sender_id, sender_name, content, sent_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (id) DO NOTHING
            """;

    private final JdbcTemplate jdbcTemplate;

    /**
     * Schreibt alle Nachrichten eines Stapels und gibt zurück, wie viele davon neu waren.
     *
     * "@Transactional" heisst: Spring öffnet vor der Methode eine Transaktion und schliesst
     * sie danach mit COMMIT. Wirft die Methode eine Exception, gibt es ein ROLLBACK, und
     * keine Zeile des Stapels steht in der Tabelle.
     */
    @Transactional
    public int insertAll(List<ChatMessage> messages) {
        List<Object[]> rows = new ArrayList<>();
        for (ChatMessage message : messages) {
            Object[] row = toRow(message);
            rows.add(row);
        }

        int[] insertedPerRow = jdbcTemplate.batchUpdate(INSERT_MESSAGE, rows);

        int newRows = 0;
        for (int inserted : insertedPerRow) {
            newRows = newRows + inserted;
        }
        return newRows;
    }

    /**
     * Bringt eine Nachricht in die Reihenfolge der Spalten im INSERT.
     *
     * Der Zeitpunkt geht als OffsetDateTime in UTC an den Treiber. Damit ist eindeutig,
     * welcher Moment gemeint ist, egal in welcher Zeitzone der Container läuft.
     */
    private Object[] toRow(ChatMessage message) {
        OffsetDateTime sentAtUtc = message.sentAt().atOffset(ZoneOffset.UTC);
        return new Object[] {
                message.id(),
                message.roomId(),
                message.senderId(),
                message.senderName(),
                message.content(),
                sentAtUtc
        };
    }
}
