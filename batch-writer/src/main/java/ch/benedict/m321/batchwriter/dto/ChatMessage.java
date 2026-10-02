package ch.benedict.m321.batchwriter.dto;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Eine Nachricht, wie sie in chat.persist ankommt — die eigene Kopie des batch-writer.
 *
 * Der Vertrag zwischen den Diensten ist das JSON, nicht eine gemeinsame Klasse. Der
 * chat-service hat seine Fassung, wir haben unsere. Nur Feldnamen und Typen müssen
 * zusammenpassen (Spec Kap. 2).
 */
public record ChatMessage(
        UUID id,
        UUID roomId,
        String senderId,
        String senderName,
        String content,
        Instant sentAt) {

    /**
     * Prüft beim Einlesen, dass kein Feld fehlt.
     *
     * Jackson ruft diesen Konstruktor für jede Nachricht auf. Fehlt ein Feld, scheitert
     * schon das Einlesen, und die Nachricht geht einzeln in die DLQ. Ohne diese Prüfung
     * scheiterte sie erst in der Datenbank an NOT NULL — und dort würde der Dienst sie
     * als Ausfall behandeln und endlos wiederholen (Spec 3.2.3).
     */
    public ChatMessage {
        Objects.requireNonNull(id, "id is missing");
        Objects.requireNonNull(roomId, "roomId is missing");
        Objects.requireNonNull(senderId, "senderId is missing");
        Objects.requireNonNull(senderName, "senderName is missing");
        Objects.requireNonNull(content, "content is missing");
        Objects.requireNonNull(sentAt, "sentAt is missing");
    }
}
