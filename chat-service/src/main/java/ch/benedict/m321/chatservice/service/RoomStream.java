package ch.benedict.m321.chatservice.service;

import ch.benedict.m321.chatservice.dto.BufferedMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Ein Raum aus Sicht einer Instanz: der Puffer seiner letzten Nachrichten und
 * die Liste der offenen Streams, die ihm gerade zuhören.
 *
 * Alle öffentlichen Methoden sind synchronized. Das ist das Schloss pro Raum
 * aus Spec 3.2.5: Puffer und Liste werden nie von zwei Threads gleichzeitig
 * verändert, und andere Räume warten nicht mit.
 */
@Slf4j
public class RoomStream {

    /** So viele Nachrichten hält der Puffer pro Raum. Begründung: Spec 3.2.5. */
    static final int BUFFER_SIZE = 200;

    private final Deque<BufferedMessage> buffer = new ArrayDeque<>();

    private final List<SseEmitter> emitters = new ArrayList<>();

    /**
     * Legt die Nachricht in den Puffer und schickt sie an alle offenen Streams.
     *
     * Gesendet wird über eine Kopie der Liste: Scheitert ein Stream, wird er
     * mitten in der Schleife ausgetragen. Das Schloss hält den eigenen Thread
     * nicht auf, ohne Kopie gäbe es deshalb eine ConcurrentModificationException.
     */
    public synchronized void publish(BufferedMessage message) {
        addToBuffer(message);
        List<SseEmitter> receivers = new ArrayList<>(emitters);
        for (SseEmitter emitter : receivers) {
            sendMessage(emitter, message);
        }
    }

    /** Meldet einen Stream an. Ab jetzt bekommt er jede neue Nachricht des Raums. */
    public synchronized void subscribe(SseEmitter emitter) {
        emitters.add(emitter);
    }

    /**
     * Trägt einen Stream aus. Wird auch aufgerufen, wenn er schon ausgetragen
     * ist, zum Beispiel aus den Rückrufen des SseEmitter. Dann passiert nichts.
     */
    public synchronized void unsubscribe(SseEmitter emitter) {
        emitters.remove(emitter);
    }

    /**
     * Eine Kopie des Puffers, älteste Nachricht zuerst. Nur zum Nachsehen,
     * etwa im Test: Wer die Kopie verändert, verändert den Puffer nicht.
     */
    synchronized List<BufferedMessage> copyOfBuffer() {
        return new ArrayList<>(buffer);
    }

    /** Hängt die Nachricht hinten an und wirft vorne die älteste raus, wenn der Puffer voll ist. */
    private void addToBuffer(BufferedMessage message) {
        buffer.addLast(message);
        if (buffer.size() > BUFFER_SIZE) {
            buffer.removeFirst();
        }
    }

    /**
     * Schickt eine Nachricht an einen Stream: id ist die id der Nachricht,
     * data der unveränderte Body.
     *
     * Ein Fehler betrifft nur diesen einen Stream. Er wird ausgetragen, und
     * keine Exception verlässt die Methode. Sonst würde der Listener die
     * Nachricht zurück in die Queue legen und alle anderen Streams bekämen
     * sie nie (Spec 3.2.6).
     */
    private void sendMessage(SseEmitter emitter, BufferedMessage message) {
        String eventId = message.id().toString();
        SseEmitter.SseEventBuilder event = SseEmitter.event();
        event.id(eventId);
        event.data(message.body());
        try {
            emitter.send(event);
        } catch (IOException | IllegalStateException exception) {
            log.info("Removing stream after failed send: {}", exception.getMessage());
            emitters.remove(emitter);
        }
    }
}
