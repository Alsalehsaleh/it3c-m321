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
            boolean sent = trySend(emitter, message);
            if (!sent) {
                emitters.remove(emitter);
            }
        }
    }

    /**
     * Meldet einen Stream an: zuerst die verpassten Nachrichten nach
     * lastEventId aus dem Puffer, DANN in die Liste eintragen.
     *
     * Weil publish dasselbe Schloss hat, liegt jede Nachricht entweder schon
     * im Puffer und wird hier nachgeholt, oder sie kommt erst danach und geht
     * live an den eingetragenen Stream. Beides oder keins ist unmöglich
     * (Spec 3.2.5). Andersherum, erst eintragen und dann nachholen, könnte
     * eine Live-Nachricht vor den nachgeholten ankommen.
     */
    public synchronized void subscribe(SseEmitter emitter, String lastEventId) {
        List<BufferedMessage> missedMessages = messagesAfter(lastEventId);
        for (BufferedMessage message : missedMessages) {
            boolean sent = trySend(emitter, message);
            if (!sent) {
                return;
            }
        }
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
     * Wie viele Streams dem Raum gerade zuhören. Für Tests und zur Kontrolle,
     * ob ausgetragene Streams wirklich weg sind (Spec 3.2.6).
     */
    public synchronized int openStreamCount() {
        return emitters.size();
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
     * Alle Nachrichten im Puffer, die NACH der Nachricht mit lastEventId kamen.
     *
     * Fehlt die ID, ist sie leer oder steht sie nicht im Puffer (zu alt,
     * anderer Raum, Instanz neu gestartet, keine UUID), gibt es nichts
     * nachzuholen. Das ist kein Fehler: Ein Fehler würde den Browser für immer
     * aussperren, weil er mit derselben ID neu verbindet (Spec 2.3).
     */
    private List<BufferedMessage> messagesAfter(String lastEventId) {
        List<BufferedMessage> missedMessages = new ArrayList<>();
        if (lastEventId == null || lastEventId.isBlank()) {
            return missedMessages;
        }
        boolean found = false;
        for (BufferedMessage message : buffer) {
            if (found) {
                missedMessages.add(message);
            }
            String messageId = message.id().toString();
            if (messageId.equals(lastEventId)) {
                found = true;
            }
        }
        if (!found) {
            log.info("Last-Event-ID {} not in buffer, starting live", lastEventId);
        }
        return missedMessages;
    }

    /**
     * Schickt eine Nachricht an einen Stream: id ist die id der Nachricht,
     * data der unveränderte Body. Gibt zurück, ob es geklappt hat.
     *
     * Ein Fehler betrifft nur diesen einen Stream, und keine Exception
     * verlässt die Methode. Sonst würde der Listener die Nachricht zurück in
     * die Queue legen und alle anderen Streams bekämen sie nie (Spec 3.2.6).
     * Austragen muss der Aufrufer, denn nur er weiss, ob der Stream schon in
     * der Liste steht.
     */
    private boolean trySend(SseEmitter emitter, BufferedMessage message) {
        String eventId = message.id().toString();
        SseEmitter.SseEventBuilder event = SseEmitter.event();
        event.id(eventId);
        event.data(message.body());
        try {
            emitter.send(event);
            return true;
        } catch (IOException | IllegalStateException exception) {
            log.info("Dropping stream after failed send: {}", exception.getMessage());
            return false;
        }
    }
}
