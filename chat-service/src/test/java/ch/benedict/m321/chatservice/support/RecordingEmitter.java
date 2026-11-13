package ch.benedict.m321.chatservice.support;

import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Ein SseEmitter für Tests, der mitschreibt, was gesendet wird.
 *
 * So lässt sich RoomStream ohne HTTP und ohne Server prüfen. Bewusst eine
 * Unterklasse und kein Interface: CLAUDE.md verbietet ein Interface, das nur
 * eine einzige echte Implementierung hätte.
 */
public class RecordingEmitter extends SseEmitter {

    private final List<String> sentEvents = new ArrayList<>();

    private boolean failing;

    /** Ohne Zeitlimit, wie die echten Streams im StreamController. */
    public RecordingEmitter() {
        super(0L);
    }

    /**
     * Schreibt das Ereignis als Text mit, statt es über HTTP zu schicken.
     * Ist der Emitter auf "kaputt" gestellt, wirft er dieselbe Exception wie
     * eine abgerissene Verbindung.
     */
    @Override
    public synchronized void send(SseEventBuilder builder) throws IOException {
        if (failing) {
            throw new IOException("Simulated broken connection");
        }
        Set<DataWithMediaType> parts = builder.build();
        StringBuilder eventText = new StringBuilder();
        for (DataWithMediaType part : parts) {
            eventText.append(part.getData());
        }
        sentEvents.add(eventText.toString());
    }

    /** Stellt den Emitter auf "Verbindung abgerissen" oder wieder auf "in Ordnung". */
    public synchronized void setFailing(boolean failing) {
        this.failing = failing;
    }

    /** Eine Kopie, damit der Test nicht über eine Liste läuft, die sich gerade ändert. */
    public synchronized List<String> sentEvents() {
        return new ArrayList<>(sentEvents);
    }
}
