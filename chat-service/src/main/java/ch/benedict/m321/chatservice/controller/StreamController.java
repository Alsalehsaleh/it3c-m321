package ch.benedict.m321.chatservice.controller;

import ch.benedict.m321.chatservice.service.RoomStream;
import ch.benedict.m321.chatservice.service.RoomStreamRegistry;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.UUID;

/**
 * Der Anzeigeweg nach aussen: GET /stream?roomId=… öffnet einen
 * Server-Sent-Events-Stream, über den der Client jede Nachricht seines Raums
 * sofort bekommt (Spec 2.2).
 *
 * Gesendet wird hier nichts. Der Controller öffnet den Stream nur und meldet
 * ihn beim RoomStream des Raums an; zugestellt wird von dort.
 */
@RestController
@RequiredArgsConstructor
public class StreamController {

    /**
     * 0 heisst: Der Stream läuft nie ab. Ohne eigene Angabe gälte das
     * Async-Zeitlimit von Tomcat, und jeder Stream bräche nach 30 s ab (Spec 2.2).
     */
    private static final long NO_TIMEOUT = 0L;

    private final RoomStreamRegistry roomStreamRegistry;

    /**
     * Öffnet einen Stream für einen Raum.
     *
     * Fehlt roomId oder ist sie keine UUID, antwortet Spring selbst mit 400,
     * bevor diese Methode läuft. Der Header Last-Event-ID ist freiwillig: Der
     * Browser schickt ihn nur beim automatischen Neuverbinden (Spec 2.3).
     */
    @GetMapping(path = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter openStream(
            @RequestParam UUID roomId,
            @RequestHeader(name = "Last-Event-ID", required = false) String lastEventId) {
        SseEmitter emitter = new SseEmitter(NO_TIMEOUT);
        RoomStream roomStream = roomStreamRegistry.getOrCreate(roomId);
        registerCleanup(emitter, roomStream);
        roomStream.subscribe(emitter, lastEventId);
        return emitter;
    }

    /**
     * Trägt den Stream aus, sobald er endet: sauber beendet, abgelaufen oder
     * mit Fehler abgebrochen. Ohne diese Rückrufe blieben tote Streams in der
     * Liste des Raums liegen (Spec 3.2.6).
     */
    private void registerCleanup(SseEmitter emitter, RoomStream roomStream) {
        emitter.onCompletion(() -> roomStream.unsubscribe(emitter));
        emitter.onTimeout(() -> roomStream.unsubscribe(emitter));
        emitter.onError(error -> roomStream.unsubscribe(emitter));
    }
}
