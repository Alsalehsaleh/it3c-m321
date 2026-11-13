package ch.benedict.m321.chatservice.service;

import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Schickt alle 15 s ein Lebenszeichen an alle offenen Streams dieser Instanz.
 *
 * Zwei Gründe (Spec 2.2): Der Dienst merkt nur beim Schreiben, dass ein
 * Client weg ist, und Zwischenstationen wie später das Gateway schliessen
 * Verbindungen, über die lange nichts fliesst.
 */
@Component
@RequiredArgsConstructor
public class HeartbeatSender {

    /** Abstand der Lebenszeichen. Deutlich unter den üblichen Leerlaufgrenzen von Proxies. */
    private static final long HEARTBEAT_INTERVAL_MILLIS = 15000;

    private final RoomStreamRegistry roomStreamRegistry;

    /** Läuft nach Zeitplan und geht Raum für Raum durch. */
    @Scheduled(fixedRate = HEARTBEAT_INTERVAL_MILLIS)
    public void sendHeartbeats() {
        List<RoomStream> roomStreams = roomStreamRegistry.copyOfAllRoomStreams();
        for (RoomStream roomStream : roomStreams) {
            roomStream.sendHeartbeat();
        }
    }
}
