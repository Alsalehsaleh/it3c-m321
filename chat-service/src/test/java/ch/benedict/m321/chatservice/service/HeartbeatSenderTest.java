package ch.benedict.m321.chatservice.service;

import ch.benedict.m321.chatservice.support.RecordingEmitter;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Prüft, dass der HeartbeatSender die Streams ALLER Räume erreicht. Die Methode
 * wird direkt aufgerufen: Der Test soll nicht 15 s auf den Zeitplan warten.
 */
class HeartbeatSenderTest {

    /** Zwei Räume mit je einem Stream: Beide bekommen das Lebenszeichen. */
    @Test
    void sendsHeartbeatToStreamsOfAllRooms() {
        RoomStreamRegistry registry = new RoomStreamRegistry();
        RecordingEmitter inRoomA = new RecordingEmitter();
        RecordingEmitter inRoomB = new RecordingEmitter();
        RoomStream roomA = registry.getOrCreate(UUID.randomUUID());
        RoomStream roomB = registry.getOrCreate(UUID.randomUUID());
        roomA.subscribe(inRoomA, null);
        roomB.subscribe(inRoomB, null);
        HeartbeatSender heartbeatSender = new HeartbeatSender(registry);

        heartbeatSender.sendHeartbeats();

        List<String> eventsInRoomA = inRoomA.sentEvents();
        List<String> eventsInRoomB = inRoomB.sentEvents();
        assertEquals(List.of(":ping\n\n"), eventsInRoomA);
        assertEquals(List.of(":ping\n\n"), eventsInRoomB);
    }
}
