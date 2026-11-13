package ch.benedict.m321.chatservice.service;

import ch.benedict.m321.chatservice.dto.BufferedMessage;
import ch.benedict.m321.chatservice.support.RecordingEmitter;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prüft den Kern der Live-Zustellung ohne RabbitMQ und ohne HTTP: den Puffer
 * eines Raums und das Verteilen an seine Streams (Spec 3.1, 3.2.5, 3.2.6).
 */
class RoomStreamTest {

    /** Jeder angemeldete Stream bekommt die Nachricht, mit id und unverändertem Body. */
    @Test
    void sendsMessageToAllSubscribedStreams() {
        RoomStream roomStream = new RoomStream();
        RecordingEmitter first = new RecordingEmitter();
        RecordingEmitter second = new RecordingEmitter();
        roomStream.subscribe(first);
        roomStream.subscribe(second);
        BufferedMessage message = createMessage("{\"content\":\"Hallo\"}");

        roomStream.publish(message);

        List<String> firstEvents = first.sentEvents();
        List<String> secondEvents = second.sentEvents();
        assertEquals(1, firstEvents.size());
        assertEquals(1, secondEvents.size());
        String expectedEvent = "id:" + message.id() + "\ndata:" + message.body() + "\n\n";
        assertEquals(expectedEvent, firstEvents.get(0));
        assertEquals(expectedEvent, secondEvents.get(0));
    }

    /** Nach dem Abmelden geht nichts mehr an diesen Stream. */
    @Test
    void unsubscribedStreamReceivesNothing() {
        RoomStream roomStream = new RoomStream();
        RecordingEmitter emitter = new RecordingEmitter();
        roomStream.subscribe(emitter);
        roomStream.unsubscribe(emitter);

        roomStream.publish(createMessage("{}"));

        List<String> events = emitter.sentEvents();
        assertTrue(events.isEmpty());
    }

    /** Der Puffer hält die neuesten 200 Nachrichten; die älteste fällt heraus (Spec 3.2.5). */
    @Test
    void bufferKeepsOnlyTheNewest200Messages() {
        RoomStream roomStream = new RoomStream();
        BufferedMessage oldest = createMessage("{\"nr\":0}");
        roomStream.publish(oldest);
        for (int i = 1; i <= 200; i++) {
            roomStream.publish(createMessage("{\"nr\":" + i + "}"));
        }

        List<BufferedMessage> buffer = roomStream.copyOfBuffer();

        assertEquals(200, buffer.size());
        BufferedMessage first = buffer.get(0);
        BufferedMessage last = buffer.get(199);
        assertEquals("{\"nr\":1}", first.body());
        assertEquals("{\"nr\":200}", last.body());
    }

    /**
     * Ein kaputter Stream darf die anderen nicht aufhalten. Er wird ausgetragen
     * und bekommt danach nichts mehr, auch wenn er wieder funktionieren würde
     * (Spec 3.2.6).
     */
    @Test
    void brokenStreamIsRemovedAndOthersStillReceive() {
        RoomStream roomStream = new RoomStream();
        RecordingEmitter broken = new RecordingEmitter();
        RecordingEmitter healthy = new RecordingEmitter();
        roomStream.subscribe(broken);
        roomStream.subscribe(healthy);
        broken.setFailing(true);

        roomStream.publish(createMessage("{\"nr\":1}"));
        broken.setFailing(false);
        roomStream.publish(createMessage("{\"nr\":2}"));

        List<String> brokenEvents = broken.sentEvents();
        List<String> healthyEvents = healthy.sentEvents();
        assertTrue(brokenEvents.isEmpty());
        assertEquals(2, healthyEvents.size());
    }

    /** Eine Nachricht mit neuer zufälliger id, wie sie der chat-service vergibt. */
    private BufferedMessage createMessage(String body) {
        UUID id = UUID.randomUUID();
        return new BufferedMessage(id, body);
    }
}
