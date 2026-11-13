package ch.benedict.m321.chatservice.service;

import ch.benedict.m321.chatservice.dto.BufferedMessage;
import ch.benedict.m321.chatservice.support.RecordingEmitter;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

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
        roomStream.subscribe(first, null);
        roomStream.subscribe(second, null);
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
        roomStream.subscribe(emitter, null);
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
        roomStream.subscribe(broken, null);
        roomStream.subscribe(healthy, null);
        broken.setFailing(true);

        roomStream.publish(createMessage("{\"nr\":1}"));
        broken.setFailing(false);
        roomStream.publish(createMessage("{\"nr\":2}"));

        List<String> brokenEvents = broken.sentEvents();
        List<String> healthyEvents = healthy.sentEvents();
        assertTrue(brokenEvents.isEmpty());
        assertEquals(2, healthyEvents.size());
    }

    /**
     * Mit einer bekannten Last-Event-ID kommen zuerst genau die Nachrichten
     * danach, in Reihenfolge, und dann geht es live weiter (Spec 3.2.5, S7).
     */
    @Test
    void replaysMessagesAfterLastEventIdThenGoesLive() {
        RoomStream roomStream = new RoomStream();
        List<BufferedMessage> sent = publishNumbered(roomStream, 1, 10);
        BufferedMessage lastSeen = sent.get(3);
        RecordingEmitter emitter = new RecordingEmitter();

        roomStream.subscribe(emitter, lastSeen.id().toString());
        roomStream.publish(createMessage("{\"nr\":11}"));

        List<Integer> numbers = receivedNumbers(emitter);
        List<Integer> expected = numbersFromTo(5, 11);
        assertEquals(expected, numbers);
    }

    /**
     * Ohne brauchbare Last-Event-ID wird nichts nachgeholt, aber auch kein
     * Fehler gemeldet: Der Stream beginnt live (Spec 2.3).
     */
    @Test
    void unknownLastEventIdStartsLive() {
        String unknownId = UUID.randomUUID().toString();
        String[] lastEventIds = {null, "", "keine-uuid", unknownId};
        for (String lastEventId : lastEventIds) {
            RoomStream roomStream = new RoomStream();
            publishNumbered(roomStream, 1, 5);
            RecordingEmitter emitter = new RecordingEmitter();

            roomStream.subscribe(emitter, lastEventId);
            roomStream.publish(createMessage("{\"nr\":6}"));

            List<Integer> numbers = receivedNumbers(emitter);
            List<Integer> expected = numbersFromTo(6, 6);
            assertEquals(expected, numbers, "Last-Event-ID: " + lastEventId);
        }
    }

    /**
     * Die Nahtstelle: Während ein Stream nachholt, kommen neue Nachrichten
     * dazu. Jede muss genau einmal und in Reihenfolge ankommen, egal welcher
     * Thread zuerst das Schloss bekommt (Spec 3.2.5).
     */
    @Test
    void seamBetweenReplayAndLiveLosesAndDuplicatesNothing() throws InterruptedException {
        RoomStream roomStream = new RoomStream();
        List<BufferedMessage> sent = publishNumbered(roomStream, 1, 100);
        BufferedMessage lastSeen = sent.get(9);
        RecordingEmitter emitter = new RecordingEmitter();
        CountDownLatch startSignal = new CountDownLatch(1);

        Thread publisher = new Thread(() -> publishAfterSignal(roomStream, startSignal));
        publisher.start();
        startSignal.countDown();
        roomStream.subscribe(emitter, lastSeen.id().toString());
        publisher.join();

        List<Integer> numbers = receivedNumbers(emitter);
        List<Integer> expected = numbersFromTo(11, 200);
        assertEquals(expected, numbers);
    }

    /** Wartet auf das Startsignal und publiziert dann die Nachrichten 101 bis 200. */
    private void publishAfterSignal(RoomStream roomStream, CountDownLatch startSignal) {
        try {
            startSignal.await();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            return;
        }
        publishNumbered(roomStream, 101, 200);
    }

    /** Publiziert Nachrichten mit Body {"nr":from} bis {"nr":to} und gibt sie zurück. */
    private List<BufferedMessage> publishNumbered(RoomStream roomStream, int from, int to) {
        List<BufferedMessage> sent = new ArrayList<>();
        for (int nr = from; nr <= to; nr++) {
            BufferedMessage message = createMessage("{\"nr\":" + nr + "}");
            roomStream.publish(message);
            sent.add(message);
        }
        return sent;
    }

    /** Liest aus jedem empfangenen Ereignis die Nummer aus dem Body {"nr":N}. */
    private List<Integer> receivedNumbers(RecordingEmitter emitter) {
        List<String> events = emitter.sentEvents();
        List<Integer> numbers = new ArrayList<>();
        for (String event : events) {
            int start = event.indexOf("\"nr\":") + 5;
            int end = event.indexOf("}", start);
            String digits = event.substring(start, end);
            numbers.add(Integer.parseInt(digits));
        }
        return numbers;
    }

    /** Die Zahlen von from bis to, als erwartete Reihenfolge. */
    private List<Integer> numbersFromTo(int from, int to) {
        List<Integer> numbers = new ArrayList<>();
        for (int nr = from; nr <= to; nr++) {
            numbers.add(nr);
        }
        return numbers;
    }

    /** Eine Nachricht mit neuer zufälliger id, wie sie der chat-service vergibt. */
    private BufferedMessage createMessage(String body) {
        UUID id = UUID.randomUUID();
        return new BufferedMessage(id, body);
    }
}
