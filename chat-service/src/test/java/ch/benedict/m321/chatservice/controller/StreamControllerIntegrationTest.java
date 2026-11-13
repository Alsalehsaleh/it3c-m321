package ch.benedict.m321.chatservice.controller;

import ch.benedict.m321.chatservice.dto.BufferedMessage;
import ch.benedict.m321.chatservice.service.RoomStream;
import ch.benedict.m321.chatservice.service.RoomStreamRegistry;
import ch.benedict.m321.chatservice.support.StreamReader;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prüft den Endpunkt GET /stream über echtes HTTP: Antwort, Format der
 * Ereignisse und das Austragen eines Clients, der weggeht (Spec 2.2, 3.2.6).
 *
 * RabbitMQ läuft mit, weil der Spring-Kontext des chat-service ohne Broker
 * nicht startet, wie in den bestehenden Tests.
 *
 * server.shutdown=immediate gilt nur hier: Am Ende des Tests sind noch Streams
 * offen, und mit "graceful" würde Spring bis zu 30 s auf sie warten. Im
 * Betrieb bleibt "graceful" (Spec 3.2.2).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = "server.shutdown=immediate")
@Testcontainers
class StreamControllerIntegrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    @Container
    @ServiceConnection
    static RabbitMQContainer rabbitMq = new RabbitMQContainer("rabbitmq:3.13-management");

    @LocalServerPort
    private int port;

    @Autowired
    private RoomStreamRegistry roomStreamRegistry;

    /**
     * Eine gültige roomId öffnet einen Stream mit 200 und text/event-stream.
     * Die Kopfzeilen schickt Spring mit dem ersten Ereignis, deshalb wird
     * zuerst eine Nachricht publiziert.
     */
    @Test
    void opensEventStreamForValidRoom() throws Exception {
        UUID roomId = UUID.randomUUID();
        StreamReader reader = new StreamReader(streamUrl(roomId.toString()), null);
        RoomStream roomStream = roomStreamRegistry.getOrCreate(roomId);
        waitUntilOpenStreams(roomStream, 1);

        roomStream.publish(new BufferedMessage(UUID.randomUUID(), "{}"));

        int statusCode = reader.statusCode();
        String contentType = reader.contentType();
        reader.close();
        assertEquals(200, statusCode);
        assertTrue(contentType.startsWith("text/event-stream"), contentType);
    }

    /** Fehlt die roomId oder ist sie keine UUID, antwortet Spring mit 400 (Spec 2.2). */
    @Test
    void rejectsMissingOrInvalidRoomId() throws Exception {
        int missingStatus = statusOf("http://localhost:" + port + "/stream");
        int invalidStatus = statusOf(streamUrl("abc"));

        assertEquals(400, missingStatus);
        assertEquals(400, invalidStatus);
    }

    /**
     * Eine Nachricht im RoomStream kommt im echten Stream als id: und data:
     * an, ohne Leerzeichen nach dem Doppelpunkt (Spec 2.2).
     */
    @Test
    void deliversPublishedMessageAsSseEvent() throws Exception {
        UUID roomId = UUID.randomUUID();
        StreamReader reader = new StreamReader(streamUrl(roomId.toString()), null);
        UUID messageId = UUID.randomUUID();
        String body = "{\"id\":\"" + messageId + "\",\"content\":\"Hallo\"}";
        RoomStream roomStream = roomStreamRegistry.getOrCreate(roomId);
        waitUntilOpenStreams(roomStream, 1);

        roomStream.publish(new BufferedMessage(messageId, body));

        boolean idArrived = reader.waitForLine("id:" + messageId, TIMEOUT);
        boolean dataArrived = reader.waitForLine("data:" + body, TIMEOUT);
        reader.close();
        assertTrue(idArrived);
        assertTrue(dataArrived);
    }

    /**
     * Geht ein Client weg, wird sein Stream ausgetragen. Der Dienst merkt das
     * erst beim Schreiben, deshalb wird so lange publiziert, bis der Stream
     * weg ist, höchstens TIMEOUT lang (Spec 3.2.6).
     */
    @Test
    void closedStreamIsRemoved() throws Exception {
        UUID roomId = UUID.randomUUID();
        StreamReader reader = new StreamReader(streamUrl(roomId.toString()), null);
        RoomStream roomStream = roomStreamRegistry.getOrCreate(roomId);
        waitUntilOpenStreams(roomStream, 1);
        UUID firstId = UUID.randomUUID();
        roomStream.publish(new BufferedMessage(firstId, "{}"));
        boolean connected = reader.waitForLine("id:" + firstId, TIMEOUT);
        int countWhileOpen = roomStream.openStreamCount();

        reader.close();
        int countAfterClose = publishUntilNoStreamLeft(roomStream);

        assertTrue(connected);
        assertEquals(1, countWhileOpen);
        assertEquals(0, countAfterClose);
    }

    /**
     * Wartet, bis der Dienst so viele Streams im Raum angemeldet hat. Erst dann
     * darf publiziert werden, sonst ginge die Nachricht am Stream vorbei.
     */
    private void waitUntilOpenStreams(RoomStream roomStream, int expected) throws InterruptedException {
        Instant deadline = Instant.now().plus(TIMEOUT);
        int openStreams = roomStream.openStreamCount();
        while (openStreams < expected && Instant.now().isBefore(deadline)) {
            Thread.sleep(20);
            openStreams = roomStream.openStreamCount();
        }
        assertEquals(expected, openStreams);
    }

    /** Publiziert alle 100 ms eine Nachricht, bis kein Stream mehr offen ist oder die Zeit um ist. */
    private int publishUntilNoStreamLeft(RoomStream roomStream) throws InterruptedException {
        Instant deadline = Instant.now().plus(TIMEOUT);
        int openStreams = roomStream.openStreamCount();
        while (openStreams > 0 && Instant.now().isBefore(deadline)) {
            UUID messageId = UUID.randomUUID();
            roomStream.publish(new BufferedMessage(messageId, "{}"));
            Thread.sleep(100);
            openStreams = roomStream.openStreamCount();
        }
        return openStreams;
    }

    /** Die URL des Streams für eine roomId, so wie sie der Client aufruft. */
    private String streamUrl(String roomId) {
        return "http://localhost:" + port + "/stream?roomId=" + roomId;
    }

    /** Ein einfacher GET, der nur den Status zurückgibt. */
    private int statusOf(String url) throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        URI uri = URI.create(url);
        HttpRequest request = HttpRequest.newBuilder(uri).build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        return response.statusCode();
    }
}
