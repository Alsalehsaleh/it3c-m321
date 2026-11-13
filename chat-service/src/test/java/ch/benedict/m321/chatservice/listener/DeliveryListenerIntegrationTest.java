package ch.benedict.m321.chatservice.listener;

import ch.benedict.m321.chatservice.config.QueueNames;
import ch.benedict.m321.chatservice.service.RoomStream;
import ch.benedict.m321.chatservice.service.RoomStreamRegistry;
import ch.benedict.m321.chatservice.support.StreamReader;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueInformation;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Der ganze Weg einer Nachricht in einer Instanz: POST /messages, RabbitMQ,
 * DeliveryListener, RoomStream, data: im Stream (Spec 3.1).
 *
 * server.shutdown=immediate nur im Test, damit Spring am Ende nicht 30 s auf
 * die offenen Streams wartet. Im Betrieb bleibt "graceful" (Spec 3.2.2).
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = "server.shutdown=immediate")
@Testcontainers
@ExtendWith(OutputCaptureExtension.class)
class DeliveryListenerIntegrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    @Container
    @ServiceConnection
    static RabbitMQContainer rabbitMq = new RabbitMQContainer("rabbitmq:3.13-management");

    @LocalServerPort
    private int port;

    @Autowired
    private RoomStreamRegistry roomStreamRegistry;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private RabbitAdmin rabbitAdmin;

    @Autowired
    @Qualifier("deliveryQueue")
    private Queue deliveryQueue;

    /**
     * Eine Nachricht für Raum A erscheint im Stream von Raum A und nicht in dem
     * von Raum B. Gefiltert wird in der Instanz (Spec 3.2.1).
     */
    @Test
    void postedMessageReachesOnlyStreamsOfItsRoom() throws Exception {
        UUID roomA = UUID.randomUUID();
        UUID roomB = UUID.randomUUID();
        StreamReader readerA = openStream(roomA);
        StreamReader readerB = openStream(roomB);

        String messageId = postMessage(roomA, "Hallo Raum A");

        boolean arrivedInA = readerA.waitForLine("id:" + messageId, TIMEOUT);
        List<String> linesInB = readerB.lines();
        readerA.close();
        readerB.close();
        assertTrue(arrivedInA);
        assertEquals(0, countDataLines(linesInB));
    }

    /**
     * data: ist Zeichen für Zeichen derselbe Body, der auch in chat.persist
     * liegt. Das verlangt das Prüfskript (Spec 2.1).
     */
    @Test
    void dataIsTheUnchangedBodyFromThePersistQueue() throws Exception {
        UUID roomId = UUID.randomUUID();
        StreamReader reader = openStream(roomId);

        String messageId = postMessage(roomId, "Format pruefen");

        boolean arrived = reader.waitForLine("id:" + messageId, TIMEOUT);
        String persistBody = receivePersistBody(messageId);
        List<String> lines = reader.lines();
        reader.close();
        assertTrue(arrived);
        assertNotNull(persistBody, "Nachricht nicht in chat.persist gefunden");
        assertTrue(lines.contains("data:" + persistBody), "data: weicht vom Body in chat.persist ab");
    }

    /**
     * 50 Nachrichten eines Absenders kommen in der gesendeten Reihenfolge an.
     * Das sichert der eine Listener-Thread (Spec 3.2.1).
     */
    @Test
    void messagesOfOneSenderArriveInOrder() throws Exception {
        UUID roomId = UUID.randomUUID();
        StreamReader reader = openStream(roomId);
        List<String> sentIds = new ArrayList<>();
        for (int i = 1; i <= 50; i++) {
            String messageId = postMessage(roomId, "Nachricht " + i);
            sentIds.add(messageId);
        }

        String lastId = sentIds.get(49);
        boolean lastArrived = reader.waitForLine("id:" + lastId, TIMEOUT);
        List<String> receivedIds = idsInStream(reader.lines());
        reader.close();
        assertTrue(lastArrived);
        assertEquals(sentIds, receivedIds);
    }

    /**
     * Ungültige Nachrichten direkt am Exchange chat.delivery: kaputtes JSON, JSON
     * ohne roomId und JSON ohne id. Keine wird zugestellt, keine bleibt in der
     * Queue, und für die beiden ohne Feld steht eine Warnung im Log statt einer
     * NullPointerException. Eine gültige Nachricht danach kommt normal an: Der
     * Listener läuft weiter (Spec 3.2.6).
     */
    @Test
    void invalidMessagesAreDroppedWithWarningAndListenerKeepsRunning(CapturedOutput output) throws Exception {
        UUID roomId = UUID.randomUUID();
        StreamReader reader = openStream(roomId);
        String withoutRoomId = "{\"id\":\"" + UUID.randomUUID() + "\",\"senderId\":\"anna\","
                + "\"senderName\":\"Anna\",\"content\":\"ohne Raum\",\"sentAt\":\"2026-11-13T10:00:00Z\"}";
        String withoutId = "{\"roomId\":\"" + roomId + "\",\"senderId\":\"anna\","
                + "\"senderName\":\"Anna\",\"content\":\"ohne id\",\"sentAt\":\"2026-11-13T10:00:00Z\"}";

        sendRawToDeliveryExchange("das ist kein JSON");
        sendRawToDeliveryExchange(withoutRoomId);
        sendRawToDeliveryExchange(withoutId);
        String validId = postMessage(roomId, "gueltig");

        boolean validArrived = reader.waitForLine("id:" + validId, TIMEOUT);
        List<String> lines = reader.lines();
        reader.close();
        int waitingInQueue = messagesWaitingInDeliveryQueue();
        String log = output.getAll();
        assertTrue(validArrived);
        assertEquals(1, countDataLines(lines));
        assertEquals(0, waitingInQueue);
        assertTrue(log.contains("Ignoring message without id or roomId"), "Warnung fehlt im Log");
        assertFalse(log.contains("NullPointerException"), "NullPointerException im Log");
    }

    /** Legt einen Body ohne Umweg über den chat-service an den Fanout-Exchange. */
    private void sendRawToDeliveryExchange(String body) {
        MessageProperties properties = new MessageProperties();
        properties.setContentType(MessageProperties.CONTENT_TYPE_JSON);
        byte[] bodyBytes = body.getBytes(StandardCharsets.UTF_8);
        Message message = new Message(bodyBytes, properties);
        rabbitTemplate.send(QueueNames.DELIVERY_EXCHANGE, "", message);
    }

    /** Wie viele Nachrichten in der Zustell-Queue dieser Instanz warten. */
    private int messagesWaitingInDeliveryQueue() {
        QueueInformation information = rabbitAdmin.getQueueInfo(deliveryQueue.getName());
        return information.getMessageCount();
    }

    /** Öffnet einen Stream und wartet, bis der Dienst ihn angemeldet hat. */
    private StreamReader openStream(UUID roomId) throws InterruptedException {
        String url = "http://localhost:" + port + "/stream?roomId=" + roomId;
        StreamReader reader = new StreamReader(url, null);
        RoomStream roomStream = roomStreamRegistry.getOrCreate(roomId);
        Instant deadline = Instant.now().plus(TIMEOUT);
        while (roomStream.openStreamCount() < 1 && Instant.now().isBefore(deadline)) {
            Thread.sleep(20);
        }
        assertEquals(1, roomStream.openStreamCount());
        return reader;
    }

    /** Schickt eine Nachricht wie ein Client über POST /messages und gibt ihre id zurück. */
    private String postMessage(UUID roomId, String content) throws Exception {
        String json = "{\"roomId\":\"" + roomId + "\",\"senderId\":\"anna\","
                + "\"senderName\":\"Anna Muster\",\"content\":\"" + content + "\"}";
        URI uri = URI.create("http://localhost:" + port + "/messages");
        HttpRequest request = HttpRequest.newBuilder(uri)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build();
        HttpClient client = HttpClient.newHttpClient();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(202, response.statusCode());
        JsonNode accepted = objectMapper.readTree(response.body());
        return accepted.path("id").asText();
    }

    /**
     * Holt Nachrichten aus chat.persist, bis die mit dieser id kommt, und gibt
     * ihren Body als Text zurück. Im Test liest kein batch-writer mit, deshalb
     * liegen dort auch Nachrichten anderer Tests.
     */
    private String receivePersistBody(String messageId) {
        Message message = rabbitTemplate.receive(QueueNames.PERSIST_QUEUE, 5000);
        while (message != null) {
            String body = new String(message.getBody(), StandardCharsets.UTF_8);
            if (body.contains(messageId)) {
                return body;
            }
            message = rabbitTemplate.receive(QueueNames.PERSIST_QUEUE, 5000);
        }
        return null;
    }

    /** Zählt die Zeilen mit data:. */
    private int countDataLines(List<String> lines) {
        int count = 0;
        for (String line : lines) {
            if (line.startsWith("data:")) {
                count++;
            }
        }
        return count;
    }

    /** Die ids aller Ereignisse im Stream, in der Reihenfolge ihrer Ankunft. */
    private List<String> idsInStream(List<String> lines) {
        List<String> ids = new ArrayList<>();
        for (String line : lines) {
            if (line.startsWith("id:")) {
                ids.add(line.substring(3));
            }
        }
        return ids;
    }
}
