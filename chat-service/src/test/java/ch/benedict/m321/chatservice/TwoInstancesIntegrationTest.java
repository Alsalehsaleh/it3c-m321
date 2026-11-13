package ch.benedict.m321.chatservice;

import ch.benedict.m321.chatservice.config.QueueNames;
import ch.benedict.m321.chatservice.service.RoomStream;
import ch.benedict.m321.chatservice.service.RoomStreamRegistry;
import ch.benedict.m321.chatservice.support.StreamReader;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Zwei Instanzen des chat-service am selben RabbitMQ, wie bei
 * docker compose up --scale chat-service=2 (S3).
 *
 * Jeder Test startet zwei frische Instanzen und schliesst sie danach. Das
 * kostet einige Sekunden, aber kein Test hängt davon ab, was ein anderer mit
 * den Instanzen gemacht hat, etwa eine geschlossen.
 */
@Testcontainers
class TwoInstancesIntegrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(15);

    @Container
    static RabbitMQContainer rabbitMq = new RabbitMQContainer("rabbitmq:3.13-management");

    private final ObjectMapper objectMapper = new ObjectMapper();

    private ConfigurableApplicationContext firstInstance;

    private ConfigurableApplicationContext secondInstance;

    /** Startet zwei Instanzen, jede mit eigenem Port und eigener Zustell-Queue. */
    @BeforeEach
    void startTwoInstances() {
        firstInstance = startInstance();
        secondInstance = startInstance();
    }

    /** Schliesst beide Instanzen, auch wenn ein Test eine schon geschlossen hat. */
    @AfterEach
    void stopInstances() {
        firstInstance.close();
        secondInstance.close();
    }

    /**
     * S3 im Kleinen: je ein Stream für Raum A an jeder Instanz, einer für Raum B.
     * Zwei Absender schicken je 20 Nachrichten, anna über Instanz 1, ben über
     * Instanz 2. Beide A-Streams bekommen alle 40 genau einmal, pro Absender in
     * Reihenfolge; der B-Stream bekommt keine (Spec 3.2.1).
     */
    @Test
    void everyRoomStreamGetsEveryMessageOfItsRoomExactlyOnce() throws Exception {
        String firstQueue = deliveryQueueName(firstInstance);
        String secondQueue = deliveryQueueName(secondInstance);
        assertNotEquals(firstQueue, secondQueue);
        assertQueueExists(firstInstance, firstQueue);
        assertQueueExists(secondInstance, secondQueue);

        UUID roomA = UUID.randomUUID();
        UUID roomB = UUID.randomUUID();
        StreamReader roomAOnFirst = openStream(firstInstance, roomA);
        StreamReader roomAOnSecond = openStream(secondInstance, roomA);
        StreamReader roomBOnFirst = openStream(firstInstance, roomB);

        List<String> annaIds = new ArrayList<>();
        List<String> benIds = new ArrayList<>();
        for (int i = 1; i <= 20; i++) {
            String annaId = postMessage(firstInstance, roomA, "anna", "Nachricht " + i);
            String benId = postMessage(secondInstance, roomA, "ben", "Nachricht " + i);
            annaIds.add(annaId);
            benIds.add(benId);
        }

        String lastAnna = annaIds.get(19);
        String lastBen = benIds.get(19);
        assertTrue(roomAOnFirst.waitForLine("id:" + lastAnna, TIMEOUT));
        assertTrue(roomAOnFirst.waitForLine("id:" + lastBen, TIMEOUT));
        assertTrue(roomAOnSecond.waitForLine("id:" + lastAnna, TIMEOUT));
        assertTrue(roomAOnSecond.waitForLine("id:" + lastBen, TIMEOUT));
        assertRoomStreamComplete(roomAOnFirst, annaIds, benIds);
        assertRoomStreamComplete(roomAOnSecond, annaIds, benIds);
        List<String> roomBIds = idsInStream(roomBOnFirst);
        assertEquals(0, roomBIds.size());
        roomAOnFirst.close();
        roomAOnSecond.close();
        roomBOnFirst.close();
    }

    /**
     * S7 im Kleinen: Ein Client an Instanz 1 bekommt eine Nachricht und trennt.
     * Dann kommen 50 Nachrichten. Mit Last-Event-ID neu verbunden, an derselben
     * UND an der anderen Instanz, bekommt er genau die 50, in Reihenfolge,
     * keine doppelt, und danach geht es live weiter (Spec 3.2.5).
     */
    @Test
    void reconnectWithLastEventIdDeliversExactlyTheMissedMessagesOnBothInstances() throws Exception {
        UUID roomId = UUID.randomUUID();
        StreamReader firstConnection = openStream(firstInstance, roomId);
        String seenId = postMessage(firstInstance, roomId, "anna", "vor dem Trennen");
        assertTrue(firstConnection.waitForLine("id:" + seenId, TIMEOUT));
        firstConnection.close();

        List<String> missedIds = new ArrayList<>();
        for (int i = 1; i <= 50; i++) {
            String missedId = postMessage(firstInstance, roomId, "anna", "verpasst " + i);
            missedIds.add(missedId);
        }
        String lastMissed = missedIds.get(49);
        StreamReader sameInstance = new StreamReader(baseUrl(firstInstance) + "/stream?roomId=" + roomId, seenId);
        StreamReader otherInstance = new StreamReader(baseUrl(secondInstance) + "/stream?roomId=" + roomId, seenId);
        assertTrue(sameInstance.waitForLine("id:" + lastMissed, TIMEOUT));
        assertTrue(otherInstance.waitForLine("id:" + lastMissed, TIMEOUT));

        String liveId = postMessage(secondInstance, roomId, "anna", "wieder live");
        assertTrue(sameInstance.waitForLine("id:" + liveId, TIMEOUT));
        assertTrue(otherInstance.waitForLine("id:" + liveId, TIMEOUT));

        List<String> expectedIds = new ArrayList<>(missedIds);
        expectedIds.add(liveId);
        List<String> idsOnSameInstance = idsInStream(sameInstance);
        List<String> idsOnOtherInstance = idsInStream(otherInstance);
        sameInstance.close();
        otherInstance.close();
        assertEquals(expectedIds, idsOnSameInstance);
        assertEquals(expectedIds, idsOnOtherInstance);
        assertFalse(idsOnSameInstance.contains(seenId));
    }

    /**
     * S4 im Kleinen: Wird Instanz 2 geschlossen, verschwindet ihre Zustell-Queue
     * sofort, weil auto-delete greift, sobald ihr Consumer weg ist. Die 5 s sind
     * strenger als die 30 s aus S4 und kürzer als x-expires (10 s): So zeigt der
     * Test, dass wirklich auto-delete aufräumt und nicht erst das Sicherheitsnetz
     * (Spec 3.2.2). Die Queue von Instanz 1, chat.persist und chat.dlq bleiben.
     */
    @Test
    void closedInstanceLosesItsDeliveryQueueOthersStay() throws Exception {
        String firstQueue = deliveryQueueName(firstInstance);
        String secondQueue = deliveryQueueName(secondInstance);
        RabbitAdmin rabbitAdmin = firstInstance.getBean(RabbitAdmin.class);
        assertNotNull(rabbitAdmin.getQueueProperties(secondQueue));

        secondInstance.close();

        Instant deadline = Instant.now().plus(Duration.ofSeconds(5));
        while (rabbitAdmin.getQueueProperties(secondQueue) != null && Instant.now().isBefore(deadline)) {
            Thread.sleep(100);
        }
        assertNull(rabbitAdmin.getQueueProperties(secondQueue), "Queue von Instanz 2 besteht noch");
        assertNotNull(rabbitAdmin.getQueueProperties(firstQueue));
        assertNotNull(rabbitAdmin.getQueueProperties(QueueNames.PERSIST_QUEUE));
        assertNotNull(rabbitAdmin.getQueueProperties(QueueNames.DEAD_LETTER_QUEUE));
    }

    /**
     * Ein A-Stream hat genau 40 verschiedene ids, und die Nachrichten jedes
     * Absenders stehen in der Reihenfolge, in der er sie gesendet hat.
     * Zwischen den beiden Absendern ist die Reihenfolge nicht zugesagt.
     */
    private void assertRoomStreamComplete(StreamReader reader, List<String> annaIds, List<String> benIds) {
        List<String> receivedIds = idsInStream(reader);
        Set<String> distinctIds = new HashSet<>(receivedIds);
        assertEquals(40, receivedIds.size());
        assertEquals(40, distinctIds.size());
        List<String> receivedFromAnna = keepOnly(receivedIds, annaIds);
        List<String> receivedFromBen = keepOnly(receivedIds, benIds);
        assertEquals(annaIds, receivedFromAnna);
        assertEquals(benIds, receivedFromBen);
    }

    /**
     * Startet eine Instanz mit zufälligem Port gegen den RabbitMQ-Container.
     *
     * Die Werte gehen als Kommandozeilen-Argumente hinein, wie bei
     * java -jar ... --server.port=0. Nur die haben Vorrang vor application.yml;
     * builder.properties(...) wären blosse Vorgabewerte und würden überschrieben.
     */
    private ConfigurableApplicationContext startInstance() {
        SpringApplicationBuilder builder = new SpringApplicationBuilder(ChatServiceApplication.class);
        return builder.run(
                "--server.port=0",
                "--server.shutdown=immediate",
                "--spring.rabbitmq.host=" + rabbitMq.getHost(),
                "--spring.rabbitmq.port=" + rabbitMq.getAmqpPort(),
                "--spring.rabbitmq.username=" + rabbitMq.getAdminUsername(),
                "--spring.rabbitmq.password=" + rabbitMq.getAdminPassword());
    }

    /** Der Name der Zustell-Queue, den diese Instanz beim Start gebildet hat. */
    private String deliveryQueueName(ConfigurableApplicationContext instance) {
        Queue deliveryQueue = instance.getBean("deliveryQueue", Queue.class);
        return deliveryQueue.getName();
    }

    /** Prüft im Broker, dass die Queue wirklich existiert. */
    private void assertQueueExists(ConfigurableApplicationContext instance, String queueName) {
        RabbitAdmin rabbitAdmin = instance.getBean(RabbitAdmin.class);
        assertNotNull(rabbitAdmin.getQueueProperties(queueName), queueName);
    }

    /** Öffnet einen Stream an dieser Instanz und wartet, bis sie ihn angemeldet hat. */
    private StreamReader openStream(ConfigurableApplicationContext instance, UUID roomId)
            throws InterruptedException {
        String url = baseUrl(instance) + "/stream?roomId=" + roomId;
        StreamReader reader = new StreamReader(url, null);
        RoomStreamRegistry registry = instance.getBean(RoomStreamRegistry.class);
        RoomStream roomStream = registry.getOrCreate(roomId);
        Instant deadline = Instant.now().plus(TIMEOUT);
        while (roomStream.openStreamCount() < 1 && Instant.now().isBefore(deadline)) {
            Thread.sleep(20);
        }
        assertEquals(1, roomStream.openStreamCount());
        return reader;
    }

    /** Schickt eine Nachricht über POST /messages an diese Instanz und gibt ihre id zurück. */
    private String postMessage(ConfigurableApplicationContext instance, UUID roomId,
                               String senderId, String content) throws Exception {
        String json = "{\"roomId\":\"" + roomId + "\",\"senderId\":\"" + senderId + "\","
                + "\"senderName\":\"" + senderId + "\",\"content\":\"" + content + "\"}";
        URI uri = URI.create(baseUrl(instance) + "/messages");
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

    /** http://localhost:<Port> der Instanz. */
    private String baseUrl(ConfigurableApplicationContext instance) {
        String port = instance.getEnvironment().getProperty("local.server.port");
        return "http://localhost:" + port;
    }

    /** Die ids aller Ereignisse im Stream, in der Reihenfolge ihrer Ankunft. */
    private List<String> idsInStream(StreamReader reader) {
        List<String> lines = reader.lines();
        List<String> ids = new ArrayList<>();
        for (String line : lines) {
            if (line.startsWith("id:")) {
                ids.add(line.substring(3));
            }
        }
        return ids;
    }

    /** Behält aus receivedIds nur die ids eines Absenders, in der empfangenen Reihenfolge. */
    private List<String> keepOnly(List<String> receivedIds, List<String> senderIds) {
        Set<String> wanted = new HashSet<>(senderIds);
        List<String> kept = new ArrayList<>();
        for (String id : receivedIds) {
            if (wanted.contains(id)) {
                kept.add(id);
            }
        }
        return kept;
    }
}
