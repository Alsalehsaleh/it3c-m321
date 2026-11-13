package ch.benedict.m321.chatservice.config;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prüft gegen einen echten RabbitMQ, dass jede Instanz ihre eigene
 * Zustell-Queue anlegt, an chat.delivery bindet und mit den Eigenschaften aus
 * Spec 4 versieht.
 *
 * Die Eigenschaften liest der Test über die HTTP-Management-API des Containers.
 * Nur dort sieht man, was der Broker wirklich angelegt hat, nicht nur, was
 * Spring angelegt haben wollte.
 */
@SpringBootTest
@Testcontainers
class DeliveryQueueIntegrationTest {

    @Container
    @ServiceConnection
    static RabbitMQContainer rabbitMq = new RabbitMQContainer("rabbitmq:3.13-management");

    @Autowired
    @Qualifier("deliveryQueue")
    private Queue deliveryQueue;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private RabbitAdmin rabbitAdmin;

    /**
     * Spring legt Queues und Bindungen erst beim ersten Verbindungsaufbau an.
     * Im Betrieb öffnet der Listener die Verbindung beim Start; hier öffnet
     * sie der Test ausdrücklich, damit er nicht vom Listener abhängt.
     */
    @BeforeEach
    void declareBrokerObjects() {
        rabbitAdmin.initialize();
    }

    /** Der Name beginnt mit chat.delivery. und die Queue existiert im Broker (Spec 4). */
    @Test
    void declaresDeliveryQueueWithInstancePrefix() throws Exception {
        String queueName = deliveryQueue.getName();

        JsonNode queueInfo = managementGet("/api/queues/%2F/" + queueName);

        assertTrue(queueName.startsWith("chat.delivery."), queueName);
        String brokerName = queueInfo.path("name").asText();
        assertEquals(queueName, brokerName);
    }

    /**
     * Die Queue ist an den Fanout-Exchange chat.delivery gebunden. Ohne diese
     * Bindung verwirft RabbitMQ die Kopie (Spec 1.1).
     */
    @Test
    void bindsDeliveryQueueToFanoutExchange() throws Exception {
        String queueName = deliveryQueue.getName();

        JsonNode bindings = managementGet("/api/exchanges/%2F/chat.delivery/bindings/source");

        boolean bound = false;
        for (JsonNode binding : bindings) {
            String destination = binding.path("destination").asText();
            if (destination.equals(queueName)) {
                bound = true;
            }
        }
        assertTrue(bound, "keine Bindung von chat.delivery an " + queueName);
    }

    /**
     * Die Eigenschaften, an denen S4 und S6 hängen: nicht durable, auto-delete,
     * x-expires 10000, x-max-length 900, drop-head und keine Dead-Letter-Route
     * (Spec 3.2.2, 3.2.4, 4).
     */
    @Test
    void deliveryQueueHasPropertiesFromSpec() throws Exception {
        String queueName = deliveryQueue.getName();

        JsonNode queueInfo = managementGet("/api/queues/%2F/" + queueName);

        JsonNode arguments = queueInfo.path("arguments");
        assertFalse(queueInfo.path("durable").asBoolean());
        assertTrue(queueInfo.path("auto_delete").asBoolean());
        assertEquals(10000, arguments.path("x-expires").asInt());
        assertEquals(900, arguments.path("x-max-length").asInt());
        assertEquals("drop-head", arguments.path("x-overflow").asText());
        assertTrue(arguments.path("x-dead-letter-exchange").isMissingNode());
    }

    /** Ein GET an die Management-API des Test-Containers, mit dessen Admin-Zugang. */
    private JsonNode managementGet(String path) throws Exception {
        String credentials = rabbitMq.getAdminUsername() + ":" + rabbitMq.getAdminPassword();
        byte[] credentialBytes = credentials.getBytes(StandardCharsets.UTF_8);
        String encoded = Base64.getEncoder().encodeToString(credentialBytes);
        URI uri = URI.create(rabbitMq.getHttpUrl() + path);
        HttpRequest request = HttpRequest.newBuilder(uri)
                .header("Authorization", "Basic " + encoded)
                .build();
        HttpClient client = HttpClient.newHttpClient();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), "Management-API " + path);
        return objectMapper.readTree(response.body());
    }
}
