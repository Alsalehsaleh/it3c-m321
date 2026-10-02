package ch.benedict.m321.batchwriter.dto;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConversionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.json.JsonTest;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Prüft den Vertrag aus Spec Kap. 2: Kommt das JSON so an, wie der chat-service es
 * wirklich in die Queue legt, wird daraus eine vollständige ChatMessage.
 *
 * Getestet wird mit dem ObjectMapper von Spring Boot und demselben Konverter, den der
 * Dienst benutzt. "@JsonTest" startet dafür nur den JSON-Teil von Spring, ohne Container —
 * deshalb ist dieser Test schnell.
 */
@JsonTest
class ChatMessageConversionTest {

    /** Die Nachricht aus Spec 2.2, so wie sie am 30.09.2026 in chat.persist lag. */
    private static final String MEASURED_JSON = """
            {"id":"147a747e-f792-4081-80e3-6c9c7227dfb8",
             "roomId":"3f2b1c4e-0000-0000-0000-000000000001",
             "senderId":"anna",
             "senderName":"Anna Muster",
             "content":"Formatpruefung sentAt",
             "sentAt":"2026-09-30T16:31:38.474905082Z"}
            """;

    /** Dieselbe Nachricht, aber ohne das Feld content. */
    private static final String JSON_WITHOUT_CONTENT = """
            {"id":"147a747e-f792-4081-80e3-6c9c7227dfb8",
             "roomId":"3f2b1c4e-0000-0000-0000-000000000001",
             "senderId":"anna",
             "senderName":"Anna Muster",
             "sentAt":"2026-09-30T16:31:38.474905082Z"}
            """;

    @Autowired
    private ObjectMapper objectMapper;

    private Jackson2JsonMessageConverter converter;

    /** Baut den Konverter genau so, wie RabbitConfig es im Dienst tut. */
    @BeforeEach
    void createConverter() {
        converter = new Jackson2JsonMessageConverter(objectMapper);
    }

    /** Der Normalfall: alle sechs Felder kommen an, sentAt mit allen neun Nachkommastellen. */
    @Test
    void readsTheMeasuredMessageWithoutTypeIdHeader() {
        Message message = createAmqpMessage(MEASURED_JSON);

        Object converted = converter.fromMessage(message);

        ChatMessage chatMessage = (ChatMessage) converted;
        UUID expectedId = UUID.fromString("147a747e-f792-4081-80e3-6c9c7227dfb8");
        UUID expectedRoomId = UUID.fromString("3f2b1c4e-0000-0000-0000-000000000001");
        Instant expectedSentAt = Instant.parse("2026-09-30T16:31:38.474905082Z");
        assertEquals(expectedId, chatMessage.id());
        assertEquals(expectedRoomId, chatMessage.roomId());
        assertEquals("anna", chatMessage.senderId());
        assertEquals("Anna Muster", chatMessage.senderName());
        assertEquals("Formatpruefung sentAt", chatMessage.content());
        assertEquals(expectedSentAt, chatMessage.sentAt());
    }

    /** Fehlt ein Feld, ist die Nachricht kaputt und darf nicht erst an der Datenbank scheitern. */
    @Test
    void missingFieldFailsTheConversion() {
        Message message = createAmqpMessage(JSON_WITHOUT_CONTENT);

        assertThrows(MessageConversionException.class, () -> converter.fromMessage(message));
    }

    /** Kein JSON: ebenfalls eine kaputte Nachricht, die in die DLQ gehört (Spec 3.2.3). */
    @Test
    void brokenJsonFailsTheConversion() {
        Message message = createAmqpMessage("das ist kein JSON");

        assertThrows(MessageConversionException.class, () -> converter.fromMessage(message));
    }

    /**
     * Baut eine AMQP-Nachricht wie in Szenario S5: nur mit content_type, ohne __TypeId__.
     *
     * Den Zieltyp hängt hier der Test an. Im Dienst tut das der Listener-Container, der
     * ihn aus der Signatur der Listener-Methode ableitet (Spec 2.4).
     */
    private Message createAmqpMessage(String json) {
        byte[] body = json.getBytes(StandardCharsets.UTF_8);
        MessageProperties properties = new MessageProperties();
        properties.setContentType(MessageProperties.CONTENT_TYPE_JSON);
        properties.setInferredArgumentType(ChatMessage.class);
        return new Message(body, properties);
    }
}
