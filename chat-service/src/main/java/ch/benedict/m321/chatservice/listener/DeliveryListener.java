package ch.benedict.m321.chatservice.listener;

import ch.benedict.m321.chatservice.dto.BufferedMessage;
import ch.benedict.m321.chatservice.dto.ChatMessage;
import ch.benedict.m321.chatservice.service.RoomStream;
import ch.benedict.m321.chatservice.service.RoomStreamRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * Liest die eigene Zustell-Queue dieser Instanz und gibt jede Nachricht an
 * den RoomStream ihres Raums. Hier kommt die Kopie vom Fanout-Exchange an,
 * egal über welche Instanz sie gesendet wurde (Spec 3.1).
 *
 * Gelesen wird mit genau einem Thread (application.yml). Nur so bleibt die
 * Reihenfolge eines Absenders erhalten (Spec 3.2.1).
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class DeliveryListener {

    private final RoomStreamRegistry roomStreamRegistry;

    /**
     * Nimmt eine Nachricht aus der Zustell-Queue.
     *
     * queues = "#{deliveryQueue.name}" holt den Namen aus dem Bean deliveryQueue
     * in RabbitConfig. Der Name entsteht erst beim Start und steht deshalb
     * nicht fest im Code.
     *
     * Die Nachricht kommt zweimal herein: als ChatMessage, um id und roomId zu
     * lesen, und als rohe Message, deren Body unverändert in data: geht. Neu in
     * JSON geschrieben wird nichts (Spec 2.1).
     */
    @RabbitListener(queues = "#{deliveryQueue.name}")
    public void onMessage(@Payload ChatMessage chatMessage, Message rawMessage) {
        byte[] bodyBytes = rawMessage.getBody();
        String body = new String(bodyBytes, StandardCharsets.UTF_8);
        BufferedMessage bufferedMessage = new BufferedMessage(chatMessage.id(), body);
        RoomStream roomStream = roomStreamRegistry.getOrCreate(chatMessage.roomId());
        roomStream.publish(bufferedMessage);
        log.debug("Message {} delivered to room {}", chatMessage.id(), chatMessage.roomId());
    }
}
