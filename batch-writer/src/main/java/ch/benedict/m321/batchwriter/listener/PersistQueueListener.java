package ch.benedict.m321.batchwriter.listener;

import ch.benedict.m321.batchwriter.config.QueueNames;
import ch.benedict.m321.batchwriter.dto.ChatMessage;
import ch.benedict.m321.batchwriter.repository.MessageRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Holt Stapel aus chat.persist und schreibt sie in die Datenbank.
 *
 * Die Stapel bildet der Listener-Container (RabbitConfig). Diese Klasse bekommt eine
 * fertige Liste und sorgt dafür, dass sie geschrieben wird. Bestätigt wird erst, wenn die
 * Methode ohne Fehler endet — also nach dem COMMIT. Das ist At-least-once (Spec 3.1).
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class PersistQueueListener {

    private final MessageRepository messageRepository;

    /**
     * Wird für jeden Stapel einmal aufgerufen.
     *
     * Der Parametertyp List&lt;ChatMessage&gt; ist wichtig: Aus ihm leitet der Container ab,
     * in welche Klasse das JSON gelesen wird. Den Header __TypeId__ braucht es dafür nicht
     * (Spec 2.4).
     */
    @RabbitListener(queues = QueueNames.PERSIST_QUEUE, containerFactory = "batchContainerFactory")
    public void onBatch(List<ChatMessage> messages) {
        int newRows = messageRepository.insertAll(messages);
        log.info("Batch written: {} received, {} new", messages.size(), newRows);
    }
}
