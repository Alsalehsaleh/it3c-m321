package ch.benedict.m321.batchwriter.listener;

import ch.benedict.m321.batchwriter.config.QueueNames;
import ch.benedict.m321.batchwriter.dto.ChatMessage;
import ch.benedict.m321.batchwriter.repository.MessageRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.ImmediateRequeueAmqpException;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.TransactionException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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

    /** Wartezeit vor dem zweiten Versuch, wenn die Datenbank nicht antwortet. */
    private static final long FIRST_WAIT_MILLIS = 1000;

    /** Länger als 10 s wird zwischen zwei Versuchen nie gewartet. */
    private static final long MAX_WAIT_MILLIS = 10_000;

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
        List<ChatMessage> uniqueMessages = removeDuplicates(messages);
        int newRows = writeUntilSuccessful(uniqueMessages);
        log.info("Batch written: {} received, {} unique, {} new",
                messages.size(), uniqueMessages.size(), newRows);
    }

    /**
     * Schreibt den Stapel und versucht es so lange erneut, bis die Datenbank ihn annimmt.
     *
     * Ein Datenbankfehler darf diese Methode nie verlassen: Der Container würde den Stapel
     * sonst ablehnen, und er landete in der DLQ (defaultRequeueRejected = false). Ein Ausfall
     * sagt aber nichts über die Nachrichten — also warten wir, statt wegzuwerfen
     * (Spec 3.2.1). Die Wartezeit wächst: 1 s, 2 s, 4 s, 8 s, danach immer 10 s.
     */
    private int writeUntilSuccessful(List<ChatMessage> messages) {
        long waitMillis = FIRST_WAIT_MILLIS;
        int attempt = 1;
        while (true) {
            try {
                return messageRepository.insertAll(messages);
            } catch (DataAccessException | TransactionException exception) {
                log.warn("Database not reachable (attempt {}), retrying {} messages in {} ms: {}",
                        attempt, messages.size(), waitMillis, exception.getMessage());
                waitBeforeNextAttempt(waitMillis);
                waitMillis = nextWaitTime(waitMillis);
                attempt = attempt + 1;
            }
        }
    }

    /**
     * Wartet vor dem nächsten Versuch.
     *
     * Wird der Dienst während des Wartens beendet, kommt eine InterruptedException. Dann geht
     * der Stapel zurück in die Queue: ImmediateRequeueAmqpException ist die eine Exception,
     * bei der der Container trotz defaultRequeueRejected = false nicht in die DLQ ablehnt.
     * Die Nachrichten sind ja nicht kaputt (Spec 3.2.1).
     */
    private void waitBeforeNextAttempt(long waitMillis) {
        try {
            Thread.sleep(waitMillis);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new ImmediateRequeueAmqpException("Interrupted while waiting for the database", exception);
        }
    }

    /** Verdoppelt die Wartezeit, aber nie über 10 s hinaus. */
    private long nextWaitTime(long waitMillis) {
        long doubledWaitMillis = waitMillis * 2;
        if (doubledWaitMillis > MAX_WAIT_MILLIS) {
            return MAX_WAIT_MILLIS;
        }
        return doubledWaitMillis;
    }

    /**
     * Erste Verteidigungslinie gegen Duplikate: Kommt dieselbe id im Stapel mehrfach vor,
     * bleibt die erste übrig.
     *
     * Die zweite Linie ist ON CONFLICT in der Datenbank. Sie fängt Kopien in verschiedenen
     * Stapeln ab, die diese Schleife nie zu sehen bekommt (Spec 3.2.2).
     */
    private List<ChatMessage> removeDuplicates(List<ChatMessage> messages) {
        Map<UUID, ChatMessage> messagesById = new LinkedHashMap<>();
        for (ChatMessage message : messages) {
            messagesById.putIfAbsent(message.id(), message);
        }
        Collection<ChatMessage> uniqueMessages = messagesById.values();
        return new ArrayList<>(uniqueMessages);
    }
}
