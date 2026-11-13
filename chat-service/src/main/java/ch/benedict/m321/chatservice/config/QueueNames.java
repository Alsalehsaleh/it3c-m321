package ch.benedict.m321.chatservice.config;

/**
 * Die Namen der Queues und des Exchange an genau EINER Stelle.
 *
 * Sie werden an drei Orten gebraucht — beim Anlegen, beim Senden und im
 * Test. Ein Tippfehler in einem String wäre sonst erst zur Laufzeit
 * sichtbar, und zwar als "die Nachricht kommt nie an".
 */
public final class QueueNames {

    /** Schreibweg: hier holt der batch-writer die Nachrichten ab. */
    public static final String PERSIST_QUEUE = "chat.persist";

    /**
     * Zustellweg: fanout an alle Instanzen des chat-service. Jede hat ihre
     * eigene Zustell-Queue daran und stellt per SSE zu (Spec 1.4).
     */
    public static final String DELIVERY_EXCHANGE = "chat.delivery";

    /**
     * Anfang des Namens jeder Zustell-Queue. Den Rest hängt jede Instanz beim
     * Start an: Hostname und eine zufällige UUID (Spec 4).
     */
    public static final String DELIVERY_QUEUE_PREFIX = "chat.delivery.";

    /** Dead Letter: was der batch-writer endgültig nicht verarbeiten kann. */
    public static final String DEAD_LETTER_QUEUE = "chat.dlq";

    /** Diese Klasse ist eine reine Namenssammlung und wird nie erzeugt. */
    private QueueNames() {
    }
}
