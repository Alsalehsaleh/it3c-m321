package ch.benedict.m321.batchwriter.config;

/**
 * Die Namen der Queues an genau EINER Stelle.
 *
 * Sie müssen exakt so heissen wie im chat-service. Ein Tippfehler hiesse: Der batch-writer
 * hängt an einer leeren Queue, und keine Nachricht kommt je in der Datenbank an.
 */
public final class QueueNames {

    /** Schreibweg: Hier legt der chat-service jede Nachricht ab. */
    public static final String PERSIST_QUEUE = "chat.persist";

    /** Dead Letter: was der batch-writer endgültig nicht verarbeiten kann. */
    public static final String DEAD_LETTER_QUEUE = "chat.dlq";

    /** Diese Klasse ist eine reine Namenssammlung und wird nie erzeugt. */
    private QueueNames() {
    }
}
