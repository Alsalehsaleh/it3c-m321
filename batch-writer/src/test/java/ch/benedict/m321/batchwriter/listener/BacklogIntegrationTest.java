package ch.benedict.m321.batchwriter.listener;

import ch.benedict.m321.batchwriter.IntegrationTestBase;
import ch.benedict.m321.batchwriter.config.QueueNames;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Szenario S4 im Test: Ein Rückstau von 1000 Nachrichten kostet die Datenbank höchstens
 * 100 Transaktionen.
 *
 * Gemessen wird wie in Spec Kap. 5 mit dem Zähler xact_commit aus pg_stat_database, als
 * Differenz von vorher und nachher. Mitgezählt wird alles, was in der Zeit committet — auch
 * die Abfragen dieses Tests. Deshalb prüft die Grenze die Grössenordnung, nicht die exakte
 * Zahl.
 */
@Slf4j
class BacklogIntegrationTest extends IntegrationTestBase {

    /** Die Grenze aus dem Auftrag. */
    private static final long MAX_TRANSACTIONS = 100;

    /**
     * Postgres meldet seine Zähler mit etwas Verzögerung. So lange warten wir vor jedem
     * Ablesen, damit der Zähler wirklich alles enthält.
     */
    private static final long STATISTICS_DELAY_MILLIS = 2000;

    /** Hält den Listener an und startet ihn wieder, wie "docker compose stop/start" in S4. */
    @Autowired
    private RabbitListenerEndpointRegistry listenerRegistry;

    /** 1000 wartende Nachrichten werden in wenigen grossen Stapeln geschrieben. */
    @Test
    void backlogIsWrittenInFewTransactions() throws InterruptedException {
        UUID roomId = UUID.randomUUID();
        listenerRegistry.stop();
        try {
            sendMessages(roomId, 1000);
            Thread.sleep(STATISTICS_DELAY_MILLIS);
            assertEquals(1000, messagesWaitingIn(QueueNames.PERSIST_QUEUE));
            long transactionsBefore = committedTransactions();

            listenerRegistry.start();
            long rows = waitForRowsInRoom(roomId, 1000);
            Thread.sleep(STATISTICS_DELAY_MILLIS);
            long transactionsAfter = committedTransactions();

            long transactions = transactionsAfter - transactionsBefore;
            log.info("Backlog of 1000 messages cost {} transactions", transactions);
            assertEquals(1000, rows);
            assertTrue(transactions <= MAX_TRANSACTIONS, "Transaktionen: " + transactions);
        } finally {
            listenerRegistry.start();
        }
    }

    /** Liest den Zähler aller abgeschlossenen Transaktionen dieser Datenbank. */
    private long committedTransactions() {
        Long transactions = jdbcTemplate.queryForObject(
                "select xact_commit from pg_stat_database where datname = current_database()",
                Long.class);
        return transactions;
    }
}
