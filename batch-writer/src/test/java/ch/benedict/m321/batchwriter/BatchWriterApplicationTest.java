package ch.benedict.m321.batchwriter;

import org.junit.jupiter.api.Test;

/**
 * Prüft, dass der Dienst mit echtem RabbitMQ und echtem Postgres überhaupt hochfährt.
 *
 * Schlägt nur dieser Test fehl, liegt es an der Konfiguration — application.yml,
 * Abhängigkeiten, Beans — und nicht an der Logik der anderen Tests.
 */
class BatchWriterApplicationTest extends IntegrationTestBase {

    /** Kein Assert nötig: Fährt der Kontext nicht hoch, wirft Spring und der Test wird rot. */
    @Test
    void contextLoads() {
    }
}
