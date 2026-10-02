package ch.benedict.m321.batchwriter;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Startpunkt des batch-writer.
 *
 * Dieser Dienst ist der einzige, der in die Datenbank schreibt. Er hat keinen Webserver
 * und keinen Port: Seine einzige Eingangstür ist die Queue chat.persist.
 */
@SpringBootApplication
public class BatchWriterApplication {

    /** Startet Spring. Alles Weitere erledigen die Beans in config, listener und repository. */
    public static void main(String[] args) {
        SpringApplication.run(BatchWriterApplication.class, args);
    }
}
