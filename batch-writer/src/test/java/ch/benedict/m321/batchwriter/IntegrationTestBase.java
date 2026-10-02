package ch.benedict.m321.batchwriter;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

/**
 * Gemeinsame Grundlage aller Integrationstests.
 *
 * Alle Tests, die von dieser Klasse erben, teilen sich EINEN Spring-Kontext und damit
 * dieselben Container. Das spart pro Testklasse das Hochfahren von RabbitMQ und Postgres.
 * Weil der Kontext geteilt ist, arbeitet jeder Test in einem eigenen, zufälligen Raum
 * und zählt nur dessen Zeilen.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
public abstract class IntegrationTestBase {
}
