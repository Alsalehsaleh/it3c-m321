package ch.benedict.m321.batchwriter;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.RabbitMQContainer;

/**
 * Die echten Gegenstellen für alle Tests: ein RabbitMQ und ein Postgres im Container.
 *
 * Die Container sind Spring-Beans. Spring startet sie einmal pro Testkontext, und weil
 * alle Integrationstests denselben Kontext benutzen, laufen sie für den ganzen Testlauf
 * nur je einmal. "@ServiceConnection" trägt Adresse, Benutzer und Passwort der Container
 * in die Konfiguration ein — application.yml muss davon nichts wissen.
 */
@TestConfiguration
class TestcontainersConfiguration {

    /** Dasselbe Image wie in docker-compose.yml, damit Test und Stack übereinstimmen. */
    @Bean
    @ServiceConnection
    RabbitMQContainer rabbitMqContainer() {
        return new RabbitMQContainer("rabbitmq:3.13-management");
    }

    /** Dasselbe Image wie in docker-compose.yml, damit Test und Stack übereinstimmen. */
    @Bean
    @ServiceConnection
    PostgreSQLContainer<?> postgresContainer() {
        return new PostgreSQLContainer<>("postgres:17");
    }
}
