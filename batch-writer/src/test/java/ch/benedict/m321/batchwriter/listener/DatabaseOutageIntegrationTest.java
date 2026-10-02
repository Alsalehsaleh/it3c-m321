package ch.benedict.m321.batchwriter.listener;

import ch.benedict.m321.batchwriter.IntegrationTestBase;
import ch.benedict.m321.batchwriter.config.QueueNames;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.PauseContainerCmd;
import com.github.dockerjava.api.command.UnpauseContainerCmd;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.testcontainers.containers.PostgreSQLContainer;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Szenario S7 im Test: Fällt die Datenbank aus, wartet der Dienst. Er lehnt nichts ab, und
 * sobald sie zurück ist, steht jede Nachricht in der Tabelle.
 *
 * Der Postgres-Container wird dafür pausiert, nicht gestoppt. Ein gestoppter Testcontainer
 * käme mit einem neuen Port zurück, und der Dienst fände ihn unter der alten Adresse nie
 * wieder. Pausiert antwortet er einfach nicht mehr — für den Dienst ist das ein Ausfall.
 */
class DatabaseOutageIntegrationTest extends IntegrationTestBase {

    /** So lange ist die Datenbank weg: länger als ein Verbindungsversuch (5 s). */
    private static final long OUTAGE_MILLIS = 8000;

    /**
     * Pause vor dem Ausfall. Der Verbindungspool gibt eine Verbindung, die eben noch benutzt
     * wurde, ungeprüft weiter. Nach zwei Sekunden prüft er sie zuerst — so wie im Stack, wo
     * zwischen zwei Szenarien Zeit vergeht.
     */
    private static final long IDLE_MILLIS = 2000;

    /** Der Test-Postgres aus TestcontainersConfiguration, damit der Test ihn anhalten kann. */
    @Autowired
    private PostgreSQLContainer<?> postgresContainer;

    /** 300 Nachrichten während eines Ausfalls: keine in der DLQ, danach alle in der Tabelle. */
    @Test
    void messagesSurviveADatabaseOutage() throws InterruptedException {
        UUID roomId = UUID.randomUUID();
        // Wie in S7 hat der Dienst schon geschrieben, bevor die Datenbank wegfällt.
        sendMessages(roomId, 1);
        waitForRowsInRoom(roomId, 1);
        Thread.sleep(IDLE_MILLIS);

        pauseDatabase();
        try {
            sendMessages(roomId, 300);
            Thread.sleep(OUTAGE_MILLIS);
            int deadLettersDuringOutage = messagesWaitingIn(QueueNames.DEAD_LETTER_QUEUE);
            assertEquals(0, deadLettersDuringOutage);
        } finally {
            resumeDatabase();
        }

        long rows = waitForRowsInRoom(roomId, 301);
        int deadLettersAfterOutage = messagesWaitingIn(QueueNames.DEAD_LETTER_QUEUE);
        assertEquals(301, rows);
        assertEquals(0, deadLettersAfterOutage);
    }

    /** Hält alle Prozesse im Postgres-Container an. Verbindungen bleiben ohne Antwort. */
    private void pauseDatabase() {
        DockerClient dockerClient = postgresContainer.getDockerClient();
        String containerId = postgresContainer.getContainerId();
        PauseContainerCmd pauseCommand = dockerClient.pauseContainerCmd(containerId);
        pauseCommand.exec();
    }

    /** Lässt den Postgres-Container weiterlaufen: Die Datenbank ist zurück. */
    private void resumeDatabase() {
        DockerClient dockerClient = postgresContainer.getDockerClient();
        String containerId = postgresContainer.getContainerId();
        UnpauseContainerCmd unpauseCommand = dockerClient.unpauseContainerCmd(containerId);
        unpauseCommand.exec();
    }
}
