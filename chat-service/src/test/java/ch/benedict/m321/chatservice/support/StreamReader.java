package ch.benedict.m321.chatservice.support;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Ein echter SSE-Client für Tests: öffnet GET /stream über HTTP und liest die
 * Antwort Zeile für Zeile mit, so wie curl -N in der Abnahme (Spec Kap. 5).
 *
 * MockMvc ist dafür ungeeignet, weil es die Antwort erst am Ende liefert. Ein
 * SSE-Stream hat aber kein Ende. Deshalb liest hier ein eigener Thread.
 *
 * Wichtig: Spring schickt die Kopfzeilen eines SseEmitter erst mit dem ersten
 * Ereignis. Der Konstruktor wartet deshalb NICHT auf die Antwort, sonst hinge
 * er, solange im Raum nichts passiert. Status und Content-Type gibt es erst,
 * wenn das erste Ereignis da ist.
 */
public class StreamReader {

    /** Wie oft beim Warten nachgeschaut wird. Kurz, damit Tests nicht unnötig warten. */
    private static final long POLL_MILLIS = 20;

    private final List<String> lines = new ArrayList<>();

    /** So lange wartet statusCode() höchstens auf die Kopfzeilen. */
    private static final long RESPONSE_TIMEOUT_SECONDS = 10;

    private final CompletableFuture<HttpResponse<InputStream>> responseFuture;

    /**
     * Öffnet den Stream und kehrt sofort zurück. Ob der Dienst den Stream schon
     * angemeldet hat, muss der Test selbst prüfen. lastEventId darf null sein.
     */
    public StreamReader(String url, String lastEventId) {
        HttpClient client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .build();
        URI uri = URI.create(url);
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(uri);
        if (lastEventId != null) {
            requestBuilder.header("Last-Event-ID", lastEventId);
        }
        HttpRequest request = requestBuilder.build();
        responseFuture = client.sendAsync(request, HttpResponse.BodyHandlers.ofInputStream());
        Thread readerThread = new Thread(this::readLines);
        readerThread.setDaemon(true);
        readerThread.start();
    }

    /** Der HTTP-Status der Antwort. Wartet, bis die Kopfzeilen da sind. */
    public int statusCode() throws InterruptedException, ExecutionException, TimeoutException {
        HttpResponse<InputStream> response = awaitResponse();
        return response.statusCode();
    }

    /** Der Content-Type der Antwort, leer, wenn keiner gesetzt ist. Wartet wie statusCode(). */
    public String contentType() throws InterruptedException, ExecutionException, TimeoutException {
        HttpResponse<InputStream> response = awaitResponse();
        Optional<String> contentType = response.headers().firstValue("Content-Type");
        return contentType.orElse("");
    }

    /**
     * Wartet, bis genau diese Zeile im Stream steht, höchstens timeout lang.
     * Eine Schleife mit Zeitgrenze statt eines festen sleep: Der Test ist so
     * schnell wie der Dienst, aber nie endlos.
     */
    public boolean waitForLine(String expectedLine, Duration timeout) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            List<String> currentLines = lines();
            if (currentLines.contains(expectedLine)) {
                return true;
            }
            Thread.sleep(POLL_MILLIS);
        }
        return false;
    }

    /** Alle bisher gelesenen Zeilen als Kopie. */
    public synchronized List<String> lines() {
        return new ArrayList<>(lines);
    }

    /**
     * Schliesst die Verbindung, wie ein Client, der weggeht. Sind die
     * Kopfzeilen noch nicht da, wird die Anfrage abgebrochen.
     */
    public void close() throws IOException {
        boolean answered = responseFuture.isDone() && !responseFuture.isCompletedExceptionally();
        if (!answered) {
            responseFuture.cancel(true);
            return;
        }
        HttpResponse<InputStream> response = responseFuture.join();
        InputStream body = response.body();
        body.close();
    }

    /** Wartet auf die Kopfzeilen, höchstens RESPONSE_TIMEOUT_SECONDS lang. */
    private HttpResponse<InputStream> awaitResponse()
            throws InterruptedException, ExecutionException, TimeoutException {
        return responseFuture.get(RESPONSE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    /** Läuft im eigenen Thread: wartet auf die Antwort und liest, bis der Stream endet. */
    private void readLines() {
        HttpResponse<InputStream> response;
        try {
            response = responseFuture.get();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            return;
        } catch (ExecutionException | CancellationException exception) {
            // Die Anfrage wurde abgebrochen oder ist gescheitert: nichts zu lesen.
            return;
        }
        InputStream body = response.body();
        InputStreamReader bodyReader = new InputStreamReader(body, StandardCharsets.UTF_8);
        BufferedReader reader = new BufferedReader(bodyReader);
        try {
            String line = reader.readLine();
            while (line != null) {
                addLine(line);
                line = reader.readLine();
            }
        } catch (IOException exception) {
            // Der Test hat die Verbindung geschlossen. Das ist das normale Ende.
        }
    }

    /** Hängt eine Zeile an. synchronized, weil der Test gleichzeitig lesen kann. */
    private synchronized void addLine(String line) {
        lines.add(line);
    }
}
