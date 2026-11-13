# Live-Zustellung — Umsetzungsplan

**Ziel:** Jede Instanz des `chat-service` holt sich über eine eigene Queue ihre Kopie jeder
Nachricht vom Fanout-Exchange `chat.delivery` und schickt sie per Server-Sent Events an alle
offenen Streams des Raums (`GET /stream?roomId=…`). Wer mit `Last-Event-ID` neu verbindet,
bekommt die verpassten Nachrichten aus einem Puffer nachgeliefert (Szenarien S3 bis S7).

**Architektur:** `DeliveryListener` liest die Zustell-Queue der Instanz und gibt jede Nachricht
an `RoomStreamRegistry`. Die Registry hält pro Raum ein `RoomStream`-Objekt: den Puffer der
letzten 200 Nachrichten und die Liste der offenen Streams, geschützt durch ein Schloss pro
Raum. `StreamController` öffnet den Stream und meldet ihn beim `RoomStream` an.
`HeartbeatSender` schickt alle 15 s ein Lebenszeichen. Queue und Bindung entstehen als `@Bean`
in `RabbitConfig`.

**Tech-Stack:** Java 21, Spring Boot 3.5.16, Spring MVC (`SseEmitter`), Spring AMQP, RabbitMQ
3.13, JUnit 5, Testcontainers.

**Spec:** [`spec-live-zustellung.md`](spec-live-zustellung.md). Vorbild für den Aufbau:
[`plan-batch-writer.md`](plan-batch-writer.md).

## Globale Vorgaben

Diese Punkte gelten für **jede** Aufgabe:

- **Code auf Englisch** — Klassen, Methoden, Variablen, Log-Meldungen. **Alles andere auf
  Deutsch** — Kommentare, Javadoc, Commit-Messages.
- **Ein Ergebnis pro Zeile**, keine verschachtelten Aufrufe. **Keine Streams**, sondern
  `for`-Schleifen. Gilt auch in Tests.
- **Über jeder Klasse und jeder Methode ein Kommentar**, der das *Warum* erklärt — auch über
  jeder Testmethode. S8 prüft das mit dem Befehl aus Spec Kap. 5.
- **Lombok** für `@Slf4j` und `@RequiredArgsConstructor`, **`record`** für Datenklassen.
  **Kein Interface** mit nur einer Implementierung — auch nicht für Tests.
- **Kein `ports:`** und **kein `container_name:`** für den `chat-service`.
- **Tests mit echtem RabbitMQ** über Testcontainers, wie die bestehenden Tests. **Kein festes
  `sleep`** in Tests: Wer auf etwas wartet, fragt in einer Schleife mit Zeitgrenze nach.
- **Jede Aufgabe endet mit genau einem Commit.** Die Betreffzeile steht bei der Aufgabe,
  darunter erklärt der Commit-Text, warum. Jeder Commit endet mit
  `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- **Voraussetzung:** Docker läuft. Testcontainers startet für die Tests echte
  RabbitMQ-Container.

Einzelne Tests laufen mit `mvn -q -pl chat-service test -Dtest=<Testklasse>` aus dem
Wurzelverzeichnis.

## So arbeite ich mit der KI

Pro Aufgabe immer derselbe Ablauf:

1. Ich gebe der KI den **Auftrag**, genau wie er unten steht.
2. Die KI schreibt zuerst den Test und zeigt mir, dass er **rot** ist.
3. Die KI schreibt den Code, bis der Test **grün** ist.
4. Ich lese Test und Code selbst durch, lasse den Test noch einmal laufen und prüfe gegen die
   Spec und CLAUDE.md. Was nicht passt, lasse ich korrigieren oder ändere es selbst.
5. Ich trage unter **«Geprüft und korrigiert»** ein, was ich geprüft und was ich geändert habe
   und warum.
6. Erst dann wird committet — Code und Prüfnotiz zusammen.

Dieser Plan wird committet und gepusht, **bevor** Aufgabe 1 beginnt. Danach ändere ich am
bestehenden Text nichts mehr, ausser im Feld «Geprüft und korrigiert». Muss ich umplanen,
kommt ein neuer Eintrag mit Änderungsgrund unter «Änderungen am Plan», in einem eigenen
Commit.

## Abgrenzung

| Bewusst **nicht** in diesem Plan | Warum |
|---|---|
| Keycloak, `web-gateway`, Token prüfen | Laut Auftrag nicht Teil dieser Aufgabe (Spec 1.3) |
| React-Oberfläche, JavaFX-Client | Nicht Teil dieser Aufgabe. Geprüft wird mit `curl` |
| Räume anlegen, Mitglieder einladen | Nicht Teil dieser Aufgabe. Jede gültige UUID gilt als Raum (Spec 1.3) |
| Nachholen aus der Datenbank | Verworfen in Spec 3.2.5. Der `chat-service` bekommt keinen Datenbankzugang |
| Änderungen am `batch-writer` | Der Schreibweg bleibt, wie er ist (Spec 4) |
| Neue Variablen in `.env.example` | Es gibt keine. `HOSTNAME` setzt Docker selbst (Spec 4.1) |

## Dateistruktur

```
README.md                                   # Tabelle «Stand» nachführen
chat-service/
└── src/
    ├── main/java/ch/benedict/m321/chatservice/
    │   ├── config/QueueNames.java          # + Präfix der Zustell-Queue, Kommentar ohne web-gateway
    │   ├── config/RabbitConfig.java        # + Zustell-Queue und Bindung als @Bean
    │   ├── config/SchedulingConfig.java    # @EnableScheduling für das Lebenszeichen
    │   ├── controller/StreamController.java
    │   ├── dto/BufferedMessage.java        # record: id und Body als Text
    │   ├── listener/DeliveryListener.java
    │   └── service/
    │       ├── RoomStream.java             # Puffer, Liste der Streams, Schloss pro Raum
    │       ├── RoomStreamRegistry.java     # ein RoomStream pro roomId
    │       └── HeartbeatSender.java
    ├── main/resources/application.yml      # + Listener-Werte, + HOSTNAME
    └── test/java/ch/benedict/m321/chatservice/
        ├── support/RecordingEmitter.java          # SseEmitter, der mitschreibt, was gesendet wird
        ├── support/StreamReader.java              # liest einen echten SSE-Stream Zeile für Zeile
        ├── service/RoomStreamTest.java            # Aufgaben 1, 2, 9
        ├── service/RoomStreamRegistryTest.java    # Aufgabe 3
        ├── service/HeartbeatSenderTest.java       # Aufgabe 9
        ├── controller/StreamControllerIntegrationTest.java     # Aufgabe 4
        ├── config/DeliveryQueueIntegrationTest.java            # Aufgabe 5
        ├── listener/DeliveryListenerIntegrationTest.java       # Aufgaben 6, 10
        └── TwoInstancesIntegrationTest.java                    # Aufgaben 7, 8 — S3, S4, S7
```

---

## Aufgabe 1: RoomStream — Puffer und Live-Zustellung

**Ziel:** Ein `RoomStream` nimmt Nachrichten an, hält die letzten 200 im Puffer und schickt jede
an alle angemeldeten Streams.

**Warum an dieser Stelle:** Hier steckt der Kern von S3 und S7, und er lässt sich ohne RabbitMQ
und ohne HTTP testen — schnell und ohne Container. Alles Weitere (Controller, Listener) gibt nur
Nachrichten hinein oder meldet Streams an.

**Dateien:**
- Anlegen: `dto/BufferedMessage.java`, `service/RoomStream.java`
- Test: `support/RecordingEmitter.java`, `service/RoomStreamTest.java`

**Was der Test prüft:**
- Eine Nachricht geht an alle angemeldeten Streams, mit `id` und unverändertem Body.
- Ein abgemeldeter Stream bekommt nichts mehr.
- Nach 201 Nachrichten hält der Puffer genau 200, die älteste ist weg.
- Scheitert das Senden an einen Stream, bekommen die anderen die Nachricht trotzdem, und der
  kaputte Stream ist ausgetragen (Spec 3.2.6).

**Auftrag an die KI (Wortlaut):**

```text
Lies docs/spec-live-zustellung.md, Abschnitte 2.2, 3.1, 3.2.5 und 3.2.6, und CLAUDE.md.
Baue im chat-service:
- record dto/BufferedMessage(UUID id, String body).
- Klasse service/RoomStream mit den Methoden publish(BufferedMessage), subscribe(SseEmitter)
  und unsubscribe(SseEmitter). Puffer: die letzten 200 Nachrichten (Konstante), älteste fällt
  raus. Alle Methoden synchronized — das ist das Schloss pro Raum. publish legt die Nachricht
  in den Puffer und sendet sie dann an eine KOPIE der Liste der Streams, als Ereignis mit
  id = id der Nachricht und data = body unverändert. Scheitert das Senden an einen Stream
  (IOException oder IllegalStateException), wird nur dieser Stream ausgetragen, mit Log-Zeile.
  Last-Event-ID ist NICHT Teil dieses Schritts.
Zuerst den Test service/RoomStreamTest und im Testordner support/RecordingEmitter: eine
Unterklasse von SseEmitter, die send(...) überschreibt und mitschreibt, was gesendet wurde,
und auf Wunsch beim Senden eine IOException wirft. Kein Interface, kein Mockito.
Zeig mir den roten Test, bevor du den Code schreibst. Regeln: keine Streams, Kommentar auf
Deutsch über jeder Klasse und Methode, ein Ergebnis pro Zeile.
Nicht committen, bis ich es geprüft habe. Danach Commit-Message:
"feat: RoomStream hält Puffer und Streams eines Raums" mit Co-Authored-By-Zeile.
```

- [ ] **Schritt 1: Test schreiben lassen**
- [ ] **Schritt 2: Test rot sehen** — kompiliert nicht, weil `RoomStream` fehlt.
- [ ] **Schritt 3: Code schreiben lassen**
- [ ] **Schritt 4: Test grün sehen** — `-Dtest=RoomStreamTest`
- [ ] **Schritt 5: Committen** — `feat: RoomStream hält Puffer und Streams eines Raums`

**Geprüft und korrigiert:** Rot gesehen (Kompilierfehler, `BufferedMessage` fehlt), danach
grün: 4 Tests, 0 Fehler. Geprüft hat der Council (KI-Review) gegen Spec 2.2, 3.2.5, 3.2.6
und CLAUDE.md: Ereignis genau `id:<id>`, `data:<body>`, Leerzeile, ohne Leerzeichen; Puffer
wirft bei 201 die älteste raus; gesendet wird über eine Kopie der Liste, weil das Austragen
im selben Thread sonst eine `ConcurrentModificationException` gäbe; `IOException` und
`IllegalStateException` (Stream schon beendet) tragen nur diesen Stream aus. Keine Streams,
Kommentar über jeder Klasse und Methode. Korrigiert: nichts. `copyOfBuffer()` ist nur
paketweit sichtbar und dient dem Test; das ist bewusst so gelassen.

---

## Aufgabe 2: Nachholen nach Last-Event-ID

**Ziel:** `subscribe` bekommt die zuletzt empfangene ID und schickt zuerst alle Nachrichten
danach aus dem Puffer, dann geht es live weiter — ohne Lücke und ohne Doppelte.

**Warum an dieser Stelle:** Das ist das Herz von S7 und gehört in denselben `RoomStream`.
Als eigener Schritt, weil es ein eigenes Thema ist: die Nahtstelle zwischen Nachholen und live
(Spec 3.2.5).

**Dateien:**
- Ändern: `service/RoomStream.java`
- Test: `service/RoomStreamTest.java`

**Was der Test prüft:**
- ID im Puffer: genau die Nachrichten danach, in Reihenfolge, danach live.
- Unbekannte ID, leere ID oder keine ID: nichts nachgeholt, nur live (Spec 2.3).
- Nahtstelle: Ein zweiter Thread ruft `publish` auf, während `subscribe` nachholt. Jede
  Nachricht kommt genau einmal an, in der richtigen Reihenfolge.

**Auftrag an die KI (Wortlaut):**

```text
Lies docs/spec-live-zustellung.md, Abschnitte 2.3 und 3.2.5.
Erweitere service/RoomStream: subscribe(SseEmitter emitter, String lastEventId).
Im selben Schloss wie publish: zuerst alle Nachrichten NACH lastEventId aus dem Puffer an
diesen Stream senden, DANN den Stream in die Liste eintragen. Ist lastEventId null, leer,
keine UUID oder nicht im Puffer: nichts nachholen, live weiter, eine Log-Zeile auf INFO.
Kein Fehler an den Client.
Zuerst die Tests in RoomStreamTest, auch einen Test für die Nahtstelle: ein zweiter Thread
publiziert, während nachgeholt wird; am Ende muss jede Nachricht genau einmal und in
Reihenfolge angekommen sein. Kein Thread.sleep als Synchronisation, nimm CountDownLatch.
Zeig mir die roten Tests, bevor du den Code änderst. Regeln aus CLAUDE.md wie in Aufgabe 1.
Nicht committen, bis ich es geprüft habe. Danach Commit-Message:
"feat: verpasste Nachrichten nach Last-Event-ID nachholen" mit Co-Authored-By-Zeile.
```

- [ ] **Schritt 1: Tests schreiben lassen**
- [ ] **Schritt 2: Tests rot sehen** — `subscribe` mit zwei Parametern fehlt.
- [ ] **Schritt 3: Code schreiben lassen**
- [ ] **Schritt 4: Tests grün sehen** — `-Dtest=RoomStreamTest`
- [ ] **Schritt 5: Committen** — `feat: verpasste Nachrichten nach Last-Event-ID nachholen`

**Geprüft und korrigiert:** Rot gesehen (`subscribe` mit zwei Parametern fehlte), danach
grün: 7 Tests, 0 Fehler. Geprüft hat der Council (KI-Review) gegen Spec 2.3 und 3.2.5:
`subscribe` holt im selben Schloss wie `publish` zuerst nach und trägt erst dann ein; der
Nahtstellen-Test mit zweitem Thread bekommt 11 bis 200 genau einmal und in Reihenfolge;
`null`, leer, keine UUID und unbekannte ID ergeben nur eine Log-Zeile und keinen Fehler.
Korrigiert am Vorschlag: `sendMessage` trug einen kaputten Stream selbst aus. Das passte
nicht mehr, weil `subscribe` den Stream beim Nachholen noch gar nicht in der Liste hat.
Jetzt heisst die Methode `trySend`, gibt `true`/`false` zurück, und der Aufrufer
entscheidet; die Log-Meldung lautet entsprechend «Dropping» statt «Removing».

---

## Aufgabe 3: RoomStreamRegistry — ein RoomStream pro Raum

**Ziel:** Zu jeder `roomId` gibt es genau ein `RoomStream`-Objekt. Es entsteht beim ersten
Bedarf.

**Warum an dieser Stelle:** Controller und Listener müssen denselben `RoomStream` finden. Ohne
Registry gäbe es zwei Puffer für denselben Raum, und das Schloss pro Raum wäre wirkungslos.

**Dateien:**
- Anlegen: `service/RoomStreamRegistry.java`
- Test: `service/RoomStreamRegistryTest.java`

**Was der Test prüft:**
- Zweimal dieselbe `roomId` ergibt dasselbe Objekt, zwei verschiedene ergeben zwei.
- Zwei Threads, die gleichzeitig dieselbe neue `roomId` anfragen, bekommen dasselbe Objekt.

**Auftrag an die KI (Wortlaut):**

```text
Lies docs/spec-live-zustellung.md, Abschnitt 3.1 (Tabelle "Die neuen Klassen").
Baue service/RoomStreamRegistry als Spring-@Component mit einer Methode
RoomStream getOrCreate(UUID roomId). Gleiche roomId -> dasselbe Objekt, auch wenn zwei Threads
gleichzeitig fragen. Wähle die einfachste lesbare Lösung (z. B. synchronized-Methode mit
HashMap) und begründe sie im Kommentar.
Zuerst den Test service/RoomStreamRegistryTest, rot zeigen, dann den Code.
Regeln aus CLAUDE.md wie in Aufgabe 1.
Nicht committen, bis ich es geprüft habe. Danach Commit-Message:
"feat: RoomStreamRegistry hält einen RoomStream pro Raum" mit Co-Authored-By-Zeile.
```

- [ ] **Schritt 1: Test schreiben lassen**
- [ ] **Schritt 2: Test rot sehen** — kompiliert nicht, weil die Klasse fehlt.
- [ ] **Schritt 3: Code schreiben lassen**
- [ ] **Schritt 4: Test grün sehen** — `-Dtest=RoomStreamRegistryTest`
- [ ] **Schritt 5: Committen** — `feat: RoomStreamRegistry hält einen RoomStream pro Raum`

**Geprüft und korrigiert:** Rot gesehen (`RoomStreamRegistry` fehlte), danach grün: 2 Tests,
0 Fehler. Geprüft hat der Council (KI-Review): `getOrCreate` ist `synchronized`, der Test
mit zwei gleichzeitigen Threads läuft 200 Mal und bekommt jedes Mal dasselbe Objekt.
Bewusst `HashMap` mit `synchronized` statt `ConcurrentHashMap.computeIfAbsent` mit Lambda,
weil es sich laut vorlesen lässt (CLAUDE.md). Das Schloss der Registry hält nur das
Nachschlagen, gesendet wird ausserhalb. Korrigiert: nichts.

---

## Aufgabe 4: StreamController — GET /stream

**Ziel:** Der Endpunkt `GET /stream?roomId=…` öffnet einen SSE-Stream ohne Zeitlimit und meldet
ihn beim `RoomStream` des Raums an. Eine fehlende oder ungültige `roomId` ergibt `400`.

**Warum an dieser Stelle:** Jetzt gibt es etwas, woran sich ein Stream anmelden kann. Der
Endpunkt ist der feste Punkt, an dem das Prüfskript ansetzt — er muss stehen, bevor Nachrichten
aus RabbitMQ kommen.

**Dateien:**
- Anlegen: `controller/StreamController.java`
- Test: `support/StreamReader.java`, `controller/StreamControllerIntegrationTest.java`

**Was der Test prüft:**
- `GET /stream?roomId=<uuid>` antwortet `200` mit `Content-Type: text/event-stream`.
- Ohne `roomId` und mit `roomId=abc`: `400`.
- Eine Nachricht, die direkt in den `RoomStream` gegeben wird, kommt im echten Stream als
  `id:` und `data:` an — ohne Leerzeichen nach dem Doppelpunkt (Spec 2.2).
- Wird der Stream geschlossen, ist er beim `RoomStream` ausgetragen.

**Auftrag an die KI (Wortlaut):**

```text
Lies docs/spec-live-zustellung.md, Abschnitte 2.2, 2.3 und 3.2.6.
Baue controller/StreamController mit GET /stream, Parameter @RequestParam UUID roomId und
optionalem Header Last-Event-ID. Erzeuge new SseEmitter(0L) (Konstante mit Kommentar: 0 heisst
"nie", sonst bricht Tomcat nach 30 s ab). Registriere onCompletion, onTimeout und onError, die
den Stream beim RoomStream austragen. Dann roomStream.subscribe(emitter, lastEventId).
Antwort-Typ text/event-stream.
Zuerst den Test controller/StreamControllerIntegrationTest: @SpringBootTest mit
webEnvironment RANDOM_PORT und RabbitMQ per Testcontainers wie in den bestehenden Tests.
Im Testordner support/StreamReader: liest einen echten Stream mit java.net.http.HttpClient
Zeile für Zeile und wartet mit Zeitgrenze auf eine bestimmte Zeile. 400-Fälle mit MockMvc
oder HttpClient. Rot zeigen, dann den Code.
Regeln aus CLAUDE.md wie in Aufgabe 1.
Nicht committen, bis ich es geprüft habe. Danach Commit-Message:
"feat: Endpunkt GET /stream öffnet einen SSE-Stream pro Raum" mit Co-Authored-By-Zeile.
```

- [ ] **Schritt 1: Test schreiben lassen**
- [ ] **Schritt 2: Test rot sehen** — `404` statt `200`, weil es den Endpunkt nicht gibt.
- [ ] **Schritt 3: Code schreiben lassen**
- [ ] **Schritt 4: Test grün sehen** — `-Dtest=StreamControllerIntegrationTest`
- [ ] **Schritt 5: Committen** — `feat: Endpunkt GET /stream öffnet einen SSE-Stream pro Raum`

**Geprüft und korrigiert:** Rot gesehen (4 Fehler, überall `404`), danach grün: 4 Tests,
0 Fehler. Geprüft hat der Council (KI-Review). Drei Korrekturen, alle im Test:
1. Der erste grüne Lauf hing. Ursache: Spring schickt die Kopfzeilen eines `SseEmitter` erst
   mit dem ersten Ereignis, der `StreamReader` wartete beim Öffnen aber darauf. Jetzt öffnet
   er asynchron, und der Test publiziert zuerst eine Nachricht. Der Controller blieb gleich.
2. Für den Test «geschlossener Stream wird ausgetragen» kam `RoomStream.openStreamCount()`
   dazu, schon mit dem Test, damit «rot» am fehlenden Endpunkt lag und nicht am Kompilieren.
3. Am Ende wartete Spring wegen `graceful` Shutdown 30 s auf offene Streams, und Surefire
   beendete die JVM hart. Nur in diesem Test gilt jetzt `server.shutdown=immediate`; im
   Betrieb bleibt `graceful` (Spec 3.2.2).
Offen benannt: Tomcat protokolliert einen abgerissenen Client zusätzlich als `ERROR` mit
Stacktrace. Der Dienst behandelt ihn richtig (Stream ausgetragen), das Log ist nur laut.

---

## Aufgabe 5: Zustell-Queue pro Instanz und Bindung an chat.delivery

**Ziel:** Jede Instanz legt beim Start ihre eigene Zustell-Queue an, mit dem Namen
`chat.delivery.<HOSTNAME>.<UUID>` und den Eigenschaften aus Spec 4, und bindet sie an
`chat.delivery`.

**Warum an dieser Stelle:** Ohne gebundene Queue verwirft RabbitMQ die Kopie (Spec 1.1). Der
Listener in Aufgabe 6 braucht eine Queue, aus der er lesen kann. Die Eigenschaften (Grenze,
Aufräumen) sind die Grundlage für S4 und S6.

**Dateien:**
- Ändern: `config/QueueNames.java`, `config/RabbitConfig.java`, `application.yml`
- Test: `config/DeliveryQueueIntegrationTest.java`

**Was der Test prüft:**
- Die Queue existiert, ihr Name beginnt mit `chat.delivery.`.
- Eine Nachricht an den Exchange `chat.delivery` landet in dieser Queue.
- Über die Management-API des Test-Containers: `durable = false`, `auto_delete = true`,
  `x-expires = 10000`, `x-max-length = 900`, `x-overflow = drop-head`, keine Dead-Letter-Route.

**Auftrag an die KI (Wortlaut):**

```text
Lies docs/spec-live-zustellung.md, Abschnitte 3.2.2, 3.2.4 und 4 mit 4.1.
In config/QueueNames ein Präfix "chat.delivery." für die Zustell-Queues ergänzen.
In config/RabbitConfig:
- Ein @Bean für die Zustell-Queue. Der Name wird EINMAL beim Start gebildet:
  "chat.delivery." + Hostname + "." + UUID.randomUUID(). Hostname aus application.yml
  (instance.hostname: ${HOSTNAME:local}). Name beim Start auf INFO loggen.
  Eigenschaften: nicht durable, autoDelete, x-expires 10000, x-max-length 900,
  x-overflow drop-head, KEINE Dead-Letter-Route. Zahlen als Konstanten mit Kommentar,
  der auf die Spec verweist.
- Ein @Bean für die Bindung an den bestehenden FanoutExchange chat.delivery.
Den Listener NICHT in diesem Schritt.
Zuerst den Test config/DeliveryQueueIntegrationTest mit RabbitMQ per Testcontainers. Die
Eigenschaften der Queue über die HTTP-Management-API des Containers prüfen
(GET /api/queues/%2F/<name>). Rot zeigen, dann den Code.
Regeln aus CLAUDE.md wie in Aufgabe 1.
Nicht committen, bis ich es geprüft habe. Danach Commit-Message:
"feat: eigene Zustell-Queue pro Instanz an chat.delivery binden" mit Co-Authored-By-Zeile.
```

- [ ] **Schritt 1: Test schreiben lassen**
- [ ] **Schritt 2: Test rot sehen** — es gibt keine Queue mit dem Präfix.
- [ ] **Schritt 3: Code schreiben lassen**
- [ ] **Schritt 4: Test grün sehen** — `-Dtest=DeliveryQueueIntegrationTest`, danach alle
  Tests des `chat-service`: die bestehenden dürfen nicht rot werden.
- [ ] **Schritt 5: Committen** — `feat: eigene Zustell-Queue pro Instanz an chat.delivery binden`

**Geprüft und korrigiert:** Rot gesehen (3 Fehler, kein Bean `deliveryQueue`), danach grün:
3 Tests, 0 Fehler; alle 11 Testklassen des `chat-service` bleiben grün. Geprüft hat der
Council (KI-Review) gegen Spec 3.2.2, 3.2.4 und 4: Die Management-API des Brokers zeigt
`durable = false`, `auto_delete = true`, `x-expires = 10000`, `x-max-length = 900`,
`x-overflow = drop-head`, keine Dead-Letter-Route; Name `chat.delivery.<Hostname>.<UUID>`,
unter Windows `local`. Zwei Korrekturen am Test:
1. Statt «eine Nachricht landet in der Queue» prüft der Test die **Bindung** über die
   Management-API. Ab Aufgabe 6 liest der Listener die Queue leer, eine Zählung der
   Nachrichten würde dann zufällig scheitern. Die Bindung beweist dasselbe.
2. Der erste Lauf nach dem Code war noch rot (Queue im Broker `404`): Spring legt Queues
   erst beim ersten Verbindungsaufbau an. Der Test ruft deshalb `rabbitAdmin.initialize()`
   auf. Im Betrieb öffnet ab Aufgabe 6 der Listener die Verbindung beim Start.

---

## Aufgabe 6: DeliveryListener — vom Broker in den Stream

**Ziel:** Der Listener liest die Zustell-Queue mit genau einem Thread und gibt jede Nachricht mit
unverändertem Body an den `RoomStream` ihres Raums. Ein `POST /messages` erscheint damit im
Stream.

**Warum an dieser Stelle:** Jetzt sind beide Enden da — Queue und Stream. Dieser Schritt
verbindet sie und schliesst den Weg vom `POST` bis zum `data:` (Spec 3.1).

**Dateien:**
- Anlegen: `listener/DeliveryListener.java`
- Ändern: `application.yml` (Listener-Werte)
- Test: `listener/DeliveryListenerIntegrationTest.java`

**Was der Test prüft:**
- `POST /messages` für Raum A: Der Stream von Raum A bekommt das Ereignis, der Stream von
  Raum B nicht.
- `data:` ist Zeichen für Zeichen der Body, der auch in `chat.persist` liegt (Spec 2.1).
- 50 Nachrichten eines Absenders kommen in der gesendeten Reihenfolge an.

**Auftrag an die KI (Wortlaut):**

```text
Lies docs/spec-live-zustellung.md, Abschnitte 2.1, 3.1, 3.2.1 und 4.1.
Baue listener/DeliveryListener mit @RabbitListener auf die Zustell-Queue aus Aufgabe 5
(Queue-Name aus dem Bean, im Kommentar erklären, wie). Die Methode bekommt die Nachricht
als ChatMessage (für id und roomId) UND als org.springframework.amqp.core.Message. Der Body
geht als UTF-8-Text unverändert in eine BufferedMessage und an
roomStreamRegistry.getOrCreate(roomId).publish(...). Die Nachricht NICHT neu in JSON
schreiben.
In application.yml unter spring.rabbitmq.listener.simple: concurrency 1, max-concurrency 1,
prefetch 50, default-requeue-rejected false — jeweils mit Kommentar und Verweis auf die Spec.
Zuerst den Test listener/DeliveryListenerIntegrationTest (RANDOM_PORT, Testcontainers,
StreamReader aus Aufgabe 4): Stream öffnen, POST /messages, Ereignis prüfen; Body mit dem
Body aus chat.persist vergleichen; zweiter Raum bekommt nichts; Reihenfolge von 50
Nachrichten. Rot zeigen, dann den Code.
Regeln aus CLAUDE.md wie in Aufgabe 1.
Nicht committen, bis ich es geprüft habe. Danach Commit-Message:
"feat: DeliveryListener stellt Nachrichten an die Streams ihres Raums zu" mit
Co-Authored-By-Zeile.
```

- [ ] **Schritt 1: Test schreiben lassen**
- [ ] **Schritt 2: Test rot sehen** — der Stream bekommt kein Ereignis.
- [ ] **Schritt 3: Code schreiben lassen**
- [ ] **Schritt 4: Test grün sehen** — `-Dtest=DeliveryListenerIntegrationTest`
- [ ] **Schritt 5: Committen** — `feat: DeliveryListener stellt Nachrichten an die Streams ihres Raums zu`

**Geprüft und korrigiert:** Rot gesehen (3 Fehler, der Stream bekam kein Ereignis), danach
grün: 3 Tests, 0 Fehler; alle 12 Testklassen des `chat-service` grün (33 Tests). Geprüft
hat der Council (KI-Review) gegen Spec 2.1, 3.1, 3.2.1 und 4.1: `data:` ist Zeichen für
Zeichen der Body aus `chat.persist` (der Test holt ihn dort ab und vergleicht); Raum B
bekommt nichts; 50 Nachrichten kommen in Reihenfolge. Der Listener liest mit
`concurrency 1`, `prefetch 50`, `default-requeue-rejected false`, jeweils mit Kommentar
in `application.yml`. Korrigiert: nichts. Hinweis: `@Payload` steht ausdrücklich am
Parameter `ChatMessage`, damit man ohne Spring-Wissen sieht, welcher Parameter
umgewandelt wird.

---

## Aufgabe 7: Zwei Instanzen — S3 als Test

**Ziel:** Ein Test startet zwei Instanzen des `chat-service` am selben RabbitMQ und zeigt: Beide
A-Clients bekommen alle Nachrichten genau einmal, pro Absender in Reihenfolge, der B-Client
keine.

**Warum an dieser Stelle:** Erst jetzt ist der ganze Weg gebaut. Dieser Test ist die Pflicht
«Zwei-Instanzen-Test» (Teil D) und deckt den Fall ab, an dem eine gemeinsame Queue scheitern
würde.

**Dateien:**
- Test: `TwoInstancesIntegrationTest.java`

**Was der Test prüft:**
- Zwei Kontexte, je mit eigenem Port, legen zwei verschiedene Zustell-Queues an.
- Je ein Stream für Raum A an jeder Instanz, einer für Raum B. Je 20 Nachrichten von zwei
  Absendern über jede Instanz. Beide A-Streams: 40 Ereignisse, 40 verschiedene IDs, pro
  Absender in Reihenfolge. B-Stream: 0.

**Besonderheit — der Test ist sofort grün.** Dieser Schritt bringt keinen neuen Produktivcode,
«rot sehen» geht deshalb nicht. Statt dessen eine **Gegenprobe**: Den Queue-Namen in
`RabbitConfig` vorübergehend fest machen (ohne UUID) — dann teilen sich beide Instanzen eine
Queue, und der Test muss rot werden. Danach zurückstellen. Das Ergebnis der Gegenprobe kommt
in «Geprüft und korrigiert».

**Auftrag an die KI (Wortlaut):**

```text
Lies docs/spec-live-zustellung.md, Abschnitte 3.2.1 und 4, und Spec Kap. 5 S3.
Schreibe TwoInstancesIntegrationTest: Ein RabbitMQ-Container per Testcontainers. Zwei
Instanzen mit new SpringApplicationBuilder(ChatServiceApplication.class), server.port=0 und
den Verbindungsdaten des Containers, nach dem Test beide schliessen. An jeder Instanz ein
Stream für Raum A, an einer ein Stream für Raum B (StreamReader). Je 20 Nachrichten über
jede Instanz, von zwei verschiedenen Absendern. Prüfen: beide A-Streams 40 Ereignisse,
40 verschiedene IDs, Reihenfolge pro Absender; B-Stream 0 Ereignisse; zwei verschiedene
Zustell-Queues existieren.
Es gibt keinen neuen Produktivcode. Mach danach die Gegenprobe: Queue-Name vorübergehend ohne
UUID, Test muss rot sein, zeig mir das, dann zurückstellen.
Regeln aus CLAUDE.md wie in Aufgabe 1.
Nicht committen, bis ich es geprüft habe. Danach Commit-Message:
"test: zwei Instanzen stellen jedem Raum alle Nachrichten zu" mit Co-Authored-By-Zeile.
```

- [ ] **Schritt 1: Test schreiben lassen**
- [ ] **Schritt 2: Test grün sehen** — `-Dtest=TwoInstancesIntegrationTest`
- [ ] **Schritt 3: Gegenprobe rot sehen** — mit festem Queue-Namen, danach zurückstellen
- [ ] **Schritt 4: Committen** — `test: zwei Instanzen stellen jedem Raum alle Nachrichten zu`

**Geprüft und korrigiert:** Grün: 1 Test, 0 Fehler, zwei Instanzen mit den Queues
`chat.delivery.local.35faa16e-…` und `chat.delivery.local.c747e008-…`. Gegenprobe mit festem
Queue-Namen ohne UUID: rot («expected: not equal but was: chat.delivery.local»), danach
zurückgestellt. Geprüft hat der Council (KI-Review) gegen Spec 3.2.1 und 4: beide A-Streams
40 Ereignisse, 40 verschiedene ids, pro Absender in Reihenfolge; B-Stream 0. Korrigiert:
Der erste Lauf startete beide Instanzen auf Port 8080 gegen `localhost:5672`, weil
`SpringApplicationBuilder.properties(...)` nur Vorgabewerte setzt, die `application.yml`
überschreibt. Die Werte gehen jetzt als Kommandozeilen-Argumente hinein. Jeder Test startet
eigene Instanzen (`@BeforeEach`/`@AfterEach`), damit Aufgabe 8 eine schliessen kann, ohne
andere Tests zu stören.

---

## Aufgabe 8: Neuverbindung und Aufräumen über zwei Instanzen — S7 und S4 als Test

**Ziel:** Tests zeigen, dass die Neuverbindung mit `Last-Event-ID` an **beiden** Instanzen genau
die verpassten Nachrichten liefert und danach live weitergeht, und dass die Queue einer
geschlossenen Instanz verschwindet.

**Warum an dieser Stelle:** Der Pflicht-Test «Neuverbindung» (Teil D) braucht die zwei Instanzen
aus Aufgabe 7. Das Aufräumen (S4) lässt sich mit demselben Aufbau prüfen: zweite Instanz
schliessen.

**Dateien:**
- Ändern: `TwoInstancesIntegrationTest.java`

**Was der Test prüft:**
- S7: Stream an Instanz 1 empfängt 1 Nachricht, wird geschlossen. 50 Nachrichten. Neuer Stream
  mit `Last-Event-ID` an Instanz 1 **und** an Instanz 2: je genau die 50, in Reihenfolge, keine
  doppelt. Danach 1 neue Nachricht: kommt in beiden live an.
- S4: Instanz 2 wird geschlossen. Ihre Zustell-Queue ist danach innerhalb von 15 s weg
  (Schleife mit Zeitgrenze), die Queue von Instanz 1, `chat.persist` und `chat.dlq` bestehen.

**Gegenprobe:** Wie in Aufgabe 7. Für S7: `subscribe` vorübergehend ohne Nachholen — der Test
muss rot werden. Für S4: `autoDelete` vorübergehend weg — der Test muss rot werden.

**Auftrag an die KI (Wortlaut):**

```text
Lies docs/spec-live-zustellung.md, Abschnitte 2.3, 3.2.2 und 3.2.5, und Spec Kap. 5 S4 und S7.
Ergänze TwoInstancesIntegrationTest um zwei Tests:
1. Neuverbindung: Stream an Instanz 1, eine Nachricht empfangen, Stream schliessen. 50
   Nachrichten senden. Dann mit Header Last-Event-ID neu verbinden, an Instanz 1 und an
   Instanz 2. Beide: genau die 50, in Reihenfolge, keine doppelt, die erste Nachricht nicht.
   Dann eine neue Nachricht: kommt in beiden Streams live an.
2. Aufräumen: Instanz 2 schliessen. In einer Schleife mit Zeitgrenze 15 s prüfen, dass ihre
   Zustell-Queue weg ist. Queue von Instanz 1, chat.persist und chat.dlq bestehen weiter.
Kein Thread.sleep als Warten, sondern Schleife mit Zeitgrenze. Das Schliessen von Instanz 2
darf die anderen Tests nicht stören: Reihenfolge der Tests festlegen oder eigene Instanzen
pro Test, und im Kommentar begründen.
Es gibt keinen neuen Produktivcode. Mach danach die Gegenproben: subscribe vorübergehend ohne
Nachholen -> Test 1 rot; autoDelete vorübergehend weg -> Test 2 rot. Zeig mir beides, dann
zurückstellen.
Regeln aus CLAUDE.md wie in Aufgabe 1.
Nicht committen, bis ich es geprüft habe. Danach Commit-Message:
"test: Neuverbindung und Aufräumen über zwei Instanzen" mit Co-Authored-By-Zeile.
```

- [ ] **Schritt 1: Tests schreiben lassen**
- [ ] **Schritt 2: Tests grün sehen** — `-Dtest=TwoInstancesIntegrationTest`
- [ ] **Schritt 3: Gegenproben rot sehen** — danach zurückstellen
- [ ] **Schritt 4: Committen** — `test: Neuverbindung und Aufräumen über zwei Instanzen`

**Geprüft und korrigiert:** Grün: 3 Tests in `TwoInstancesIntegrationTest`, 0 Fehler. Beide
Gegenproben rot und danach zurückgestellt: `subscribe` ohne Nachholen → die verpassten
Nachrichten kommen nie an; ohne `autoDelete` → die Queue von Instanz 2 besteht nach 5 s noch
(0 Consumer). Geprüft hat der Council (KI-Review) gegen Spec 2.3, 3.2.2 und 3.2.5: An beiden
Instanzen genau die 50 verpassten ids in Reihenfolge, danach die neue live, die schon
gesehene nicht. Korrigiert am Auftrag: Der Plan nannte für S4 eine Zeitgrenze von 15 s. Der
Test nimmt 5 s, kürzer als `x-expires` (10 s). Mit 15 s wäre die Gegenprobe ohne
`autoDelete` grün geblieben, weil dann `x-expires` aufgeräumt hätte, und der Test hätte
nicht gezeigt, welcher Mechanismus wirkt.

---

## Aufgabe 9: Lebenszeichen alle 15 s

**Ziel:** Alle offenen Streams bekommen alle 15 s die Kommentarzeile `:ping`.

**Warum an dieser Stelle:** Für die Szenarien nicht nötig, aber ohne Lebenszeichen sammeln sich
tote Streams in ruhigen Räumen an (Spec 2.2). Es kommt nach dem Hauptweg, weil es ihn nicht
verändert, sondern nur ergänzt.

**Dateien:**
- Anlegen: `service/HeartbeatSender.java`, `config/SchedulingConfig.java`
- Ändern: `service/RoomStream.java` (`sendHeartbeat`), `service/RoomStreamRegistry.java`
- Test: `service/RoomStreamTest.java`, `service/HeartbeatSenderTest.java`

**Was der Test prüft:**
- `sendHeartbeat` schickt jedem Stream des Raums einen Kommentar `ping`.
- Scheitert es bei einem Stream, ist er ausgetragen.
- `HeartbeatSender` erreicht die Streams aller Räume. Die Methode wird im Test direkt
  aufgerufen — kein Warten auf 15 s.

**Auftrag an die KI (Wortlaut):**

```text
Lies docs/spec-live-zustellung.md, Abschnitte 2.2 und 3.2.5 (Absatz "Wer sonst noch an die
Liste der Streams geht").
- RoomStream: synchronized-Methode sendHeartbeat(), sendet an eine KOPIE der Liste einen
  Kommentar "ping" (SseEmitter.event().comment(...)); scheitert es, Stream austragen.
- RoomStreamRegistry: Methode, die alle RoomStream-Objekte als Kopie zurückgibt.
- service/HeartbeatSender: @Scheduled alle 15 s (Konstante mit Kommentar), ruft für jeden
  Raum sendHeartbeat auf.
- config/SchedulingConfig mit @EnableScheduling.
Zuerst die Tests (RoomStreamTest, HeartbeatSenderTest), die Methode des HeartbeatSender im
Test direkt aufrufen. Rot zeigen, dann den Code.
Regeln aus CLAUDE.md wie in Aufgabe 1.
Nicht committen, bis ich es geprüft habe. Danach Commit-Message:
"feat: Lebenszeichen alle 15 s an alle offenen Streams" mit Co-Authored-By-Zeile.
```

- [ ] **Schritt 1: Tests schreiben lassen**
- [ ] **Schritt 2: Tests rot sehen** — `sendHeartbeat` fehlt.
- [ ] **Schritt 3: Code schreiben lassen**
- [ ] **Schritt 4: Tests grün sehen** — `-Dtest=RoomStreamTest,HeartbeatSenderTest`
- [ ] **Schritt 5: Committen** — `feat: Lebenszeichen alle 15 s an alle offenen Streams`

**Geprüft und korrigiert:** Rot gesehen (`sendHeartbeat` und `HeartbeatSender` fehlten),
danach grün: `RoomStreamTest` 9, `HeartbeatSenderTest` 1, `RoomStreamRegistryTest` 2, 0
Fehler. Geprüft hat der Council (KI-Review) gegen Spec 2.2 und 3.2.5: Auf der Leitung steht
genau `:ping` und eine Leerzeile; das Lebenszeichen landet nicht im Puffer; ein Stream, an
dem es scheitert, wird ausgetragen; `sendHeartbeat` ist `synchronized` und sendet über eine
Kopie der Liste. Korrigiert: `trySend` nimmt jetzt ein fertiges Ereignis statt einer
Nachricht, damit Nachricht und Lebenszeichen denselben Fehlerweg nehmen; das Ereignis einer
Nachricht baut die neue Methode `messageEvent`. Der `HeartbeatSender` holt die Räume als
Kopie und hält das Schloss der Registry nicht, während er sendet.

---

## Aufgabe 10: Ungültige Nachrichten in der Zustell-Queue

**Ziel:** Eine Nachricht ohne `id` oder `roomId` oder mit kaputtem JSON wird mit einer Warnung
verworfen. Der Listener läuft weiter, keine Endlosschleife.

**Warum an dieser Stelle:** Fehlerfall aus Spec 3.2.6. Er kommt nach dem Normalweg, weil man
erst prüfen kann, dass der Listener «weiterläuft», wenn er überhaupt läuft.

**Dateien:**
- Ändern: `listener/DeliveryListener.java`
- Test: `listener/DeliveryListenerIntegrationTest.java`

**Was der Test prüft:**
- Kaputtes JSON und JSON ohne `roomId` direkt an `chat.delivery`: keine Zustellung, die
  Zustell-Queue ist danach leer (nicht zurückgelegt).
- Eine gültige Nachricht danach kommt normal im Stream an.
- Im Log steht für die Nachricht ohne `roomId` eine Warnung des `DeliveryListener` und keine
  `ListenerExecutionFailedException` (`OutputCaptureExtension` von Spring Boot).

**Auftrag an die KI (Wortlaut):**

```text
Lies docs/spec-live-zustellung.md, Abschnitt 3.2.6 (Absatz "Eine ungültige Nachricht in der
Zustell-Queue").
Im DeliveryListener: Fehlt id oder roomId, eine Warnung loggen und normal zurückkehren
(Nachricht ist damit bestätigt und verworfen). Kaputtes JSON behandelt Spring selbst
(MessageConversionException, ohne Zurücklegen) — prüfen, nicht nachbauen.
Zuerst die Tests in DeliveryListenerIntegrationTest: kaputtes JSON und JSON ohne roomId direkt
an den Exchange chat.delivery senden; danach eine gültige Nachricht; die gültige muss im
Stream ankommen und die Zustell-Queue leer sein; für die Nachricht ohne roomId muss eine
Warnung im Log stehen und keine ListenerExecutionFailedException (OutputCaptureExtension).
Rot zeigen, dann den Code.
Regeln aus CLAUDE.md wie in Aufgabe 1.
Nicht committen, bis ich es geprüft habe. Danach Commit-Message:
"feat: ungültige Nachrichten in der Zustell-Queue verwerfen" mit Co-Authored-By-Zeile.
```

- [ ] **Schritt 1: Tests schreiben lassen**
- [ ] **Schritt 2: Tests rot sehen** — statt der Warnung steht eine
  `ListenerExecutionFailedException` mit `NullPointerException` im Log. Hinweis: Dass die
  Queue leer ist, stimmt schon vorher, weil `default-requeue-rejected` auf `false` steht
  (Aufgabe 6). Rot wird der Test über das Log.
- [ ] **Schritt 3: Code schreiben lassen**
- [ ] **Schritt 4: Tests grün sehen** — `-Dtest=DeliveryListenerIntegrationTest`
- [ ] **Schritt 5: Committen** — `feat: ungültige Nachrichten in der Zustell-Queue verwerfen`

**Geprüft und korrigiert:** Rot gesehen («Warnung fehlt im Log»; im Log stand eine
`NullPointerException` für die Nachricht ohne `id`), danach grün: 4 Tests, 0 Fehler. Geprüft
hat der Council (KI-Review) gegen Spec 3.2.6: Kaputtes JSON lehnt Spring beim Umwandeln ab
(`MessageConversionException`), JSON ohne `roomId` und ohne `id` ergibt je eine Warnung
«Ignoring message without id or roomId»; keine wird zugestellt, die Zustell-Queue ist danach
leer, und eine gültige Nachricht kommt normal an. Korrigiert am Plan: Der Test sendet
zusätzlich eine Nachricht **ohne `id`** in einen Raum mit offenem Stream. Nur dieser Fall
erzeugte wirklich eine `NullPointerException`; ohne `roomId` landete die Nachricht
unbemerkt im Raum `null` der `HashMap`. Beides fängt jetzt die Prüfung im Listener ab.

---

## Aufgabe 11: Kommentare und README nachführen

**Ziel:** Kommentare nennen den `chat-service` als Verbraucher von `chat.delivery` statt des
`web-gateway`. Die README-Tabelle «Stand» zeigt die Live-Zustellung.

**Warum an dieser Stelle:** Erst wenn alles gebaut ist, stimmt die Beschreibung. Spec 1.4
verlangt, die veralteten Kommentare anzupassen.

**Dateien:**
- Ändern: `config/QueueNames.java`, `config/RabbitConfig.java`, `controller/MessageController.java`
  (Kommentar zum web-gateway, falls nötig), `README.md`

**Was der Test prüft:** Kein neuer Test. Prüfung: `mvn -q clean test` bleibt grün, und
`grep -rn "web-gateway" chat-service/src` zeigt nur noch Stellen, die vom späteren Gateway
sprechen.

**Auftrag an die KI (Wortlaut):**

```text
Lies docs/spec-live-zustellung.md, Abschnitt 1.4.
Passe die Kommentare in config/QueueNames und config/RabbitConfig an: Verbraucher von
chat.delivery sind jetzt die Instanzen des chat-service, Zustellung per SSE. Prüfe mit
grep -rn "web-gateway" chat-service/src, ob weitere Kommentare veraltet sind, und zeig sie mir
vorher.
README.md: Zeile chat-service in der Tabelle "Stand" ergänzen um "stellt Nachrichten per
GET /stream (SSE) an die Clients eines Raums zu". Nur Kommentare und Doku, kein Code.
Danach mvn -q clean test laufen lassen.
Nicht committen, bis ich es geprüft habe. Danach Commit-Message:
"docs: Kommentare und README für die Live-Zustellung nachführen" mit Co-Authored-By-Zeile.
```

- [ ] **Schritt 1: Änderungen machen lassen**
- [ ] **Schritt 2: Tests grün sehen** — `mvn -q clean test`
- [ ] **Schritt 3: Committen** — `docs: Kommentare und README für die Live-Zustellung nachführen`

**Geprüft und korrigiert:** `mvn -q clean test` im Wurzelverzeichnis: EXIT 0, 55 Tests
(chat-service 41, batch-writer 14), 0 Fehler. `grep` fand fünf Stellen mit «web-gateway».
Angepasst sind die zwei veralteten in `QueueNames` und `RabbitConfig`. Stehen gelassen,
weil sie vom späteren Gateway sprechen und stimmen: `MessageController` (das Gateway ruft
`POST /messages` später auf), `ChatMessage` (das Gateway bekommt später eine eigene Kopie
des Vertrags) und der Kommentar im bestehenden `MessagePublisherIntegrationTest`. README:
Zeile `chat-service` um `GET /stream` ergänzt. Geprüft hat der Council (KI-Review).

---

## Aufgabe 12: Durchsicht nach CLAUDE.md (S8)

**Ziel:** Der ganze `chat-service` hält die Regeln aus CLAUDE.md ein, gemessen mit den Befehlen
aus Spec Kap. 5 S8.

**Warum an dieser Stelle:** Am Schluss, weil erst jetzt aller Code da ist. In Bewertung 1 hat
genau diese Durchsicht noch einen Fund gebracht.

**Dateien:** je nach Fund

**Was der Test prüft:** Die drei Befehle aus Spec Kap. 5 S8 geben das erwartete Ergebnis.
`mvn -q clean test` bleibt grün.

**Auftrag an die KI (Wortlaut):**

```text
Führe die Befehle aus docs/spec-live-zustellung.md Kap. 5, S8, aus und zeig mir die Ausgabe.
Lies danach alle neuen und geänderten Klassen gegen CLAUDE.md durch (ein Ergebnis pro Zeile,
sprechende Namen, kurze Methoden, Kommentar erklärt das Warum, Deutsch/Englisch). Liste jeden
Fund mit Datei und Zeile auf, bevor du etwas änderst. Gibt es keinen Fund, sag das und ändere
nichts.
Nach meinem OK: Funde beheben, mvn -q clean test laufen lassen.
Nicht committen, bis ich es geprüft habe. Commit-Message nur bei Funden:
"refactor: Durchsicht der Live-Zustellung nach CLAUDE.md" mit Co-Authored-By-Zeile.
```

- [ ] **Schritt 1: Befehle und Durchsicht laufen lassen**
- [ ] **Schritt 2: Funde beheben lassen** — nur nach meinem OK
- [ ] **Schritt 3: Tests grün sehen** — `mvn -q clean test`
- [ ] **Schritt 4: Committen** — nur bei Funden: `refactor: Durchsicht der Live-Zustellung nach CLAUDE.md`

**Geprüft und korrigiert:** Durchsicht durch den Council (KI-Review) mit den drei Befehlen
aus Spec Kap. 5 S8. Funde:
1. S8.1 meldete jede Zeile mit `roomStream.` als Treffer, weil der Befehl `Stream\.` suchte.
   Es gibt keine Java-Streams. Der Befehl in der Spec sucht jetzt `\bStream\.`, in einem
   eigenen Commit (`docs: S8-Prüfbefehl sucht Stream nur als ganzes Wort`). Danach blieben zwei
   deutsche Kommentarsätze, die auf «…Stream.» endeten; sie sind umgestellt.
2. S8.2 fand 19 Methoden ohne Kommentar. 17 davon stammen aus Bewertung 1 (`main`,
   `handleBrokerNotAvailable` und Testmethoden der alten Tests), weil S8 dort nur den
   `batch-writer` geprüft hatte. Jetzt hat jede einen Kommentar, der das Warum nennt. Die
   zwei anderen waren `@SpringBootTest(...)` über mehrere Zeilen in zwei neuen Tests; das
   verwirrte den Prüfbefehl. Die Annotation steht jetzt auf einer Zeile.
3. S8.3 ohne Fund: `.env` nicht versioniert, in `.gitignore`, im Code nur Platzhalter.
Ausserdem ein veralteter Kommentar im `MessagePublisherIntegrationTest` («Wir spielen ein
web-gateway»). Danach alle drei Befehle ohne Ausgabe, `mvn -q clean test` EXIT 0, 55 Tests.

---

## Änderungen am Plan

*Noch keine.* Jede Änderung kommt hier als neuer Eintrag mit Datum, Änderungsgrund und dem, was
neu gilt — in einem eigenen Commit. Der Text oben bleibt stehen.

---

## Abschluss-Prüfung

Alle acht Szenarien mit den Befehlen aus Spec Kap. 5, in dieser Reihenfolge, auf einem
**frischen Klon** mit `.env` aus `.env.example`. Gemessener Wert neben dem erwarteten.

- [ ] S1 `mvn -q clean test` — erwartet: Exit-Code 0
- [ ] S2 Stack startet, kein Port veröffentlicht — erwartet: alle Dienste laufen, 0 Ports
- [ ] S3 zwei Instanzen — erwartet: beide A-Clients 400, pro Absender in Reihenfolge, B 0
- [ ] S4 zurück auf eine Instanz — erwartet: nach 30 s eine Zustell-Queue, `chat.persist`
  und `chat.dlq` bestehen
- [ ] S5 ohne Datenbank — erwartet: 100 im Stream nach 5 s, danach 100 neue Zeilen
- [ ] S6 hängende Instanz — erwartet: Queue nie über 1000, gesunder Client 5000
- [ ] S7 Neuverbindung — erwartet: an beiden Instanzen genau die 50, danach live
- [ ] S8 Quelltext — erwartet: keine Ausgabe der Prüfbefehle, `.env` nicht im Repo
- [ ] Tag `bewertung-2` gesetzt und gepusht

---

## Ergebnis der Abschluss-Prüfung

Gemessen am 13.11.2026 auf einem **frischen Klon von GitHub** (Commit `0cb0c1f`) mit `.env`
aus `.env.example`, Szenarien in dieser Reihenfolge auf demselben Stack, mit den Befehlen aus
Spec Kap. 5. Ausgeführt vom Council (KI) über ein lokales Hilfsskript auf meinem Rechner
(Docker Desktop, Git Bash).

| Szenario | Erwartet | Gemessen | |
|---|---|---|---|
| S1 `mvn -q clean test` | ein Lauf, alles grün | EXIT 0, 55 Tests (chat-service 41, batch-writer 14), 0 Fehler | bestanden |
| S2 frischer Klon, `up -d --build` | alle Dienste laufen, kein Port | 4 Dienste laufen, `postgres` und `rabbitmq` healthy; 0 Ports mit `->`; kein `ports:` | bestanden |
| S3 zwei Instanzen | beide A-Clients 400, je einmal, pro Absender in Reihenfolge; B 0 | 2 Zustell-Queues je 1 Consumer, 2 Bindungen; A an I1: 400 / 400 ids, anna und ben je 200 in Reihenfolge; A an I2: dasselbe; B: 0 | bestanden |
| S4 zurück auf eine Instanz | nach ≤ 30 s eine Queue; `chat.persist`, `chat.dlq` bleiben | Scale dauerte 11 s, nach 13 s eine Queue (die der übrigen Instanz), 1 Bindung; `chat.persist` und `chat.dlq` vorhanden | bestanden |
| S5 ohne Postgres und batch-writer | 100 nach ≤ 5 s im Stream; danach 100 in der Tabelle | 100 im Stream 5 s nach dem letzten POST; 100 neue Zeilen nach dem Neustart | bestanden |
| S6 eine Instanz pausiert, 5000 Nachrichten | Queue nie über 1000; gesunder Client 5000; danach wieder Zustellung | höchste Tiefe 950 (900 + Prefetch 50, wie Spec 3.2.4); gesunder Client 5000 in Reihenfolge; nach dem Fortsetzen 2 Queues je 1 Consumer, pausierte Instanz stellt wieder zu | bestanden |
| S7 Neuverbindung mit `Last-Event-ID` | genau die 50 fehlenden, in Reihenfolge, keine doppelt, danach live | an derselben und an der anderen Instanz je 51 Ereignisse, 51 ids: die 50 verpassten in Reihenfolge, dann die neue live; die schon gesehene nicht | bestanden |
| S8 Quelltext | keine Streams, Kommentar über jeder Klasse und Methode; `.env` nicht im Repo | alle drei Befehle aus Spec Kap. 5 S8 ohne Ausgabe; `.env` nicht versioniert, in `.gitignore` | bestanden |
