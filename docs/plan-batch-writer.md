# batch-writer — Umsetzungsplan

> **Stand 02.10.2026: umgesetzt und abgenommen.** Alle zwölf Aufgaben sind gebaut und
> committet (`git log --oneline`, Commits `e566e46` bis `980220c`); jede Betreffzeile
> steht genau so unten bei ihrer Aufgabe. Die Abschluss-Prüfung lief auf einem frischen
> Klon mit `.env` aus `.env.example`, alle acht Szenarien in Reihenfolge auf demselben
> Stack — die gemessenen Werte stehen ganz unten. Zwei Funde aus dieser Prüfung sind in
> eigenen Commits behoben, siehe «Abweichungen vom Plan».

**Ziel:** Der `batch-writer` holt Nachrichten aus der Queue `chat.persist`, schreibt sie
stapelweise in die Tabelle `message` und bestätigt erst nach dem COMMIT. Er übersteht
Rückstau, Duplikate, zwei Instanzen und einen Datenbankausfall (Szenarien S3 bis S7).

**Architektur:** Vier Klassen mit je einer Aufgabe. `ChatMessage` ist der Vertrag als
`record`. `MessageRepository` schreibt einen Stapel in einer Transaktion.
`PersistQueueListener` empfängt Stapel, entdoppelt sie, schreibt sie und wartet bei einem
Datenbankausfall. `RabbitConfig` legt die Queues an und stellt die Stapelbildung ein. Kein
Webserver, kein Port.

**Tech-Stack:** Java 21, Spring Boot 3.5.16, Spring AMQP (Batch-Listener), Spring JDBC
(`JdbcTemplate`), PostgreSQL 17, RabbitMQ 3.13, JUnit 5, Testcontainers.

**Spec:** [`spec-batch-writer.md`](spec-batch-writer.md). Vorbild für den Aufbau dieses
Plans: [`plan-chat-service.md`](plan-chat-service.md).

## Globale Vorgaben

Diese Punkte gelten für **jede** Aufgabe:

- **Code auf Englisch** — Klassen, Methoden, Variablen, Log-Meldungen. **Alles andere auf
  Deutsch** — Kommentare, Javadoc, Commit-Messages.
- **Ein Ergebnis pro Zeile**, keine verschachtelten Aufrufe. **Keine Streams**, sondern
  `for`-Schleifen. Gilt auch in Tests.
- **Über jeder Klasse und jeder Methode ein Kommentar**, der das *Warum* erklärt — auch
  über jeder Testmethode. S8 prüft das mit dem Befehl aus Spec Kap. 5.
- **Lombok** für `@Slf4j` und `@RequiredArgsConstructor`, **`record`** für Datenklassen.
- **Kein `ports:`** und **kein `container_name:`** für den `batch-writer`.
- **Keine Geheimnisse im Repository.** Neue Variablen kommen mit Beispielwerten in
  `.env.example`.
- **Jede Aufgabe endet mit genau einem Commit.** Die Betreffzeile steht unten bei der
  Aufgabe; darunter erklärt der Commit-Text, warum. Jeder Commit endet mit
  `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- **Voraussetzung:** Docker läuft. Testcontainers startet für die Tests echte RabbitMQ-
  und Postgres-Container.

Einzelne Tests laufen mit `mvn -q -pl batch-writer test -Dtest=<Testklasse>` aus dem
Wurzelverzeichnis.

---

## Abgrenzung

| Bewusst **nicht** in diesem Plan | Warum |
|---|---|
| Chat-Historie lesen | Der Lesepfad gehört dem `chat-service` und ist laut Auftrag nicht Teil dieser Aufgabe |
| Räume, Mitgliedschaften, Tabelle `room` | Nicht Teil dieser Aufgabe. Deshalb auch kein Fremdschlüssel auf `room` (Spec 4.1) |
| Keycloak, `web-gateway`, `load-generator` | Nicht Teil dieser Aufgabe |
| DLQ auswerten oder wieder einspielen | Die DLQ wird hier nur befüllt. Wer sie leert, ist eine spätere Entscheidung |
| Prüfung auf `\u0000` im Text | Gehört vor die Queue in den `chat-service` (Spec 3.2.3) |

---

## Dateistruktur

```
pom.xml                                   # + <module>batch-writer</module>
docker-compose.yml                        # + postgres, + batch-writer, Volume postgres-data
.env.example                              # + POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB
chat-service/Dockerfile                   # baut nur noch das eigene Modul
postgres/init/01-schema.sql               # Tabelle message und Index
batch-writer/
├── Dockerfile
├── pom.xml
└── src/
    ├── main/java/ch/benedict/m321/batchwriter/
    │   ├── BatchWriterApplication.java
    │   ├── config/QueueNames.java
    │   ├── config/RabbitConfig.java
    │   ├── dto/ChatMessage.java
    │   ├── listener/PersistQueueListener.java
    │   └── repository/MessageRepository.java
    ├── main/resources/application.yml
    └── test/java/ch/benedict/m321/batchwriter/
        ├── TestcontainersConfiguration.java      # RabbitMQ und Postgres als Spring-Beans
        ├── IntegrationTestBase.java              # gemeinsamer Kontext und Hilfsmethoden
        ├── BatchWriterApplicationTest.java
        ├── SchemaIntegrationTest.java
        ├── dto/ChatMessageConversionTest.java
        ├── repository/MessageRepositoryIntegrationTest.java
        └── listener/
            ├── PersistQueueIntegrationTest.java      # S3
            ├── BacklogIntegrationTest.java           # S4
            ├── DuplicateMessageIntegrationTest.java  # S5
            ├── DatabaseOutageIntegrationTest.java    # S7
            └── BrokenMessageIntegrationTest.java     # Spec 3.2.3
```

---

## Aufgabe 1: chat-service-Image unabhängig von der Modulliste bauen

**Warum an dieser Stelle:** Aufgabe 2 trägt `batch-writer` ins Eltern-POM ein. Das heutige
`chat-service/Dockerfile` kopiert nur sein eigenes Modul, und Maven bricht ab, sobald ein
Modul der Liste fehlt («Child module … does not exist», lokal nachgestellt). Die Falle wird
entschärft, bevor sie zuschnappt.

**Dateien:**
- Ändern: `chat-service/Dockerfile`

**Was der Test prüft:** Das Image baut weiterhin. Der eigentliche Beweis folgt in
Aufgabe 2: Dort baut es auch dann noch, wenn das Eltern-POM ein Modul nennt, das nicht im
Build liegt.

- [x] **Schritt 1: Build-Befehl umstellen** — statt `mvn -pl chat-service -am package`
  baut der Build mit `mvn -q -f chat-service/pom.xml package -DskipTests` nur das eigene
  Modul. Das Eltern-POM wird dabei nur für die gemeinsamen Einstellungen gelesen, seine
  Modulliste spielt keine Rolle. Ein Kommentar im Dockerfile erklärt das.
- [x] **Schritt 2: Prüfen** — `docker compose build chat-service` läuft durch.
- [x] **Schritt 3: Committen** — `chore: chat-service-Image unabhängig von der Modulliste bauen`

---

## Aufgabe 2: Maven-Modul batch-writer und Anwendungsstart

**Warum an dieser Stelle:** Ohne Modul im Eltern-POM läuft kein einziger Test des neuen
Dienstes. Alles Weitere braucht dieses Gerüst und die Testkonfiguration mit echtem RabbitMQ
und echtem Postgres, die jede spätere Aufgabe benutzt.

**Dateien:**
- Ändern: `pom.xml` (`<module>batch-writer</module>`)
- Anlegen: `batch-writer/pom.xml`, `BatchWriterApplication.java`,
  `src/main/resources/application.yml`
- Test: `TestcontainersConfiguration.java`, `IntegrationTestBase.java`,
  `BatchWriterApplicationTest.java`

**Was der Test prüft:** `BatchWriterApplicationTest.contextLoads` — der Spring-Kontext
fährt mit echtem RabbitMQ und echtem Postgres hoch. Die beiden Container sind Spring-Beans
in `TestcontainersConfiguration`; alle Integrationstests erben über `IntegrationTestBase`
denselben Kontext und starten die Container deshalb nur einmal.

- [x] **Schritt 1: Test und Testkonfiguration schreiben**
- [x] **Schritt 2: Test rot sehen** — `mvn -q -pl batch-writer test` scheitert, weil es das
  Modul noch nicht gibt.
- [x] **Schritt 3: Modul anlegen** — Abhängigkeiten nach Spec 4.5, Hauptklasse,
  `application.yml` mit den Variablen aus Spec 4.3.
- [x] **Schritt 4: Test grün sehen** — `mvn -q -pl batch-writer test`. Zusätzlich
  `docker compose build chat-service`: baut weiterhin (Nachweis für Aufgabe 1).
- [x] **Schritt 5: Committen** — `chore: Maven-Modul batch-writer anlegen`

---

## Aufgabe 3: Datenklasse ChatMessage — der Vertrag

**Warum an dieser Stelle:** Was auf der Queue ankommt (Spec Kap. 2), bestimmt alles
Weitere: die Tabelle, das INSERT, den Listener. Der Vertrag wird deshalb zuerst
festgenagelt, und zwar so, wie die Nachricht wirklich auf der Leitung liegt.

**Dateien:**
- Anlegen: `dto/ChatMessage.java` — eigene Kopie, prüft im Konstruktor, dass kein Feld
  `null` ist (Spec 3.2.3)
- Test: `dto/ChatMessageConversionTest.java`

**Was der Test prüft** (mit dem `ObjectMapper` von Spring Boot und demselben
`Jackson2JsonMessageConverter`, den der Dienst benutzt):
- Die gemessene Nachricht aus Spec 2.2, **ohne** `__TypeId__`, mit neun Nachkommastellen
  in `sentAt`, ergibt eine `ChatMessage` mit allen sechs Feldern.
- JSON ohne `content` führt zu einer `MessageConversionException`.
- Kaputtes JSON führt zu einer `MessageConversionException`.

- [x] **Schritt 1: Test schreiben**
- [x] **Schritt 2: Test rot sehen** — kompiliert nicht, weil `ChatMessage` fehlt.
- [x] **Schritt 3: `ChatMessage` anlegen**
- [x] **Schritt 4: Test grün sehen** — `-Dtest=ChatMessageConversionTest`
- [x] **Schritt 5: Committen** — `feat: Datenklasse ChatMessage als eigene Kopie des Vertrags`

---

## Aufgabe 4: Tabelle message als Init-Skript

**Warum an dieser Stelle:** Bevor Code schreiben kann, muss es die Tabelle geben. Das
Skript entsteht vor dem Code, der es benutzt, und der Test lädt es auf demselben Weg wie
später `docker compose`.

**Dateien:**
- Anlegen: `postgres/init/01-schema.sql` (Spec 4.1)
- Ändern: `TestcontainersConfiguration.java` — kopiert das Skript nach
  `/docker-entrypoint-initdb.d/`
- Test: `SchemaIntegrationTest.java`

**Was der Test prüft:** Die Tabelle `message` hat genau die sechs Spalten aus
PLANUNG.md §3.7 mit den richtigen Typen, alle `NOT NULL`. Es gibt den Primärschlüssel auf
`id` und den Index `idx_message_room_sent` auf `(room_id, sent_at DESC)`.

- [x] **Schritt 1: Test schreiben**
- [x] **Schritt 2: Test rot sehen** — die Tabelle fehlt.
- [x] **Schritt 3: Skript anlegen und im Test-Postgres einhängen**
- [x] **Schritt 4: Test grün sehen** — `-Dtest=SchemaIntegrationTest`
- [x] **Schritt 5: Committen** — `feat: Tabelle message als Init-Skript für Postgres`

---

## Aufgabe 5: MessageRepository — ein Stapel, eine Transaktion

**Warum an dieser Stelle:** Das Schreiben in die Datenbank ist der Kern des Dienstes und
lässt sich ohne Queue prüfen. Erst wenn es allein stimmt, lohnt es sich, die Queue
davorzuschalten.

**Dateien:**
- Anlegen: `repository/MessageRepository.java` — `insertAll(List<ChatMessage>)` mit
  `@Transactional`, `JdbcTemplate.batchUpdate` und `ON CONFLICT (id) DO NOTHING`; gibt die
  Zahl der neuen Zeilen zurück
- Test: `repository/MessageRepositoryIntegrationTest.java`

**Was der Test prüft:**
- 500 Nachrichten ergeben 500 Zeilen, die Felder einer Zeile stimmen.
- Zwei gleiche `id` im **selben** Stapel ergeben eine Zeile und keinen Fehler — das ist
  die Messung, die Spec 3.2.2 ankündigt.
- Eine `id`, die schon in der Tabelle steht, wird in einem späteren Stapel übergangen;
  die Rückgabe zählt nur die neuen Zeilen.

- [x] **Schritt 1: Test schreiben**
- [x] **Schritt 2: Test rot sehen** — kompiliert nicht, weil `MessageRepository` fehlt.
- [x] **Schritt 3: `MessageRepository` anlegen**
- [x] **Schritt 4: Test grün sehen** — `-Dtest=MessageRepositoryIntegrationTest`
- [x] **Schritt 5: Committen** — `feat: Stapel in einer Transaktion mit ON CONFLICT schreiben`

---

## Aufgabe 6: Listener — Stapel aus chat.persist in die Datenbank

**Warum an dieser Stelle:** Vertrag und Schreiben sind jetzt einzeln geprüft. Erst damit
ist es sinnvoll, die Queue anzuschliessen. Ab hier funktioniert der Hauptweg (S3).

**Dateien:**
- Anlegen: `config/QueueNames.java`, `config/RabbitConfig.java` (beide Queues mit exakt
  den Eigenschaften des `chat-service`, JSON-Konverter, Container-Factory mit Stapel
  500 / 200 ms und Prefetch 500), `listener/PersistQueueListener.java`
- Test: `listener/PersistQueueIntegrationTest.java`

**Was der Test prüft:** 1000 Nachrichten in `chat.persist` stehen nach spätestens 60 s als
1000 Zeilen in der Tabelle, und die Queue ist leer.

- [x] **Schritt 1: Test schreiben**
- [x] **Schritt 2: Test rot sehen** — kompiliert nicht, weil `QueueNames` fehlt.
- [x] **Schritt 3: Konfiguration und Listener anlegen**
- [x] **Schritt 4: Test grün sehen** — `-Dtest=PersistQueueIntegrationTest`
- [x] **Schritt 5: Committen** — `feat: Nachrichten stapelweise aus chat.persist in die Datenbank schreiben`

---

## Aufgabe 7: Rückstau in wenigen Transaktionen (S4)

**Warum an dieser Stelle:** Der Test prüft die Stapel-Einstellungen aus Aufgabe 6 unter
der Bedingung, auf die es ankommt. Er kommt direkt danach, damit ein falscher Wert auffällt,
bevor weiter darauf gebaut wird.

**Dateien:**
- Test: `listener/BacklogIntegrationTest.java` — nur ein Test, der Code steht seit
  Aufgabe 6

**Was der Test prüft:** Listener anhalten, 1000 Nachrichten in die Queue legen,
`xact_commit` ablesen, Listener starten, warten, bis alle 1000 Zeilen da sind,
`xact_commit` erneut ablesen. Die Differenz ist höchstens 100.

- [x] **Schritt 1: Test schreiben**
- [x] **Schritt 2: Test grün sehen** — `-Dtest=BacklogIntegrationTest`. Ein rotes Vorher
  gibt es hier nicht, weil der Test bestehendes Verhalten nachweist. Zur Gegenprobe einmal
  mit `batchSize = 1` laufen lassen: dann muss er rot werden.
- [x] **Schritt 3: Committen** — `test: Rückstau wird in wenigen Transaktionen geschrieben`

---

## Aufgabe 8: Duplikate im Stapel entfernen (S5)

**Warum an dieser Stelle:** Die Aufgabe braucht den ganzen Weg von der Queue in die
Datenbank aus Aufgabe 6. Sie ergänzt vor der zweiten Verteidigungslinie (`ON CONFLICT`,
Aufgabe 5) die erste: die Entdopplung in Java.

**Dateien:**
- Ändern: `listener/PersistQueueListener.java` — `removeDuplicates`
- Test: `listener/DuplicateMessageIntegrationTest.java`

**Was der Test prüft:** Dieselbe Nachricht wird zweimal als rohes JSON direkt in
`chat.persist` gelegt, **nur** mit `content_type: application/json` (ohne `__TypeId__`).
Danach steht genau eine Zeile in der Tabelle, und `chat.dlq` ist leer.

**Ehrlich vermerkt:** Der Test ist schon ohne die neue Schleife grün, weil `ON CONFLICT`
aus Aufgabe 5 das Duplikat abfängt. Er weist S5 nach, treibt aber die Schleife nicht. Die
Schleife ist die Entscheidung aus Spec 3.2.2.

- [x] **Schritt 1: Test schreiben und laufen lassen** — `-Dtest=DuplicateMessageIntegrationTest`
- [x] **Schritt 2: `removeDuplicates` einbauen** — mit Log-Zeile, wie viele Nachrichten ein
  Stapel hatte und wie viele davon verschieden waren.
- [x] **Schritt 3: Test grün sehen**
- [x] **Schritt 4: Committen** — `feat: Duplikate innerhalb eines Stapels vor dem Schreiben entfernen`

---

## Aufgabe 9: Datenbankausfall aussitzen (S7)

**Warum an dieser Stelle:** Diese Aufgabe ändert, wie der Listener mit Fehlern umgeht.
Das ergibt erst Sinn, wenn der Normalfall (Aufgaben 6 bis 8) bewiesen ist. Sonst wüsste
man bei einem roten Test nicht, ob es am Ausfall liegt oder am Normalfall.

**Dateien:**
- Ändern: `config/RabbitConfig.java` (`defaultRequeueRejected = false`),
  `listener/PersistQueueListener.java` (Wiederholung mit 1 s, 2 s, 4 s, 8 s, dann 10 s),
  `application.yml` (`spring.datasource.hikari.connection-timeout: 5000`)
- Test: `listener/DatabaseOutageIntegrationTest.java`

**Was der Test prüft:** Den Postgres-Container **pausieren** (nicht stoppen — ein
gestoppter Testcontainer käme mit neuem Port zurück), 300 Nachrichten senden, 8 s warten:
`chat.dlq` bleibt leer. Den Container fortsetzen: Alle 300 Zeilen kommen an, `chat.dlq`
ist weiterhin leer.

- [x] **Schritt 1: Test schreiben, `defaultRequeueRejected = false` und den
  Verbindungs-Timeout setzen**
- [x] **Schritt 2: Test rot sehen** — ohne Wiederholung landen die Nachrichten in
  `chat.dlq`. Genau das verhindert die Schleife.
- [x] **Schritt 3: Wiederholung im Listener einbauen**
- [x] **Schritt 4: Test grün sehen** — `-Dtest=DatabaseOutageIntegrationTest`
- [x] **Schritt 5: Committen** — `feat: bei Datenbankausfall warten und wiederholen statt ablehnen`

---

## Aufgabe 10: Kaputte Nachricht landet einzeln in der DLQ

**Warum an dieser Stelle:** Der Test prüft am laufenden Broker das Zusammenspiel von
Aufgabe 3 (Prüfung im Konstruktor) und Aufgabe 9 (es gibt nur zwei Ausgänge: geschrieben
oder DLQ).

**Dateien:**
- Test: `listener/BrokenMessageIntegrationTest.java` — nur ein Test

**Was der Test prüft:** Vier Nachrichten kurz hintereinander, also im selben Stapel: eine
gültige, eine ohne `content`, eine mit kaputtem JSON, wieder eine gültige. Danach stehen
die zwei gültigen in der Tabelle und die zwei kaputten in `chat.dlq` (Spec 3.2.3).

- [x] **Schritt 1: Test schreiben**
- [x] **Schritt 2: Test grün sehen** — `-Dtest=BrokenMessageIntegrationTest`
- [x] **Schritt 3: Committen** — `test: kaputte Nachricht landet einzeln in chat.dlq`

---

## Aufgabe 11: Image und docker-compose (S2)

**Warum an dieser Stelle:** Der Dienst kommt erst in den Stack, wenn sein Code mit Tests
bewiesen ist. Und S2 bis S7 lassen sich nur im Stack prüfen.

**Dateien:**
- Anlegen: `batch-writer/Dockerfile`
- Ändern: `docker-compose.yml` (Dienste `postgres` und `batch-writer` im Netz `chat-net`,
  Init-Skript, Healthcheck, Volume `postgres-data`, kein `ports:`, kein `container_name:`),
  `.env.example` (`POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`)

**Was der Test prüft:** S2 aus Spec Kap. 5. `docker compose up -d --build`, alle Dienste
laufen bzw. sind `healthy`, kein Port ist veröffentlicht. Dazu eine Nachricht per
`POST /messages`, die danach in der Tabelle steht.

- [x] **Schritt 1: Dockerfile, Compose-Dienste und `.env.example` anlegen**
- [x] **Schritt 2: Prüfen** — Befehle von S2, dann eine Nachricht senden und mit `psql`
  nachsehen.
- [x] **Schritt 3: Committen** — `chore: batch-writer und Postgres in docker-compose abbilden`

---

## Aufgabe 12: README nachführen

**Warum an dieser Stelle:** Die Doku folgt dem, was wirklich läuft. Erst nach Aufgabe 11
stimmt «vorhanden».

**Dateien:**
- Ändern: `README.md` (Tabelle «Stand», Start- und Testbefehle),
  `docker-compose.yml` (Kopfkommentar «Ausbaustufe»)

**Was der Test prüft:** Durchlesen. Jeder Befehl im README ist in Aufgabe 11 oder in der
Abschluss-Prüfung tatsächlich gelaufen.

- [x] **Schritt 1: README anpassen**
- [x] **Schritt 2: Committen** — `docs: README-Stand für batch-writer und Postgres nachführen`

---

## Abweichungen vom Plan

- **Aufgabe 9, der Test:** Der erste Entwurf pausierte die Datenbank, bevor der Dienst je
  geschrieben hatte. Für den allerersten Verbindungsaufbau hat der Postgres-Treiber keine
  Zeitgrenze; der Versuch hing einfach, bis die Datenbank zurück war. Der Test war deshalb
  auch **ohne** Wiederholung grün und bewies nichts. Korrigiert: Der Test schreibt zuerst
  eine Nachricht — wie in S7, wo der Dienst schon läuft. Danach war er ohne Schleife rot
  (79 Nachrichten in `chat.dlq`) und mit Schleife grün.
- **Nach Aufgabe 12, zwei Commits aus der Abschluss-Prüfung:**
  - `refactor: Ergebnisse in den Tests vor dem Vergleich benennen` — die Durchsicht für
    S8 fand Abfragen direkt in `assertEquals(...)`. CLAUDE.md verlangt auch in Tests ein
    Ergebnis pro Zeile.
  - `docs: send-Befehl der Abnahme nummeriert jede Nachricht` — im Messbefehl aus
    Spec Kap. 5 setzte die Shell die Nummer nie ein. S6 zeigte deshalb `1000|1` statt
    `1000|1000`, obwohl 1000 verschiedene `id` angekommen waren. Der Fehler lag im
    Messbefehl, nicht im Dienst.

---

## Abschluss-Prüfung

Alle acht Szenarien mit den Befehlen aus Spec Kap. 5, in dieser Reihenfolge, auf einem
**frischen Klon** mit `.env` aus `.env.example`. Gemessen am 02.10.2026 auf Commit
`e648844`; danach kam nur noch dieser Plan-Commit dazu.

- [x] S1 `mvn -q clean test` — Exit-Code 0, 29 Tests (chat-service 14, batch-writer 15),
  0 Fehler
- [x] S2 Stack startet, kein Port veröffentlicht — alle vier Dienste laufen, `postgres`
  und `rabbitmq` sind `healthy`, 0 veröffentlichte Ports, kein `ports:`-Schlüssel
- [x] S3 1000 Nachrichten — 1000 neue Zeilen, schon direkt nach dem Senden;
  `chat.persist` leer
- [x] S4 Rückstau — 1000 Nachrichten warteten ohne Verbraucher, danach 1000 neue Zeilen
  in **25** Transaktionen (Grenze 100)
- [x] S5 dieselbe Nachricht zweimal — genau 1 Zeile mit der `id`, `chat.dlq` leer
- [x] S6 zwei Instanzen — `consumers = 2`, 1000 neue Zeilen, Texte `1000|1000`, beide
  Instanzen haben Stapel geschrieben
- [x] S7 Postgres 15 s weg — 300 neue Zeilen, `chat.dlq` leer, `RestartCount` 0 und
  Startzeit beider Instanzen unverändert, 8 Wiederhol-Zeilen im Log
- [x] S8 Quelltext — keine Streams, 0 Deklarationen ohne Kommentar in 17 Java-Dateien,
  `.env` nicht versioniert und über `.gitignore:17` ignoriert

Die Haken und Werte stehen in einem eigenen Commit:
`docs: erledigte Aufgaben im Plan des batch-writer abhaken`.
