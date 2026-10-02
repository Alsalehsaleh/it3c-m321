# Spezifikation: `batch-writer`

**Modul M321 · Klasse IT3c**

Der `batch-writer` ist der einzige Dienst, der in die Datenbank schreibt. Er holt
Nachrichten aus der Queue `chat.persist`, sammelt sie zu Stapeln und schreibt jeden
Stapel mit **einem** INSERT.

Dieses Dokument beschreibt, *was* der Dienst tun muss — nicht, in welcher Reihenfolge
man ihn baut. Grundlage: [`../PLANUNG.md`](../PLANUNG.md) Abschnitt 3.6 (Ablauf),
3.7 (Datenmodell) und 4.1 (Mengengerüst). Die Regeln aus
[`../CLAUDE.md`](../CLAUDE.md) gelten für den Code, der daraus entsteht.

---

## 1. Zweck und Abgrenzung

### 1.1 Warum es den Dienst gibt

Bei 1'667 Nachrichten pro Sekunde (PLANUNG.md §4.1) würde ein Einzel-INSERT pro Nachricht
die Datenbank 1'667 Transaktionen pro Sekunde kosten. Der `batch-writer` verwandelt
diesen Strom in wenige grosse Transaktionen. Das ist seine einzige Daseinsberechtigung —
und der Grund, warum zwischen `chat-service` und Datenbank überhaupt eine Queue liegt.

Der zweite Zweck ist die Entkopplung: Die Nachricht ist beim Empfänger, **bevor** sie in
der Datenbank steht. Das Speichern darf langsam sein, das Zustellen nicht.

### 1.2 Was der Dienst tut

1. Er ist Verbraucher der Queue `chat.persist`.
2. Er sammelt eingehende Nachrichten zu einem Stapel von **500 Stück oder 200 ms**,
   je nachdem, was zuerst eintritt.
3. Er entdoppelt den Stapel nach `id` und schreibt ihn mit einem einzigen
   `INSERT ... ON CONFLICT (id) DO NOTHING` in die Tabelle `message`.
4. Er bestätigt den Stapel gegenüber RabbitMQ **erst nach dem COMMIT**.
5. Ist die Datenbank nicht erreichbar, wartet er und versucht es erneut, statt die
   Nachrichten abzulehnen.

### 1.3 Was der Dienst bewusst NICHT tut

| Nicht seine Aufgabe | Warum nicht |
|---|---|
| Eine REST-Schnittstelle anbieten | Er wird von niemandem aufgerufen. Seine einzige Eingangstür ist die Queue |
| Einen Port veröffentlichen | Vorgabe: nur die Web-App ist über `localhost` erreichbar |
| Chat-Historie lesen | Der Lesepfad gehört dem `chat-service` (PLANUNG.md §3.1) |
| Räume und Mitgliedschaften verwalten | Nicht Teil dieser Aufgabe. Er schreibt nur in `message` |
| Token prüfen | Das Gateway ist der einzige Wachposten (PLANUNG.md §3.1). Von aussen ist der Dienst ohnehin nicht erreichbar |
| Auf `chat.delivery` hören | Das ist der Zustellweg und gehört dem `web-gateway` |
| Selbst etwas veröffentlichen | Er ist Verbraucher, nie Erzeuger |
| **Exactly-once** behaupten | Er garantiert At-least-once. Duplikate werden harmlos gemacht, nicht verhindert (siehe 3.2.2) |

---

## 2. Vertrag: was auf `chat.persist` ankommt

Dieser Abschnitt ist aus dem laufenden Code des `chat-service` hergeleitet, nicht aus
der Planung. Jede Aussage ist belegt.

### 2.1 Woher die Nachricht kommt

| Aussage | Beleg |
|---|---|
| Die Queue heisst `chat.persist` | [`chat-service/.../config/QueueNames.java:13`](../chat-service/src/main/java/ch/benedict/m321/chatservice/config/QueueNames.java#L13) |
| Sie ist `durable`, überlebt also einen Broker-Neustart | [`RabbitConfig.java:28`](../chat-service/src/main/java/ch/benedict/m321/chatservice/config/RabbitConfig.java#L28) |
| An ihr hängt eine Dead-Letter-Route über den Standard-Exchange `""` auf `chat.dlq` | [`RabbitConfig.java:29-30`](../chat-service/src/main/java/ch/benedict/m321/chatservice/config/RabbitConfig.java#L29-L30) |
| Die Dead-Letter-Queue heisst `chat.dlq` und ist ebenfalls `durable` | [`QueueNames.java:19`](../chat-service/src/main/java/ch/benedict/m321/chatservice/config/QueueNames.java#L19), [`RabbitConfig.java:37`](../chat-service/src/main/java/ch/benedict/m321/chatservice/config/RabbitConfig.java#L37) |
| Gesendet wird über den Standard-Exchange mit dem Queue-Namen als Routing-Key | [`MessagePublisher.java:35`](../chat-service/src/main/java/ch/benedict/m321/chatservice/service/MessagePublisher.java#L35) — die zweistellige Form von `convertAndSend` |
| Es gibt genau einen Erzeuger; niemand sonst schreibt in diese Queue | [`MessagePublisher.java:34-40`](../chat-service/src/main/java/ch/benedict/m321/chatservice/service/MessagePublisher.java#L34-L40) |

**Folge für den `batch-writer`:** Er darf die Queue und die DLQ selbst deklarieren, aber
**mit exakt denselben Eigenschaften**. Eine Deklaration mit abweichenden Argumenten
lehnt RabbitMQ mit `PRECONDITION_FAILED` ab, und der Dienst startet nicht.

### 2.2 Die Nutzlast

Die Nachricht ist JSON. Die Felder ergeben sich aus dem `record`, den der `chat-service`
serialisiert — [`ChatMessage.java:14-20`](../chat-service/src/main/java/ch/benedict/m321/chatservice/dto/ChatMessage.java#L14-L20):

| JSON-Feld | JSON-Typ | Java-Typ | Nie leer? | Beleg |
|---|---|---|---|---|
| `id` | String (UUID) | `UUID` | ja | [`ChatMessage.java:15`](../chat-service/src/main/java/ch/benedict/m321/chatservice/dto/ChatMessage.java#L15); Wert aus `UUID.randomUUID()` in [`MessageService.java:38`](../chat-service/src/main/java/ch/benedict/m321/chatservice/service/MessageService.java#L38) |
| `roomId` | String (UUID) | `UUID` | ja | [`ChatMessage.java:16`](../chat-service/src/main/java/ch/benedict/m321/chatservice/dto/ChatMessage.java#L16); vorgelagert `@NotNull` in [`SendMessageRequest.java:21`](../chat-service/src/main/java/ch/benedict/m321/chatservice/dto/SendMessageRequest.java#L21) |
| `senderId` | String | `String` | ja | [`ChatMessage.java:17`](../chat-service/src/main/java/ch/benedict/m321/chatservice/dto/ChatMessage.java#L17); vorgelagert `@NotBlank` in [`SendMessageRequest.java:22`](../chat-service/src/main/java/ch/benedict/m321/chatservice/dto/SendMessageRequest.java#L22) |
| `senderName` | String | `String` | ja | [`ChatMessage.java:18`](../chat-service/src/main/java/ch/benedict/m321/chatservice/dto/ChatMessage.java#L18); vorgelagert `@NotBlank` in [`SendMessageRequest.java:23`](../chat-service/src/main/java/ch/benedict/m321/chatservice/dto/SendMessageRequest.java#L23) |
| `content` | String | `String` | ja | [`ChatMessage.java:19`](../chat-service/src/main/java/ch/benedict/m321/chatservice/dto/ChatMessage.java#L19); vorgelagert `@NotBlank` in [`SendMessageRequest.java:24`](../chat-service/src/main/java/ch/benedict/m321/chatservice/dto/SendMessageRequest.java#L24) |
| `sentAt` | String (Zeitpunkt) | `Instant` | ja | [`ChatMessage.java:20`](../chat-service/src/main/java/ch/benedict/m321/chatservice/dto/ChatMessage.java#L20); Wert aus `Instant.now()` in [`MessageService.java:39`](../chat-service/src/main/java/ch/benedict/m321/chatservice/service/MessageService.java#L39) |

Beispiel, wie eine Nachricht auf der Leitung aussieht:

Beispiel — das ist **keine erfundene Nutzlast**, sondern die Nachricht, die am
30.09.2026 tatsächlich in `chat.persist` lag, ausgelesen über die Management-API:

```json
{
  "id": "147a747e-f792-4081-80e3-6c9c7227dfb8",
  "roomId": "3f2b1c4e-0000-0000-0000-000000000001",
  "senderId": "anna",
  "senderName": "Anna Muster",
  "content": "Formatpruefung sentAt",
  "sentAt": "2026-09-30T16:31:38.474905082Z"
}
```

> **Die Schreibweise von `sentAt`, gemessen:** ISO-8601 in UTC, mit **neun**
> Nachkommastellen — also Nanosekunden, nicht Millisekunden. Damit bestätigt sich der
> Kommentar in
> [`RabbitConfig.java:56-57`](../chat-service/src/main/java/ch/benedict/m321/chatservice/config/RabbitConfig.java#L56-L57).
> Abgelesen wurde am Inhalt der Queue, nicht an der HTTP-Antwort des `chat-service`.
>
> **Zwei Folgen für den `batch-writer`:**
>
> 1. **Nicht selbst parsen.** Nach `Instant` deserialisieren und Jackson machen lassen.
>    Wer die Zeichenkette von Hand zerlegt, stolpert über die neun Stellen.
> 2. **Genauigkeit geht beim Schreiben verloren.** `timestamptz` löst in PostgreSQL nur
>    bis Mikrosekunden auf, die letzten drei Stellen fallen weg. Das ist hingenommen und
>    folgenlos — siehe 4.1.

### 2.3 Message-Properties

Der `chat-service` setzt keine Properties von Hand. Alles unten sind daher Standardwerte
von Spring AMQP 3.2.12 — der Fassung, die Spring Boot 3.5.16 mitbringt. Nachgesehen im
Jar aus dem lokalen Maven-Repository, nicht aus dem Gedächtnis:

| Property | Wert | Herkunft |
|---|---|---|
| `content_type` | `application/json` | `Jackson2JsonMessageConverter` übergibt den MimeType `application/json` an seine Oberklasse; der Konverter wird gesetzt in [`RabbitConfig.java:61`](../chat-service/src/main/java/ch/benedict/m321/chatservice/config/RabbitConfig.java#L61) |
| `content_encoding` | `UTF-8` | `AbstractJackson2MessageConverter.DEFAULT_CHARSET = StandardCharsets.UTF_8` |
| `delivery_mode` | `2` (persistent) | `MessageProperties.DEFAULT_DELIVERY_MODE = MessageDeliveryMode.PERSISTENT` |
| `priority` | `0` | `MessageProperties.DEFAULT_PRIORITY` |
| Header `__TypeId__` | `ch.benedict.m321.chatservice.dto.ChatMessage` | `AbstractJavaTypeMapper.DEFAULT_CLASSID_FIELD_NAME = "__TypeId__"` |

**Am laufenden Stack bestätigt (30.09.2026).** Dieselbe Nachricht wie in 2.2, aus
`chat.persist` ausgelesen, meldete genau diese Werte:

```json
"properties": {
  "priority": 0,
  "delivery_mode": 2,
  "headers": { "__TypeId__": "ch.benedict.m321.chatservice.dto.ChatMessage" },
  "content_encoding": "UTF-8",
  "content_type": "application/json"
},
"exchange": "",
"routing_key": "chat.persist"
```

Damit ist auch 2.1 nachgemessen: `exchange` ist leer, der Routing-Key trägt den
Queue-Namen — genau die zweistellige `convertAndSend`-Form aus
[`MessagePublisher.java:35`](../chat-service/src/main/java/ch/benedict/m321/chatservice/service/MessagePublisher.java#L35).

`delivery_mode: 2` zusammen mit der `durable`-Queue ist die Zusage, auf der S4 und S7
aufbauen: Nachrichten in `chat.persist` überleben einen Neustart des Brokers.

### 2.4 Der Header `__TypeId__` — die wichtigste Stelle dieses Abschnitts

Der Header nennt eine Klasse, die der `batch-writer` **nie haben wird**:
`ch.benedict.m321.chatservice.dto.ChatMessage` liegt im Paket des anderen Dienstes.
Der `batch-writer` bekommt eine eigene Kopie in seinem eigenen Paket — genau so, wie es
[`ChatMessage.java:9-12`](../chat-service/src/main/java/ch/benedict/m321/chatservice/dto/ChatMessage.java#L9-L12)
ausdrücklich vorsieht: *„der Vertrag ist das JSON, nicht diese Klasse."*

Dass das trotzdem funktioniert, liegt an einem Standardwert: `DefaultJackson2JavaTypeMapper`
steht auf `TypePrecedence.INFERRED`. Der Zieltyp wird also aus der **Signatur der
Listener-Methode** abgeleitet, und `__TypeId__` wird ignoriert. Derselbe Mechanismus lässt
S5 durch, wo der Header ganz fehlt.

**Daraus folgen zwei Verbote für die Umsetzung:**

1. `TypePrecedence` **nicht** auf `TYPE_ID` stellen und keinen `DefaultClassMapper` mit
   festem Standardtyp setzen. Beides würde den fremden Klassennamen wieder
   massgeblich machen — und der ist im `batch-writer` nicht ladbar.
2. Die Listener-Methode braucht einen **konkreten** Parametertyp
   (`List<ChatMessage>`). Bei `List<Object>` gäbe es nichts zu folgern.

### 2.5 Was der Vertrag nicht zusagt

- **Keine Reihenfolge.** Bei mehreren `chat-service`-Instanzen ist die Reihenfolge
  innerhalb eines Raums nicht garantiert (PLANUNG.md §7, Punkt 3). Der `batch-writer`
  stellt sie nicht her und sortiert nicht.
- **Keine Einmaligkeit.** Dieselbe `id` kann mehrfach ankommen — nach einem Absturz
  liefert RabbitMQ erneut aus. Siehe 3.2.2.
- **Keine Zusage, dass `sentAt` monoton steigt.** Es ist die Uhr des sendenden
  Containers.

---

## 3. Verhalten

### 3.1 Normalfall

```
chat.persist ──> Stapel sammeln ──> entdoppeln ──> ein INSERT ──> COMMIT ──> ACK
                 (500 oder 200 ms)   (nach id)                              (ganzer Stapel)
```

**Die Stapelbildung übernimmt der Listener-Container**, nicht selbstgebauter Code:
`SimpleRabbitListenerContainerFactory` mit `consumerBatchEnabled = true`,
`batchSize = 500`, `batchReceiveTimeout = 200` (ms), `receiveTimeout = 200` (ms) und
`prefetch = 500`. Die Listener-Methode bekommt dann eine fertige `List<ChatMessage>`.

Warum diese Werte zusammengehören:

| Wert | Warum genau so |
|---|---|
| `batchSize = 500` | Aus PLANUNG.md §3.6 und §4.1. Bei 1'667 Nachrichten/s werden daraus rund 3 bis 5 Transaktionen pro Sekunde statt 1'667 |
| `batchReceiveTimeout = 200` ms | Damit eine einzelne Nachricht nicht ewig auf 499 Geschwister wartet. Im Unterricht sieht man die Zeile nach einem Fünftel einer Sekunde |
| `prefetch = 500` | **Muss mindestens so gross sein wie `batchSize`.** Prefetch ist die Obergrenze unbestätigter Nachrichten. Wäre er kleiner, könnte der Stapel nie voll werden und der Dienst liefe immer ins Zeitfenster |
| Bestätigung durch den Container nach fehlerfreiem Methodenende | Die Methode kehrt erst nach dem COMMIT zurück. Damit ist „bestätigt" gleichbedeutend mit „geschrieben" — das ist der Kern von At-least-once |

Der Schreibvorgang selbst:

1. Aus der Liste eine Abbildung `id → Nachricht` bauen und damit **innerhalb des Stapels
   entdoppeln**. Kommt dieselbe `id` zweimal im selben Stapel vor, bleibt eine übrig.
2. Ein einziges `INSERT` mit `JdbcTemplate.batchUpdate` und dem Zusatz
   `ON CONFLICT (id) DO NOTHING`.
3. COMMIT.
4. Rückkehr aus der Methode → der Container bestätigt den ganzen Stapel.

Kein JPA. `JdbcTemplate.batchUpdate` ist genau das, was hier gezeigt werden soll
(PLANUNG.md §2.1).

### 3.2 Fehlerfälle

#### 3.2.1 Die Datenbank ist nicht erreichbar — Szenario **S7**

**Verhalten:** Der Dienst versucht den Stapel erneut zu schreiben, mit wachsender
Wartezeit: 1 s, 2 s, 4 s, 8 s, danach immer 10 s. Unbegrenzt. Er lehnt die Nachrichten
**nie** ab und beendet sich nicht. Sobald die Datenbank antwortet, läuft der Stapel durch
und wird bestätigt.

**Warum so:** An `chat.persist` hängt eine Dead-Letter-Route
([`RabbitConfig.java:29-30`](../chat-service/src/main/java/ch/benedict/m321/chatservice/config/RabbitConfig.java#L29-L30)).
Jede Ablehnung mit `requeue=false` schiebt die Nachricht sofort nach `chat.dlq` — und
genau das tut Spring Boots eingebaute Wiederholung, wenn ihr die Versuche ausgehen.
Damit fiele S7 durch: die 300 Nachrichten wären in der DLQ statt in der Tabelle.

Die Regel dahinter, und das ist der eigentliche Lehrsatz:

> **Eine kaputte Nachricht gehört in die DLQ. Eine kaputte Umgebung nicht.**

Ein Datenbankausfall sagt nichts über die Nachricht aus. Sie erneut zuzustellen wird
irgendwann gelingen. Warten ist deshalb richtig und Wegwerfen falsch.

**Was das kostet, offen benannt:** Bei dauerhaft toter Datenbank wartet der Dienst
endlos. Sichtbar ist das nur im Log. Wir nehmen das in Kauf, weil die Alternative
— irgendwann aufgeben — Nachrichten verliert.

**Ein Detail, das beim Messen auffallen kann:** RabbitMQ schliesst einen Kanal, dessen
Verbraucher zu lange nicht bestätigt (`consumer_timeout`, Standard 30 Minuten). Die
Nachrichten gehen dabei **nicht** verloren und **nicht** in die DLQ, sie werden erneut
in die Queue gestellt. Bei den 15 Sekunden aus S7 passiert das nicht.

#### 3.2.2 Dieselbe Nachricht kommt zweimal — Szenario **S5**

**Woran ein Duplikat erkannt wird:** an `id`. Die UUID wird genau einmal vergeben, in
[`MessageService.java:38`](../chat-service/src/main/java/ch/benedict/m321/chatservice/service/MessageService.java#L38),
und ist laut PLANUNG.md §3.7 Primärschlüssel der Tabelle.

**Verhalten:** Zwei Verteidigungslinien.

1. **Im Stapel**, vor dem INSERT: Die Liste wird nach `id` entdoppelt. Das deckt den
   Fall ab, dass beide Kopien im selben Stapel landen — bei S5 sehr wahrscheinlich,
   weil die beiden Einlieferungen Millisekunden auseinanderliegen.
2. **In der Datenbank**: `ON CONFLICT (id) DO NOTHING`. Das deckt alles andere ab —
   zwei verschiedene Stapel, zwei verschiedene Instanzen (S6), zwei verschiedene Läufe
   nach einem Neustart.

**Warum beide und nicht nur eine:** Die Datenbank allein würde reichen, aber nur unter
einer Annahme über das Verhalten von Postgres bei zwei identischen Zeilen in **einem**
INSERT — eine Annahme, die in diesem Projekt niemand nachgemessen hat. Die Entdopplung
in Java kostet eine Schleife und macht die Annahme überflüssig. Umgekehrt reicht die
Schleife allein nicht, sobald die Kopien in verschiedenen Stapeln liegen.

**Was ausdrücklich nicht passiert:** Kein `SELECT` vor dem Einfügen. Zwei Instanzen
könnten gleichzeitig „gibt es nicht" lesen und beide einfügen — S6 würde damit
durchfallen.

**Was wir damit nicht behaupten:** Exactly-once. Wir machen Duplikate harmlos, wir
verhindern sie nicht (PLANUNG.md §3.6).

#### 3.2.3 Eine Nachricht ist nicht lesbar

**Verhalten:** Sie wird abgelehnt und landet über die bestehende Dead-Letter-Route in
`chat.dlq`. Der Stapel ringsum läuft normal weiter.

**Warum so:** Das ist das Gegenstück zu 3.2.1 und passiert von selbst — der
`DefaultExceptionStrategy` von Spring AMQP zählt `MessageConversionException` zu den
tödlichen Fehlern und lehnt ohne Requeue ab. Das ist genau richtig: Ein Wiederholen
würde nichts ändern, weil die Nachricht selbst das Problem ist. Ohne diese Ausnahme
gäbe es eine Endlosschleife.

**Für die Umsetzung heisst das: nichts tun.** Dieses Verhalten ist der Standard und darf
nicht wegkonfiguriert werden.

#### 3.2.4 In der Queue liegt ein Rückstau — Szenario **S4**

**Verhalten:** Der Dienst arbeitet den Rückstau in vollen 500er-Stapeln ab. 1000
wartende Nachrichten ergeben **2 Transaktionen**. Erlaubt sind 100.

**Warum das aufgeht:** Liegt beim Start schon etwas in der Queue, füllt sich der Puffer
sofort bis `batchSize`, das Zeitfenster kommt gar nicht zum Zug. Der Sicherheitsabstand
zur Grenze ist Faktor 50 — auch wenn die Stapel unsauber brechen, hält das Kriterium.

**Warum nichts verloren geht:** `chat.persist` ist `durable`
([`RabbitConfig.java:28`](../chat-service/src/main/java/ch/benedict/m321/chatservice/config/RabbitConfig.java#L28)),
die Nachrichten sind persistent (2.3). Dass der Verbraucher weg ist, stört den Broker
nicht — die Nachrichten warten.

#### 3.2.5 Zwei Instanzen laufen gleichzeitig — Szenario **S6**

**Verhalten:** Beide binden sich an **dieselbe** Queue `chat.persist`. RabbitMQ teilt die
Nachrichten auf; jede geht an genau einen Verbraucher. Keine Absprache zwischen den
Instanzen, kein gemeinsamer Zustand.

**Warum so:** Das ist das Muster **Competing Consumers** und der Punkt, an dem
Skalierung im Unterricht messbar wird (PLANUNG.md §4.2). Jede exklusive oder
instanzeigene Queue würde es zerstören — dann bekäme jede Instanz jede Nachricht, und
`ON CONFLICT` müsste die Hälfte wegwerfen.

**Wichtig für die Umsetzung:** Die Instanz darf sich **keine** Queue mit generiertem
Namen anlegen. Der Name ist fest `chat.persist`. Der Unterschied zum `web-gateway`, das
genau umgekehrt arbeitet, ist in PLANUNG.md §3.5 beschrieben.

#### 3.2.6 Der Dienst stürzt mitten im Stapel ab

**Verhalten:** Alles, was noch nicht bestätigt war, wird von RabbitMQ erneut zugestellt —
bis zu 500 Nachrichten. Ein Teil davon steht vielleicht schon in der Tabelle, wenn der
COMMIT durchging und nur die Bestätigung nicht mehr. Beim zweiten Durchlauf fängt
`ON CONFLICT (id) DO NOTHING` das ab.

**Warum so:** Bestätigt wird nach dem COMMIT, nicht davor. Das ist die Entscheidung für
At-least-once aus PLANUNG.md §3.6: lieber ein Duplikat, das folgenlos bleibt, als eine
verlorene Nachricht.

#### 3.2.7 Der Broker ist beim Start nicht erreichbar

**Verhalten:** Der Dienst startet trotzdem und verbindet sich erneut, sobald RabbitMQ da
ist. Er beendet sich nicht.

**Warum so:** In `docker compose up` starten alle Container fast gleichzeitig. Ein
Dienst, der beim ersten Fehlversuch aufgibt, macht S2 zu einem Glücksspiel. Der
`depends_on`-Eintrag mit `condition: service_healthy` deckt den Normalfall bereits ab;
das Wiederverbinden deckt den Rest.

### 3.3 Wann `chat.dlq` etwas enthält

Nach den Entscheidungen oben gibt es **genau einen** Weg in die DLQ: eine Nachricht, die
nicht nach `ChatMessage` lesbar ist (3.2.3). Datenbankausfälle führen nie dorthin, und
Duplikate auch nicht. In S5 und S7 muss `chat.dlq` deshalb leer bleiben — das ist in
Abschnitt 5 ein eigenes Messkriterium.

---

## 4. Datenmodell und Konfiguration

### 4.1 Die Tabelle `message`

Spalten nach PLANUNG.md §3.7:

```sql
CREATE TABLE IF NOT EXISTS message (
    id          uuid        PRIMARY KEY,
    room_id     uuid        NOT NULL,
    sender_id   varchar     NOT NULL,
    sender_name varchar     NOT NULL,
    content     text        NOT NULL,
    sent_at     timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_message_room_sent
    ON message (room_id, sent_at DESC);
```

| Spalte | Herkunft im JSON | Anmerkung |
|---|---|---|
| `id` | `id` | Primärschlüssel. Trägt die Entdopplung aus 3.2.2 |
| `room_id` | `roomId` | |
| `sender_id` | `senderId` | Die `sub`-Kennung aus Keycloak |
| `sender_name` | `senderName` | Bewusst denormalisiert, damit die Historie lesbar bleibt, wenn ein Konto verschwindet |
| `content` | `content` | |
| `sent_at` | `sentAt` | Serverzeit des `chat-service`, nicht die des Clients. Siehe Hinweis zur Genauigkeit unten |

Der Index deckt die einzige geplante Leseabfrage ab („die letzten 50 Nachrichten eines
Raums", PLANUNG.md §3.7). Er gehört trotzdem hierher, weil die Tabelle hier entsteht —
gelesen wird sie später vom `chat-service`.

**Zur Genauigkeit von `sent_at`:** In der Queue steht der Zeitpunkt mit Nanosekunden
(2.2), `timestamptz` speichert nur Mikrosekunden. Die letzten drei Stellen gehen also
verloren. Das nehmen wir hin und bauen **keine** eigene Spalte dafür:

- Für die Anzeige im Chat ist eine Mikrosekunde ohnehin weit feiner als nötig.
- Für die Entdopplung ist die Zeit bedeutungslos — die läuft über `id` (3.2.2).
- Die Sortierung im Index bleibt korrekt; nur zwei Nachrichten, die weniger als eine
  Mikrosekunde auseinanderliegen, könnten gleichauf landen. Eine garantierte Reihenfolge
  sagt der Vertrag ohnehin nicht zu (2.5).

**Abweichung von PLANUNG.md §3.7, bewusst:** Dort ist `room_id` ein Fremdschlüssel auf
`ROOM`. Hier bleibt die Spalte `uuid NOT NULL` **ohne** `REFERENCES`. Dafür gibt es zwei
Gründe — einen praktischen und einen grundsätzlichen.

**Praktisch:** Räume und Mitgliedschaften sind laut Auftrag nicht Teil dieser Aufgabe.
Die Tabelle `room` existiert also gar nicht, und ein Fremdschlüssel auf eine fehlende
Tabelle liesse sich nicht einmal anlegen.

**Grundsätzlich — und das bliebe richtig, auch wenn es die Tabelle `room` schon gäbe:**
Ein Fremdschlüssel würde das Schreiben einer Nachricht an einem Zustand scheitern lassen,
für den ein **anderer** Dienst zuständig ist. Kennt die Datenbank den Raum im Moment des
INSERT noch nicht — weil die Nachricht schneller war als die Anlage des Raums, weil ein
Raum inzwischen gelöscht wurde, weil zwei Dienste in unterschiedlicher Reihenfolge
schreiben —, dann bricht der INSERT. Der Stapel wird wiederholt, scheitert erneut, und
die Nachricht kommt nie an.

Genau diesen Zustand soll die Queue verhindern. Sie entkoppelt Zustellung und
Speicherung; ein Fremdschlüssel würde die Speicherung wieder an die Verfügbarkeit und
den Fortschritt eines fremden Dienstes binden und die Entkopplung damit rückgängig
machen. Dazu kommt die Reihenfolge der Ereignisse: Wenn der `batch-writer` schreibt, ist
die Nachricht beim Empfänger **längst angekommen** (PLANUNG.md §3.4). Sie danach an
einer Fremdschlüsselprüfung zu verlieren, wäre schlimmer als eine Zeile mit einer
`room_id`, zu der es noch keinen Raum gibt.

**Was wir dafür in Kauf nehmen:** Die Datenbank prüft nicht mehr, ob der Raum existiert.
Diese Prüfung gehört ohnehin dorthin, wo die Nachricht angenommen wird und wo man einem
Absender noch antworten kann — in den `chat-service`, **vor** der Queue. Hinter der
Queue gibt es niemanden mehr, dem man „diesen Raum gibt es nicht" sagen könnte.

### 4.2 Wo das Schema entsteht

Als **Init-Skript des Postgres-Containers**: `postgres/init/01-schema.sql`, im
Compose-Dienst `postgres` eingehängt nach `/docker-entrypoint-initdb.d/`. Das offizielle
Postgres-Image führt beim **ersten** Start mit leerem Datenverzeichnis alle `.sql`-Dateien
aus diesem Verzeichnis aus.

**Warum dort und nicht anderswo:**

- Es ist reines SQL, das man auf dem Beamer vorlesen kann. Kein Werkzeug dazwischen.
- Es braucht keine zusätzliche Abhängigkeit.
- Es läuft **einmal**, bevor irgendein `batch-writer` verbunden ist. Damit gibt es bei
  `--scale batch-writer=2` (S6) keinen Wettlauf zweier Instanzen, die gleichzeitig
  dieselbe Tabelle anlegen wollen.

**Was das kostet:** Das Skript läuft nur bei leerem Datenverzeichnis. Wer das Schema
ändert, muss das Volume löschen (`docker compose down -v`). Für ein Unterrichtsprojekt
ist das vertretbar; für ein System mit echten Daten wäre es das nicht — dort käme ein
Migrationswerkzeug zum Einsatz.

`CREATE TABLE IF NOT EXISTS` steht trotzdem im Skript, damit ein von Hand
nachgeschobener Lauf nichts kaputt macht.

### 4.3 Umgebungsvariablen

Alle Werte kommen aus `.env`, die Beispielwerte stehen in `.env.example`
(die echte `.env` steht in [`.gitignore:17`](../.gitignore#L17)).

| Variable | Zweck | Default im Dienst | Wer sie ausserdem liest |
|---|---|---|---|
| `POSTGRES_USER` | Benutzer für die Datenbankverbindung | `chat` | Der `postgres`-Container legt den Benutzer damit an |
| `POSTGRES_PASSWORD` | Passwort dazu | keiner — muss gesetzt sein | Der `postgres`-Container |
| `POSTGRES_DB` | Name der Datenbank | `chat` | Der `postgres`-Container legt sie damit an |
| `POSTGRES_HOST` | Rechnername der Datenbank | `localhost` | — |
| `RABBITMQ_HOST` | Rechnername des Brokers | `localhost` | `chat-service` |
| `RABBITMQ_USER` | Broker-Benutzer | `guest` | `chat-service`, `rabbitmq` |
| `RABBITMQ_PASSWORD` | Broker-Passwort | `guest` | `chat-service`, `rabbitmq` |

Die Defaults gelten beim Start **ausserhalb** von Docker, genau wie beim `chat-service`
([`application.yml:7-10`](../chat-service/src/main/resources/application.yml#L7-L10)).
Im Compose-Netz wird `POSTGRES_HOST=postgres` und `RABBITMQ_HOST=rabbitmq` gesetzt.

Für `POSTGRES_PASSWORD` gibt es bewusst **keinen** Default: ein Dienst, der sich
stillschweigend mit einem eingebauten Passwort verbindet, ist genau die Sorte Magie, die
in diesem Projekt nicht vorkommen soll.

Der Port ist fest `5432` und wird nicht zur Variable gemacht — der `chat-service` hält es
mit `5672` genauso ([`application.yml:8`](../chat-service/src/main/resources/application.yml#L8)).

### 4.4 Feste Werte, die keine Umgebungsvariable werden

`batchSize = 500`, `batchReceiveTimeout = 200`, `receiveTimeout = 200`, `prefetch = 500`
und die Wartezeiten der Wiederholung stehen in der Konfiguration des Dienstes —
**nicht** als Umgebungsvariable.

Begründung: Niemand hat verlangt, sie im Betrieb zu ändern. Eine Stellschraube, die
niemand dreht, ist eine Abstraktion auf Vorrat. Wer für eine Messreihe damit spielen
will, ändert den Wert und baut neu.

### 4.5 Neue Abhängigkeiten

Der Dienst braucht drei Einträge, die bisher in keinem `pom.xml` stehen. Die Versionen
kommen aus dem Spring-Boot-Eltern-POM ([`pom.xml:14`](../pom.xml#L14)); es kommt also
keine eigene Versionsnummer ins Projekt.

| Abhängigkeit | Wofür |
|---|---|
| `org.springframework.boot:spring-boot-starter-jdbc` | `JdbcTemplate` und der Verbindungspool |
| `org.postgresql:postgresql` | Der Treiber, zur Laufzeit |
| `org.testcontainers:postgresql` (Scope `test`) | Echte Datenbank im Test, passend zum RabbitMQ-Container, den der `chat-service` schon benutzt ([`chat-service/pom.xml:61-65`](../chat-service/pom.xml#L61-L65)) |

Bereits im Projekt vorhanden und ebenfalls gebraucht: `spring-boot-starter-amqp`,
`lombok`, `spring-boot-starter-test`, `spring-boot-testcontainers`,
`org.testcontainers:junit-jupiter`.

**Nicht** gebraucht: `spring-boot-starter-web`. Der Dienst hat keine REST-Schnittstelle
(1.3). Ohne den Starter fährt er als reine Anwendung ohne Webserver hoch — und kann
schon deshalb keinen Port öffnen.

---

## 5. Abnahmekriterien

Vorbereitung für alle Messungen:

```bash
cp .env.example .env          # danach Passwoerter anpassen
set -a; . ./.env; set +a      # POSTGRES_USER usw. in die Shell holen
```

Hilfsbefehle, die mehrfach vorkommen:

```bash
# Zeilen in der Tabelle zaehlen
psql_count() {
  docker compose exec -T postgres \
    psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t -A \
         -c "select count(*) from message;"
}

# Tiefe der Queues und Zahl der Verbraucher
queues() {
  docker compose exec -T rabbitmq \
    rabbitmqctl list_queues name messages consumers
}

# N Nachrichten ueber den chat-service senden
send() {
  docker run --rm --network chat-net curlimages/curl:latest sh -c "
    for i in \$(seq 1 $1); do
      curl -s -o /dev/null -X POST http://chat-service:8080/messages \
        -H 'Content-Type: application/json' \
        -d '{\"roomId\":\"3f2b1c4e-0000-0000-0000-000000000001\",
             \"senderId\":\"anna\",\"senderName\":\"Anna Muster\",
             \"content\":\"Nachricht \$i\"}';
    done"
}
```

`curlimages/curl` läuft **im Netz `chat-net`**, nicht auf dem Host. Damit muss für die
Messung kein Port geöffnet werden — die Ein-Port-Vorgabe bleibt auch beim Prüfen intakt.

### S1 — `mvn clean test` läuft in einem Lauf grün

```bash
mvn -q clean test
echo "Exit-Code: $?"
```

**Erfüllt, wenn:** Exit-Code `0`, und im Lauf sind Tests des Moduls `batch-writer`
enthalten, die mindestens abdecken: ein Stapel wird geschrieben (3.1), eine doppelte `id`
ergibt eine Zeile (3.2.2), ein Ausfall der Datenbank führt nicht zur Ablehnung (3.2.1).

### S2 — frischer Klon startet, kein Dienst veröffentlicht einen Port

```bash
docker compose up -d --build
docker compose ps --format '{{.Service}}\t{{.Status}}\t{{.Ports}}'
grep -n "ports:" docker-compose.yml
docker compose port batch-writer 8080 ; echo "Exit-Code: $?"
```

**Erfüllt, wenn:** alle Dienste `running` bzw. `healthy` sind, die Spalte `Ports` **keine**
Zuordnung der Form `0.0.0.0:...->...` zeigt, `grep` keinen echten `ports:`-Eintrag findet
(die erklärende Kommentarzeile in [`../docker-compose.yml`](../docker-compose.yml) zählt
nicht) und `docker compose port` mit einem Fehler endet, weil nichts veröffentlicht ist.

### S3 — 1000 Nachrichten sind nach spätestens 60 s in der Tabelle

```bash
send 1000
sleep 60
psql_count          # erwartet: 1000
queues              # erwartet: chat.persist -> 0 Nachrichten
```

**Erfüllt, wenn:** `psql_count` genau `1000` liefert und `chat.persist` bei `0`
Nachrichten steht.

### S4 — Rückstau wird in höchstens 100 Transaktionen geschrieben

`pg_stat_database.xact_commit` ist ein **Zähler, der seit dem Start der Datenbank
fortlaufend hochzählt**. Sein absoluter Stand sagt nichts über diesen Versuch aus — er
enthält auch alles, was vorher passiert ist. Gemessen wird deshalb als **Differenz**, in
drei Schritten:

1. Zählerstand **vorher** ablesen und merken (`XACT_VORHER`).
2. Den Versuch laufen lassen: 1000 Nachrichten in die Queue, dann den `batch-writer`
   starten und ihn den Rückstau abarbeiten lassen.
3. Zählerstand **nachher** ablesen (`XACT_NACHHER`) und **subtrahieren**.

Erst `XACT_NACHHER - XACT_VORHER` ist die Zahl der Transaktionen, die dieser Versuch
gekostet hat. Nur dieser Wert wird gegen die Grenze von 100 geprüft.

```bash
docker compose stop batch-writer

XACT_VORHER=$(docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -t -A -c "select xact_commit from pg_stat_database where datname = current_database();")

send 1000
queues                          # erwartet: chat.persist -> 1000, consumers -> 0

docker compose start batch-writer
sleep 30

XACT_NACHHER=$(docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -t -A -c "select xact_commit from pg_stat_database where datname = current_database();")

psql_count                                              # erwartet: 1000
echo "Transaktionen: $((XACT_NACHHER - XACT_VORHER))"   # erwartet: deutlich unter 100
```

**Erfüllt, wenn:** `psql_count` genau `1000` liefert und die Differenz
`XACT_NACHHER - XACT_VORHER` **höchstens 100** beträgt. Erwartet werden rund 2 bis 5.
Genau `2` wird es nie: In die Differenz zählen auch die Transaktionen der `psql`-Aufrufe
selbst und alles, was Postgres nebenher committet. Das ist der Grund, warum die Grenze
bei 100 liegt und nicht bei 2 — sie prüft die Grössenordnung, nicht die exakte Zahl.

### S5 — dieselbe Nachricht zweimal ergibt eine Zeile

Zweimal dieselbe Nutzlast direkt in `chat.persist` legen, nur mit
`content_type: application/json` und **ohne** `__TypeId__`:

```bash
VORHER=$(psql_count)

for versuch in 1 2; do
  docker run --rm --network chat-net curlimages/curl:latest \
    -s -u "$RABBITMQ_USER:$RABBITMQ_PASSWORD" \
    -H 'Content-Type: application/json' \
    -X POST 'http://rabbitmq:15672/api/exchanges/%2F/amq.default/publish' \
    -d '{"properties":{"content_type":"application/json","delivery_mode":2},
         "routing_key":"chat.persist",
         "payload":"{\"id\":\"11111111-2222-3333-4444-555555555555\",\"roomId\":\"3f2b1c4e-0000-0000-0000-000000000001\",\"senderId\":\"anna\",\"senderName\":\"Anna Muster\",\"content\":\"Doppelt\",\"sentAt\":\"2026-09-25T09:14:02.471Z\"}",
         "payload_encoding":"string"}'
done

sleep 5

docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t -A \
  -c "select count(*) from message where id = '11111111-2222-3333-4444-555555555555';"
queues        # erwartet: chat.dlq -> 0
psql_count    # erwartet: genau 1 mehr als $VORHER
```

**Erfüllt, wenn:** die Zählung auf die `id` genau `1` liefert, `chat.dlq` bei `0` steht
und `psql_count` gegenüber `$VORHER` um genau `1` gestiegen ist.

Die Management-API auf Port `15672` ist **nur im Netz `chat-net`** erreichbar; für diese
Messung wird sie nicht nach aussen veröffentlicht.

### S6 — zwei Instanzen teilen sich die Queue

```bash
docker compose up -d --scale batch-writer=2
sleep 10
queues              # erwartet: chat.persist -> consumers = 2

send 1000
sleep 60

psql_count          # erwartet: 1000
docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t -A \
  -c "select count(*) - count(distinct id) from message;"   # erwartet: 0
```

**Erfüllt, wenn:** `consumers` für `chat.persist` genau `2` ist, `psql_count` `1000`
liefert und die Differenz aus Zeilen und verschiedenen `id` genau `0` ist.

### S7 — Datenbankausfall wird ausgesessen, ohne Neustart von Hand

```bash
NEUSTARTS_VORHER=$(docker inspect -f '{{.RestartCount}}' \
  "$(docker compose ps -q batch-writer)")

docker compose stop postgres
send 300
sleep 15
docker compose start postgres
sleep 90

psql_count          # erwartet: 300 mehr als vor dem Versuch
queues              # erwartet: chat.persist -> 0, chat.dlq -> 0

docker inspect -f '{{.RestartCount}}' "$(docker compose ps -q batch-writer)"
docker compose ps batch-writer          # erwartet: durchgehend "running"
```

**Erfüllt, wenn:** alle 300 Nachrichten in der Tabelle stehen, `chat.dlq` bei `0` bleibt,
der `RestartCount` unverändert gegenüber `$NEUSTARTS_VORHER` ist und niemand den Dienst
von Hand angefasst hat.

### S8 — Quelltext hält sich an die Projektregeln, `.env` ist nicht im Repo

```bash
git ls-files batch-writer/
git ls-files --error-unmatch .env ; echo "Exit-Code: $?"   # erwartet: Fehler
git check-ignore -v .env                                    # erwartet: Treffer in .gitignore
grep -rn "password\|secret" batch-writer/src/main/resources/
```

**Erfüllt, wenn:**

- `git ls-files --error-unmatch .env` **fehlschlägt** (die Datei ist nicht versioniert)
  und `git check-ignore` sie in [`.gitignore:17`](../.gitignore#L17) nachweist.
- Die `grep`-Suche keine echten Werte findet, sondern nur Platzhalter der Form
  `${POSTGRES_PASSWORD}`.
- Beim Durchlesen des Quelltextes gilt, was [`../CLAUDE.md`](../CLAUDE.md) verlangt:
  Bezeichner und Log-Meldungen auf Englisch, Kommentare auf Deutsch; ein Ergebnis pro
  Zeile statt verschachtelter Aufrufe; `for`-Schleife statt Stream; jede Klasse und jede
  Methode mit einem Kommentar, der das *Warum* erklärt; sprechende Namen
  (`messageRepository`, nicht `repo`); keine Interfaces mit einer einzigen
  Implementierung; `@Slf4j` und `@RequiredArgsConstructor` statt handgeschriebener
  Logger und Konstruktoren; `record` für Datenklassen.
