# Spezifikation: Live-Zustellung im `chat-service`

**Modul M321 · Klasse IT3c · Bewertung 2**

Der `chat-service` bekommt einen Endpunkt `GET /stream`, über den er jede Nachricht sofort
an alle Clients schickt, die gerade im selben Raum zuhören. Das braucht es, weil heute
niemand eine Nachricht live bekommt — es funktioniert nur das Speichern.

Dieses Dokument beschreibt, *was* der Dienst tun muss — nicht, in welcher Reihenfolge man
ihn baut. Grundlage: [`../PLANUNG.md`](../PLANUNG.md) Abschnitt 3.4 (Nachrichtenfluss) und
3.5 (Queues und Exchanges), dazu der Vertrag aus
[`spec-batch-writer.md`](spec-batch-writer.md) Abschnitt 2. Die Regeln aus
[`../CLAUDE.md`](../CLAUDE.md) gelten für den Code, der daraus entsteht.

---

## 1. Zweck und Abgrenzung

### 1.1 Warum es diesen Teil gibt

Der `chat-service` schickt jede Nachricht zweimal los: in die Queue `chat.persist` und an
den Fanout-Exchange `chat.delivery`
([`MessagePublisher.java:35-36`](../chat-service/src/main/java/ch/benedict/m321/chatservice/service/MessagePublisher.java#L35-L36)).
Die erste Kopie holt der `batch-writer` ab. Die zweite Kopie geht heute verloren:

- An `chat.delivery` ist **keine Queue gebunden**. Vorgesehen war das `web-gateway`
  (PLANUNG.md §3.5), und das gibt es noch nicht.
- Eine Nachricht, die ein Exchange an keine Queue weiterleiten kann, verwirft RabbitMQ.
- Der Absender erfährt davon nichts. Dafür müsste die Nachricht mit `mandatory` gesendet
  werden, und das ist weder im Code noch in
  [`application.yml`](../chat-service/src/main/resources/application.yml) eingestellt.

Nachweisbar am laufenden Stack: In der Liste der Bindungen gibt es keine Zeile mit
`chat.delivery` als Quelle.

```bash
docker compose exec -T rabbitmq rabbitmqctl -q list_bindings source_name destination_name
```

Damit ist der Kern aus PLANUNG.md §3.4 — *„Die Nachricht ist beim Empfänger, bevor sie in
der Datenbank steht"* — bis heute nicht umgesetzt. Diese Ausbaustufe holt das nach.

### 1.2 Was der Dienst neu tut

1. Jede Instanz legt beim Start eine **eigene Zustell-Queue** an und bindet sie an
   `chat.delivery` (Abschnitt 4).
2. Ein Listener liest diese Queue mit **genau einem Thread**.
3. Jede Nachricht kommt in einen **Puffer ihres Raums**, der die letzten 200 Nachrichten
   hält (3.2.5).
4. Der neue Endpunkt `GET /stream?roomId=…` liefert `text/event-stream`. Jede Nachricht
   geht an alle offenen Streams ihres Raums.
5. Wer mit dem Header `Last-Event-ID` neu verbindet, bekommt die verpassten Nachrichten
   aus dem Puffer nachgeliefert, danach geht es live weiter.
6. Alle 15 s geht an jeden offenen Stream eine Kommentarzeile als Lebenszeichen.

### 1.3 Was der Dienst bewusst NICHT tut

| Nicht seine Aufgabe | Warum nicht |
|---|---|
| Token prüfen | Das Gateway ist der einzige Wachposten (PLANUNG.md §3.1). Keycloak und Gateway gibt es noch nicht, und von aussen ist der Dienst ohnehin nicht erreichbar |
| Einen Port veröffentlichen | Vorgabe: nur die Web-App ist über `localhost` erreichbar |
| Nachrichten über den Stream annehmen | SSE fliesst nur vom Server zum Client. Gesendet wird weiter über `POST /messages` |
| Räume und Mitgliedschaften prüfen | Nicht Teil dieser Aufgabe. Jede gültige UUID gilt als Raum |
| Speichern oder aus der Datenbank lesen | Gespeichert wird nur vom `batch-writer`. Das Nachholen bedient sich aus dem Puffer im Speicher, nicht aus der Datenbank (3.2.5) |
| Exactly-once unter Ausfällen zusagen | Wann „genau einmal" gilt und wann nicht, steht in 2.4 |

### 1.4 Abweichung von PLANUNG.md §3.4 und §3.5, bewusst

Die Planung sieht vor, dass das **`web-gateway`** `chat.delivery` konsumiert und per
**WebSocket** zustellt. In dieser Ausbaustufe macht es der **`chat-service`** per
**Server-Sent Events (SSE)**.

**Warum das für diese Ausbaustufe so ist:**

- Das Gateway setzt Keycloak voraus (PLANUNG.md §6, Schritt 2). Beides ist nicht Teil
  dieser Aufgabe.
- Für die Zustellung reicht eine Richtung, vom Server zum Client. Genau das ist SSE: ein
  gewöhnlicher `GET`, dessen Antwort offen bleibt.
- SSE lässt sich mit `curl` prüfen. Die Abnahme in Abschnitt 5 kommt deshalb ohne eigenen
  Test-Client aus.
- `SseEmitter` gehört zu `spring-webmvc`, das über `spring-boot-starter-web` schon im
  `chat-service` steckt. Es kommt keine neue Abhängigkeit dazu.
- Der Browser verbindet einen SSE-Stream nach einem Abbruch von selbst neu und schickt
  dabei die zuletzt empfangene ID im Header `Last-Event-ID` mit (2.3). WebSocket kennt
  dafür nichts Eingebautes.

**Was das für später bedeutet:**

- Kommt das Gateway dazu, reicht es `/stream` entweder an den `chat-service` weiter, oder
  es übernimmt das Muster selbst. Das Muster — eine eigene Queue pro Instanz am Fanout —
  bleibt dasselbe, nur der Dienst wechselt.
- Weil jede Instanz jede Nachricht bekommt, darf ein Client an **irgendeiner** Instanz
  hängen. Für die Zustellung braucht es keine Sticky Sessions (vgl. PLANUNG.md §7,
  Punkt 1).
- Die Kommentare in
  [`QueueNames.java:15`](../chat-service/src/main/java/ch/benedict/m321/chatservice/config/QueueNames.java#L15)
  und
  [`RabbitConfig.java:40-45`](../chat-service/src/main/java/ch/benedict/m321/chatservice/config/RabbitConfig.java#L40-L45)
  nennen noch das `web-gateway` als Verbraucher. Sie werden bei der Umsetzung angepasst.

PLANUNG.md §3.5 nennt ausserdem eine **exklusive** Queue pro Instanz. Diese Spezifikation
nimmt stattdessen `auto-delete` mit `x-expires`. Die Begründung steht in 3.2.2.

---

## 2. Vertrag

### 2.1 Was am Exchange `chat.delivery` ankommt

Dieser Abschnitt ist aus dem laufenden Code des `chat-service` hergeleitet.

| Aussage | Beleg |
|---|---|
| Der Exchange heisst `chat.delivery` | [`QueueNames.java:16`](../chat-service/src/main/java/ch/benedict/m321/chatservice/config/QueueNames.java#L16) |
| Er ist vom Typ `fanout`, `durable` und nicht `auto-delete` | [`RabbitConfig.java:48`](../chat-service/src/main/java/ch/benedict/m321/chatservice/config/RabbitConfig.java#L48) — `new FanoutExchange(name, true, false)` |
| Gesendet wird mit leerem Routing-Key `""` | [`MessagePublisher.java:23`](../chat-service/src/main/java/ch/benedict/m321/chatservice/service/MessagePublisher.java#L23), [`:36`](../chat-service/src/main/java/ch/benedict/m321/chatservice/service/MessagePublisher.java#L36) |
| Es ist **dasselbe Objekt** wie in `chat.persist`, umgewandelt vom selben Konverter | [`MessagePublisher.java:35-36`](../chat-service/src/main/java/ch/benedict/m321/chatservice/service/MessagePublisher.java#L35-L36), [`RabbitConfig.java:60-61`](../chat-service/src/main/java/ch/benedict/m321/chatservice/config/RabbitConfig.java#L60-L61) |
| `chat.persist` wird **vor** `chat.delivery` beschickt | [`MessagePublisher.java:35-36`](../chat-service/src/main/java/ch/benedict/m321/chatservice/service/MessagePublisher.java#L35-L36) |
| Es gibt genau einen Erzeuger | [`MessagePublisher.java:34-40`](../chat-service/src/main/java/ch/benedict/m321/chatservice/service/MessagePublisher.java#L34-L40) |

**Die Nutzlast** ist deshalb Zeichen für Zeichen dieselbe wie in `chat.persist`. Die
Felder `id`, `roomId`, `senderId`, `senderName`, `content` und `sentAt` sind in
[`spec-batch-writer.md`](spec-batch-writer.md) §2.2 belegt, ebenso die Schreibweise von
`sentAt`: ISO-8601 in UTC mit neun Nachkommastellen, zum Beispiel
`"2026-09-30T16:31:38.474905082Z"`.

**Die Message-Properties** sind dieselben wie in spec-batch-writer.md §2.3:
`content_type: application/json`, `content_encoding: UTF-8`, `delivery_mode: 2`,
`priority: 0` und der Header `__TypeId__` mit dem Wert
`ch.benedict.m321.chatservice.dto.ChatMessage`. Anders sind nur zwei Felder der Zustellung
selbst: `exchange` heisst `chat.delivery` statt `""`, und `routing_key` ist `""` statt
`chat.persist`.

**Bestätigen am laufenden Stack:** Eine Test-Queue an `chat.delivery` binden, eine
Nachricht senden und sie über die Management-API auslesen. Die API ist nur im Netz
`chat-net` erreichbar.

```bash
api() {   # api <Methode> <Pfad> [JSON]
  docker run --rm --network chat-net curlimages/curl:latest -s \
    -u "$RABBITMQ_USER:$RABBITMQ_PASSWORD" -H 'Content-Type: application/json' \
    -X "$1" "http://rabbitmq:15672/api/$2" ${3:+-d "$3"}
}
api PUT    'queues/%2F/probe' '{"durable":false,"auto_delete":false}'
api POST   'bindings/%2F/e/chat.delivery/q/probe' '{"routing_key":""}'
send PROBE 1 "$ROOM_A"                     # Hilfsbefehl aus Abschnitt 5
api POST   'queues/%2F/probe/get' '{"count":1,"ackmode":"ack_requeue_false","encoding":"auto"}'
api DELETE 'queues/%2F/probe'
```

Erwartet: `payload` mit den sechs Feldern wie oben, `exchange: "chat.delivery"`,
`routing_key: ""`, dieselben `properties` wie in spec-batch-writer.md §2.3.

**Folgen für den `chat-service` als Empfänger:**

1. **Die Klasse ist die eigene.** Der Header `__TypeId__` nennt
   `ch.benedict.m321.chatservice.dto.ChatMessage` — beim `chat-service` liegt diese
   Klasse im eigenen Paket. Eine Kopie wie im `batch-writer` braucht es nicht.
   Umgewandelt wird trotzdem nach dem Parametertyp der Listener-Methode, wie in
   spec-batch-writer.md §2.4 beschrieben. Damit funktioniert es auch, wenn der Header
   fehlt.
2. **`data:` ist der Body, unverändert.** Die Listener-Methode bekommt die Nachricht in
   zwei Formen: als `ChatMessage`, um `id` und `roomId` zu lesen, und als rohe
   `org.springframework.amqp.core.Message`, deren Body als UTF-8-Text in `data:` geht.
   Die Nachricht wird **nicht** aus `ChatMessage` neu in JSON geschrieben.

   *Warum:* Das Prüfskript verlangt `data:` „im selben Format wie in `chat.persist`".
   Beim Neuschreiben bliebe `sentAt` voraussichtlich gleich — `Instant` speichert
   Nanosekunden, und es wäre derselbe `ObjectMapper`. Die Gleichheit hinge dann aber an
   dieser Annahme. Wer den Body unverändert weiterreicht, muss die Frage gar nicht erst
   stellen. Derselbe Text landet auch im Puffer, damit sieht eine nachgeholte Nachricht
   genau so aus wie eine live zugestellte.

### 2.2 Was am Stream hinausgeht

| Punkt | Festlegung |
|---|---|
| Methode und Pfad | `GET /stream` |
| Query-Parameter | `roomId`, Pflicht, eine UUID |
| Antwort | `200`, `Content-Type: text/event-stream`, die Verbindung bleibt offen |
| `id:` | die `id` der Nachricht (UUID) |
| `data:` | der Body aus 2.1, unverändert, auf **einer** Zeile |
| Lebenszeichen | alle 15 s die Kommentarzeile `:ping` |

Ein Ereignis auf der Leitung — zwei Zeilen und eine Leerzeile, die das Ereignis abschliesst:

```
id:147a747e-f792-4081-80e3-6c9c7227dfb8
data:{"id":"147a747e-f792-4081-80e3-6c9c7227dfb8","roomId":"3f2b1c4e-0000-0000-0000-000000000001","senderId":"anna","senderName":"Anna Muster","content":"Formatpruefung sentAt","sentAt":"2026-09-30T16:31:38.474905082Z"}

```

Das Lebenszeichen auf der Leitung:

```
:ping

```

**Kein Leerzeichen nach dem Doppelpunkt.** `SseEmitter` schreibt `id:`, `data:` und `:`
ohne Leerzeichen danach: Im Bytecode von `SseEmitter$SseEventBuilderImpl`
(`spring-webmvc` 6.2.19) heissen die Zeichenketten `"id:"` und `"data:"` (Beleg: Anhang,
Befehl A1). Der SSE-Standard erlaubt beide
Schreibweisen; ein einzelnes Leerzeichen nach dem Doppelpunkt entfernt der Client. Die
Prüfbefehle in Abschnitt 5 suchen deshalb nach `^data:`, nicht nach `^data: `.

**Fehlende oder ungültige `roomId`:** Antwort **`400 Bad Request`**, es wird kein Stream
geöffnet. Das erledigt Spring selbst, sobald der Parameter als `@RequestParam UUID roomId`
deklariert ist: Fehlt er, scheitert die Bindung des Parameters, ist er keine UUID, scheitert
die Umwandlung. Beides beantwortet Spring mit `400`. Eine gültige UUID, zu der es noch
keine Nachricht gab, ergibt `200` und einen Stream, auf dem vorerst nur das Lebenszeichen
kommt. Räume verwaltet niemand — dieselbe Begründung wie beim fehlenden Fremdschlüssel in
spec-batch-writer.md §4.1.

**`data:` ist immer genau eine Zeile.** Ein Zeilenumbruch in `content` steht im JSON als
die zwei Zeichen `\n`, nicht als echter Umbruch. Der Body vom Broker enthält deshalb keinen
Umbruch, und `data:` bleibt einzeilig. Ginge doch ein echter Umbruch durch, würde `SseEmitter`
ihn auf mehrere `data:`-Zeilen verteilen (die Zeichenkette `"\ndata:"`, ebenfalls Befehl A1), und
der Client würde sie wieder zusammensetzen — die Prüfung „eine Zeile pro Nachricht" fiele
dann aber durch.

**Warum ein Lebenszeichen alle 15 s:**

1. Dass ein Client weg ist, merkt der Server nur beim Schreiben. In einem ruhigen Raum
   würde nie geschrieben, und tote Streams blieben unbegrenzt in der Liste. Das
   Lebenszeichen sorgt dafür, dass regelmässig geschrieben wird. Wie schnell ein toter
   Stream danach auffällt, hängt vom Betriebssystem ab und ist nicht gemessen (3.2.6).
2. Zwischenstationen wie später das Gateway oder ein Proxy schliessen Verbindungen, über die
   lange nichts fliesst. 15 s liegen deutlich unter den üblichen Leerlaufgrenzen.

**Kein Zeitlimit am Stream.** Jeder `SseEmitter` wird mit Zeitlimit `0` erzeugt, das heisst
„nie". Ohne eigene Angabe gälte das Async-Zeitlimit des Servers: In Tomcat 10.1.55 steht
`Connector.asyncTimeout` auf 30 000 ms (Beleg: Befehl A2), und
`spring.mvc.async.request-timeout` ist im `chat-service` nicht gesetzt. Jeder Stream würde
dann nach 30 s abbrechen.

### 2.3 Der Request-Header `Last-Event-ID`

**Wer ihn setzt:** Der Browser (`EventSource`) setzt ihn von selbst, und zwar **nur bei der
automatischen Neuverbindung** nach einem Abbruch. Die erste Verbindung kommt ohne den
Header. Ein Skript muss ihn von Hand mitschicken, zum Beispiel
`curl -H 'Last-Event-ID: <uuid>'`.

**Was drin steht:** die `id:` des letzten Ereignisses, das der Client bekommen hat — also
die UUID einer Nachricht.

**Was der Dienst damit macht:**

| Fall | Verhalten |
|---|---|
| Header fehlt oder ist leer | Stream beginnt live |
| ID steht im Puffer des Raums | Erst alle Nachrichten **nach** dieser ID aus dem Puffer, dann live (3.2.5) |
| ID ist unbekannt: zu alt für den Puffer, aus einem anderen Raum, nach einem Neustart der Instanz oder keine UUID | **Kein Fehler.** Stream beginnt live, dazu eine Log-Zeile auf `INFO` |

**Warum bei einer unbekannten ID kein Fehler kommt:** Auf eine Antwort, die nicht `200` mit
`text/event-stream` ist, gibt `EventSource` die Verbindung endgültig auf und verbindet nicht
mehr neu. Ein Fehler würde den Client also dauerhaft aussperren. Deshalb geht es live
weiter. Nachteil: Der Client kann eine Lücke nicht von „nichts verpasst"
unterscheiden. Das steht in 2.4.

### 2.4 Was der Vertrag nicht zusagt

**Wann „genau einmal" gilt.** Im Normalbetrieb — alle Instanzen laufen, kein Stream bricht
ab — kommt jede Nachricht an jeden Stream ihres Raums **genau einmal**. Das stellt der
Server her: eine Queue pro Instanz (3.2.1). Beim Nachholen an derselben Instanz gilt
dasselbe, dafür sorgt das Schloss pro Raum (3.2.5). Beides misst die Abnahme (S3, S7).

**Wann es nicht gilt.** Bei Ausfällen sagt der Vertrag kein Exactly-once zu:

- **Lücke**, wenn die `Last-Event-ID` unbekannt ist (2.3) oder wenn eine hängende Instanz
  Nachrichten aus ihrer Queue verliert (3.2.4).
- **Lücke oder Doppelte**, wenn ein Client an einer *anderen* Instanz neu verbindet und
  kurz vor seiner `Last-Event-ID` zwei Absender fast gleichzeitig gesendet haben. Dann kann
  die Reihenfolge der beiden Nachrichten in den Queues der beiden Instanzen verschieden
  sein (3.2.5).

**Weitere Grenzen:**

- **Keine Reihenfolge über verschiedene Absender.** Bei mehreren `chat-service`-Instanzen
  ist sie innerhalb eines Raums nicht garantiert (PLANUNG.md §7, Punkt 3). Die Reihenfolge
  *eines* Absenders bleibt erhalten, wenn er die nächste Nachricht erst nach der `202`
  schickt (3.2.1).
- **Keine Zustellung an Clients, die nicht verbunden sind** — ausser über den Puffer: die
  letzten 200 Nachrichten pro Raum, solange die Instanz läuft.
- **Keine Zusage, dass die Nachricht schon in der Datenbank steht.** Meistens steht sie dort
  noch nicht (Stapel bis 200 ms, spec-batch-writer.md §3.1).
- **Kein Zugriffsschutz.** Wer im Netz `chat-net` ist, kann jeden Raum mitlesen. Das ist mit
  „Gateway als einziger Wachposten" vereinbar (PLANUNG.md §3.1).

Ein Client darf Duplikate zusätzlich an der `id` verwerfen. Zugesagt wird „genau einmal"
aber vom Server, nicht vom Client — das Prüfskript verwirft nichts.

---

## 3. Verhalten

### 3.1 Normalfall: eine Nachricht vom `POST` bis zum `data:`

```
POST /messages
  └─> MessageController ─> MessageService ─> MessagePublisher
                                               ├─> chat.persist ─────────────> batch-writer ─> DB
                                               └─> chat.delivery (fanout)
                                                     ├─> Zustell-Queue Instanz A ─> DeliveryListener A ─> RoomStream ─> Streams an A
                                                     └─> Zustell-Queue Instanz B ─> DeliveryListener B ─> RoomStream ─> Streams an B
```

1. `MessageController.send` nimmt die Anfrage an und prüft sie
   ([`MessageController.java:33-37`](../chat-service/src/main/java/ch/benedict/m321/chatservice/controller/MessageController.java#L33-L37)).
2. `MessageService.accept` vergibt `id` und `sentAt`
   ([`MessageService.java:38-39`](../chat-service/src/main/java/ch/benedict/m321/chatservice/service/MessageService.java#L38-L39)).
3. `MessagePublisher.publish` legt die Nachricht in `chat.persist` und danach an
   `chat.delivery`
   ([`MessagePublisher.java:35-36`](../chat-service/src/main/java/ch/benedict/m321/chatservice/service/MessagePublisher.java#L35-L36)).
4. Der Fanout-Exchange gibt **jeder** gebundenen Zustell-Queue eine eigene Kopie — eine pro
   laufender Instanz.
5. In jeder Instanz liest `DeliveryListener` die eigene Zustell-Queue. Er holt sich bei
   `RoomStreamRegistry` das `RoomStream`-Objekt des Raums und übergibt ihm die Nachricht.
6. `RoomStream` legt die Nachricht in den Puffer des Raums und schickt sie an jeden
   offenen Stream dieses Raums: `id:` ist die `id`, `data:` der unveränderte Body.

**Die neuen Klassen**, alle im `chat-service`:

| Klasse | Aufgabe |
|---|---|
| `StreamController` | `GET /stream`: prüft `roomId`, erzeugt den `SseEmitter`, meldet ihn bei `RoomStream` an |
| `RoomStreamRegistry` | Hält pro `roomId` genau ein `RoomStream`-Objekt in einer `HashMap`. Beide Methoden sind `synchronized`: `getOrCreate(roomId)` legt das Objekt beim ersten Bedarf an, `allRoomStreams()` gibt eine Kopie aller Objekte zurück |
| `RoomStream` | Ein Raum: Puffer der letzten 200 Nachrichten und Liste der offenen Streams. Alle vier Methoden — `publish`, `subscribe`, `unsubscribe`, `sendHeartbeat` — sind `synchronized`. Das ist das Schloss pro Raum (3.2.5) |
| `DeliveryListener` | Liest die Zustell-Queue der Instanz |
| `HeartbeatSender` | Holt alle 15 s mit `allRoomStreams()` alle Räume und ruft für jeden `sendHeartbeat()` auf. Er schreibt nie selbst in einen Stream, sondern immer durch das Schloss des Raums |
| `BufferedMessage` (`record`) | Ein Eintrag im Puffer: `id` und der Body als Text |

Queue und Bindung entstehen als `@Bean` in der bestehenden `RabbitConfig` (Abschnitt 4).

**Die Nachricht kommt an Instanz A an, der Client hängt an Instanz B.** Die Instanzen
sprechen nicht miteinander und wissen nichts voneinander. Instanz A legt die Nachricht nur an
den Exchange. Der Broker gibt Instanz B über deren eigene Zustell-Queue eine eigene Kopie,
und B stellt sie ihrem Client zu. Instanz A bekommt ebenfalls eine Kopie, legt sie in ihren
Puffer und findet für diesen Raum keinen Stream.

### 3.2 Fälle aus den Szenarien

#### 3.2.1 Zwei Instanzen, zwei Räume — Szenario **S3**

**Verhalten:** An jeder der beiden Instanzen hängt ein Client für Raum A. Über jede Instanz
gehen 200 Nachrichten an Raum A, zusammen 400. Beide A-Clients bekommen alle 400, jede genau
einmal, und die Nachrichten eines Absenders in der Reihenfolge, in der er sie gesendet hat.
Clients in Raum B bekommen keine, nur das Lebenszeichen.

**Wo nach Raum gefiltert wird: in der Instanz.** Ein Fanout-Exchange kann nicht filtern, er
gibt jeder gebundenen Queue jede Nachricht. Jede Instanz bekommt also alle Nachrichten und
sucht in `RoomStreamRegistry` nach dem Raum. Gibt es dort keinen Stream, landet die
Nachricht nur im Puffer.

*Warum nicht am Broker:* Dafür bräuchte es einen Topic-Exchange mit der `roomId` als
Routing-Key, und jede Instanz müsste ihre Bindungen zur Laufzeit an- und abmelden, sobald
ein Client einen Raum betritt oder verlässt. Ausserdem müsste der bestehende Exchange einen
anderen Typ bekommen. RabbitMQ lehnt das als `PRECONDITION_FAILED` ab, solange der alte
Exchange existiert. Das Filtern in der Instanz ist ein einziges Nachschlagen in einer Map.
Nachteil: Jede Instanz liest jede Nachricht. Für den Puffer (3.2.5) ist das aber sogar
nötig.

**Warum jede Nachricht genau einmal ankommt:**

- Pro Instanz gibt es **eine** Zustell-Queue mit **einer** Bindung an den Exchange. Jede
  Instanz bekommt also genau eine Kopie.
- Ein Client hängt mit seinem Stream an genau einer Instanz und steht dort genau einmal in
  der Liste seines Raums.
- Die Queue-Namen sind pro Instanz verschieden (Abschnitt 4). Mit einer gemeinsamen Queue
  bekäme nur eine der beiden Instanzen die Nachricht (3.2.2).

**Warum die Reihenfolge eines Absenders erhalten bleibt:** Eine Queue gibt ihre Nachrichten
in der Reihenfolge heraus, in der sie eingereiht wurden. Die Zustell-Queue wird von
**genau einem Thread** gelesen (`concurrency: 1`, Abschnitt 4.1), und dieser Thread schickt
eine Nachricht vollständig an alle Streams, bevor er die nächste nimmt. Mit mehreren Threads
liefen zwei Nachrichten gleichzeitig durch die Methode, und Nachricht 6 könnte beim Senden
Nachricht 5 überholen. Der Prefetch von 50 ändert daran nichts: Er legt nur fest, wie viele
Nachrichten auf Vorrat beim Listener liegen, verarbeitet werden sie trotzdem eine nach der
anderen.

*Voraussetzung:* Der Absender schickt die nächste Nachricht erst nach der `202` für die
vorige. Die Sendeschleife der Abnahme tut das. Streng zugesagt ist die Reihenfolge damit
trotzdem nicht: Der `chat-service` sendet ohne Publisher Confirms, die `202` heisst also
„an den Broker geschickt", nicht „vom Broker eingereiht". In der Praxis liegen zwischen zwei
Anfragen Millisekunden, das Einreihen dauert Mikrosekunden. Die Abnahme misst die
Reihenfolge (S3).

**Zwei Absender gleichzeitig.** Die Zusage gilt für jeden Absender einzeln. Wie sich die
Nachrichten zweier Absender untereinander mischen, ist offen (2.4): Es hängt davon ab, welcher
`POST` zuerst beim Broker ankommt. Geprüft wird deshalb pro Absender. Aus dem Stream werden nur
die Nachrichten eines Absenders herausgezogen, und deren Nummern müssen streng aufsteigen.
Nachrichten des anderen Absenders dürfen dazwischen liegen. Das macht der Hilfsbefehl
`in_order` in Abschnitt 5.

#### 3.2.2 Eine Instanz verschwindet — Szenario **S4**

**Verhalten:** Nach `docker compose up -d --scale chat-service=1` ist die Zustell-Queue der
gestoppten Instanz nach spätestens rund 10 s gelöscht. Nach 30 s ist genau eine
Zustell-Queue übrig, die der laufenden Instanz.

**Die drei Möglichkeiten, eine Queue aufräumen zu lassen:**

| Eigenschaft | Wann RabbitMQ die Queue löscht | Schwäche |
|---|---|---|
| `exclusive` | Wenn die Verbindung, die sie angelegt hat, geschlossen wird | Nur diese eine Verbindung darf die Queue benutzen. Verbindet Spring nach einer Störung neu, bevor der Broker die alte Verbindung aufgegeben hat, scheitert das erneute Anlegen mit `RESOURCE_LOCKED` |
| `auto-delete` | Wenn der letzte Consumer weg ist — abgemeldet oder mit seinem Kanal bzw. seiner Verbindung verschwunden | Hatte die Queue **nie** einen Consumer, wird sie nie gelöscht |
| `x-expires` | Wenn die Queue so lange *unbenutzt* war: kein Consumer, nicht neu angelegt, kein `basic.get` (RabbitMQ-Doku, Anhang B2) | Solange der Broker den Consumer noch für lebendig hält, ist die Queue nicht unbenutzt |

**Entscheidung: `auto-delete` und zusätzlich `x-expires = 10000`.**

**Was beim Herunterskalieren genau passiert:**

1. Docker schickt `SIGTERM`. Spring fährt herunter und meldet dabei den Consumer beim
   Broker ab. Die Queue verliert ihren letzten Consumer, und `auto-delete` löscht sie sofort.
2. Offene Streams halten das Herunterfahren auf: `server.shutdown` steht in Spring Boot
   3.5.16 standardmässig auf `graceful` (Beleg: Befehl A5), und ein offener Stream ist eine laufende Anfrage. Nach 10 s
   beendet Docker den Prozess deshalb mit `SIGKILL`.
3. Spätestens dann schliesst das Betriebssystem die TCP-Verbindung zum Broker. Der Broker
   erfährt das sofort, der Consumer ist weg, und `auto-delete` löscht die Queue.

In beiden Wegen ist es **`auto-delete`**, das die Queue löscht, nach höchstens rund 10 s.
`x-expires` kommt bei S4 gar nicht zum Zug. Bis zur Messung nach 30 s bleiben rund 20 s
Reserve.

**Wofür dann `x-expires`:** für die Lücke von `auto-delete`. Stirbt eine Instanz, nachdem sie
die Queue angelegt hat, aber bevor ihr Consumer läuft — etwa weil der Start mittendrin
abbricht —, hatte die Queue nie einen Consumer. `auto-delete` würde sie nie löschen, und sie
füllte sich bis zur Grenze mit Kopien, die niemand liest. `x-expires` räumt sie nach 10 s
weg. Im Normalbetrieb liegen zwischen Anlegen und Lesen Millisekunden, ein Wiederverbinden
legt die Queue neu an und setzt die Frist damit zurück. 10 s sind also lang genug, damit
keine benutzte Queue verschwindet, und kurz genug für die 30 s aus S4.

**Was keine der drei Eigenschaften leistet:** Hängt eine Instanz, statt zu sterben, sieht
der Broker ihren Consumer weiter als lebendig. Dann hilft nur der Heartbeat — siehe 3.2.4.
S4 stoppt die Instanz aber, es friert sie nicht ein.

**Warum `exclusive` nicht genommen wird:** Es würde bei S4 ebenso funktionieren. Aber
`auto-delete` hängt am Consumer, nicht an einer bestimmten Verbindung. Ein Wiederverbinden
kann daher nie an `RESOURCE_LOCKED` scheitern.

**Warum es keine gemeinsame Queue für alle Instanzen sein darf:** An einer gemeinsamen Queue
wären die Instanzen Competing Consumers. RabbitMQ gäbe jede Nachricht nur **einer** von
ihnen, und der Client an der anderen bekäme sie nie — S3 fiele durch. Das ist genau das
Gegenteil des `batch-writer`, bei dem gerade die gemeinsame Queue gewollt ist
(spec-batch-writer.md §3.2.5).

#### 3.2.3 Datenbank und batch-writer sind weg — Szenario **S5**

**Verhalten:** Die Nachrichten erscheinen innerhalb von 5 s im Stream, obwohl Postgres und
`batch-writer` gestoppt sind. Sobald beide wieder laufen, stehen alle Nachrichten in der
Tabelle `message`.

**Warum:** Der Zustellweg berührt die Datenbank an keiner Stelle — weder beim Zustellen noch
beim Nachholen, denn der Puffer liegt im Speicher (3.2.5). Der `chat-service` hat nicht
einmal eine Datenbankverbindung: In seinem `pom.xml` steht kein JDBC-Starter, in
`application.yml` keine `datasource`. Die Kopie für die Datenbank wartet unterdessen in
`chat.persist`. Die Queue ist `durable`, die Nachrichten sind persistent
(spec-batch-writer.md §2.3). Sobald der `batch-writer` wieder läuft, arbeitet er den Rückstau
ab (spec-batch-writer.md §3.2.4).

**Warum das gewollt ist:** PLANUNG.md §3.4 trennt Zustellen und Speichern absichtlich.
Speichern darf langsam sein oder eine Weile ausfallen, Zustellen nicht. Hinge das Zustellen
an der Datenbank, stünde bei jedem Datenbankproblem der ganze Chat still, obwohl die
Nachricht längst beim Broker liegt.

#### 3.2.4 Eine Instanz hängt — Szenario **S6**

**Verhalten:** Wird eine von zwei Instanzen mit `docker pause` eingefroren, zeigt
`rabbitmqctl list_queues` für ihre Zustell-Queue nie mehr als 950 Nachrichten. Der Client an
der gesunden Instanz bekommt weiter alle Nachrichten.

**Was beim Einfrieren passiert:** `docker pause` hält den Prozess an, aber die
TCP-Verbindung zum Broker bleibt offen. Für den Broker hat die Queue weiter einen Consumer.
Sie wird also **nicht** gelöscht und bekommt vom Fanout weiter jede Kopie, ohne dass jemand
sie liest. Ohne Grenze wüchse sie mit jeder gesendeten Nachricht. Bei einem langen Hänger
erreicht RabbitMQ irgendwann seine Speichergrenze und blockiert dann **alle** Sender — auch
den Weg nach `chat.persist`. Eine einzige hängende Instanz könnte so den ganzen Chat
anhalten.

**Die Grenze:** `x-max-length = 900` mit `x-overflow = drop-head`.

| Variante | Was bei voller Queue passiert | Folge für die anderen Queues am Fanout | Folge für den Client an der hängenden Instanz |
|---|---|---|---|
| `drop-head` (gewählt) | Die **älteste** Nachricht dieser Queue wird verworfen | Keine. Jede Queue hat ihre eigene Grenze | Behält die neuesten 900. Die Lücke liegt bei den älteren |
| `reject-publish` | Die **neue** Nachricht wird von dieser Queue abgewiesen | Keine. Laut RabbitMQ-Doku (Anhang B1) geht die Nachricht trotzdem *„to all other queues which can enqueue it"* | Behält die ältesten 900, das Neueste fehlt |

Gewählt ist `drop-head` aus zwei Gründen. Im Chat ist das Neueste wichtiger als das Älteste. Und mit
`reject-publish` bekäme der Absender ein `basic.nack`, sobald Publisher Confirms
eingeschaltet sind. Eine einzige kranke Instanz würde dann jedes Senden als Fehler melden.
Mit `drop-head` merkt der Absender nichts.

**Warum 900 und nicht 1000 — der Prefetch:** Laut RabbitMQ-Doku (Anhang B1) zählen unbestätigte
Nachrichten nicht zur Grenze (*„Messages unacknowledged by consumers do not count toward the
limit"*). `rabbitmqctl list_queues messages` zählt sie aber mit — dort steht die Summe aus
wartenden und unbestätigten Nachrichten. An eine hängende Instanz hat der Broker bereits so
viele Nachrichten ausgeliefert, wie der Prefetch erlaubt, und keine davon wird bestätigt.

- Mit dem Standard von Spring AMQP (`DEFAULT_PREFETCH_COUNT = 250` in
  `spring-rabbit` 3.2.12, Beleg: Befehl A3) und einer Grenze von 1000 zeigt die Messung bis zu **1250** — S6
  fiele durch.
- Mit Prefetch **50** und Grenze **900** zeigt sie höchstens **950**. Das sind 50 Nachrichten
  Reserve unter der 1000.

**Was nach rund 60 s passiert — der Heartbeat:** Auch die Heartbeats der Instanz sind
eingefroren. Der Java-Client fordert einen Heartbeat von 60 s an
(`ConnectionFactory.DEFAULT_HEARTBEAT = 60` in `amqp-client` 5.25.0, Beleg: Befehl A4), und
`spring.rabbitmq.requested-heartbeat` ist nicht gesetzt. Bleiben die Heartbeats aus, erklärt
der Broker die Verbindung nach rund 60 s für tot und schliesst sie. Damit ist der Consumer
weg, und `auto-delete` löscht die Queue mitsamt ihren bis zu 950 Nachrichten. Gemessen ist
die genaue Zeit noch nicht. Sichtbar wird sie bei der Abnahme von S6, wenn die Queue aus
`delivery_queues` verschwindet.

**Nach dem Fortsetzen** gibt es zwei Fälle:

| Dauer des Einfrierens | Was danach passiert |
|---|---|
| Kürzer als der Heartbeat | Die Queue besteht noch. Die Instanz stellt die 50 vorab gelieferten Nachrichten zu, danach die bis zu 900 neuesten aus der Queue. Dazwischen fehlt, was `drop-head` verworfen hat |
| Länger als der Heartbeat | Der Java-Client merkt, dass der Broker die Verbindung geschlossen hat. Der Listener-Container baut alle 5 s eine neue Verbindung auf (`DEFAULT_RECOVERY_INTERVAL = 5000` in `spring-rabbit` 3.2.12, Befehl A3). Auf jeder neuen Verbindung legt Springs `RabbitAdmin` alle Queues, Exchanges und Bindungen an, die als `@Bean` deklariert sind. Die Zustell-Queue entsteht also mit **demselben Namen** neu und wird wieder gebunden, und die Zustellung läuft weiter. Alles, was dazwischen gesendet wurde, fehlt |

*Voraussetzung für den zweiten Fall:* Queue und Bindung sind `@Bean`s und werden nicht einmalig
von Hand im Code angelegt. Der Name wird einmal beim Start gebildet (Abschnitt 4) und ändert
sich beim Wiederverbinden nicht.

**Was der Client an der hängenden Instanz verliert:** alles, was `drop-head` verworfen hat,
und bei einem langen Hänger alles bis zum Wiederverbinden. Den Puffer seiner Instanz füllt
dieselbe Queue, er hat also dieselbe Lücke. Zurückholen kann der Client die Nachrichten nur,
wenn er mit `Last-Event-ID` an der **gesunden** Instanz neu verbindet — deren Puffer ist
lückenlos —, und nur die letzten 200 seines Raums (3.2.5).

**Was die gesunde Instanz davon merkt:** nichts. Sie hat ihre eigene Queue mit eigener Grenze.

**Keine Dead-Letter-Route auf `chat.dlq`.** Was `drop-head` verwirft, verschwindet. `chat.dlq`
gehört dem Schreibweg, und der `batch-writer` misst in seinen Szenarien, dass sie leer bleibt
(spec-batch-writer.md §3.3). Verworfene Live-Kopien dort abzulegen, würde diese Messung
verfälschen — und sie sind ohnehin nicht verloren, denn die Kopie in `chat.persist` landet in
der Datenbank.

#### 3.2.5 Client verbindet neu — Szenario **S7**

**Verhalten:** Ein Client verliert die Verbindung, in seinem Raum werden 50 Nachrichten
gesendet, und er verbindet mit `Last-Event-ID` neu. Er bekommt genau diese 50, jede einmal, in
der Reihenfolge, in der sie gesendet wurden, danach geht es live weiter.

**Woher die 50 kommen — die geprüften Quellen:**

| Quelle | Ergebnis der Prüfung |
|---|---|
| Datenbank | **Verworfen.** Die `id` ist eine zufällige UUID, „nach X" lässt sich daran nicht ablesen. Man müsste erst `sent_at` von X nachschlagen. X steht aber unter Umständen noch gar nicht in der Datenbank — der `batch-writer` schreibt in Stapeln bis 200 ms, und in S5 ist die Datenbank ganz weg. `sent_at` hat dort nur Mikrosekunden (spec-batch-writer.md §4.1), `data:` sähe beim Nachholen also anders aus als live. Und der `chat-service` bräuchte einen Datenbankzugang, den er heute nicht hat (3.2.3) |
| **Puffer im Speicher, pro Raum** | **Gewählt.** Jede Instanz bekommt dank Fanout jede Nachricht und legt sie in den Puffer ihres Raums — auch wenn gerade niemand zuhört. Die Reihenfolge im Puffer ist die Reihenfolge der Queue. „Nach X" ist einfach die Position hinter X in der Liste. Sortiert wird nichts, die Datenbank spielt keine Rolle |
| Anderes: RabbitMQ Streams mit Offsets, eine Queue pro Client | Möglich, aber jeweils ein ganzes neues Konzept für die Klasse. Eine Queue pro Client müsste ausserdem weiterleben, solange der Client weg ist — wie lange, weiss niemand |

**Die vier Prüffragen, beantwortet für den Puffer:**

1. *Zufällige UUID:* Sortiert wird nicht. Der Puffer hält die Nachrichten in der Reihenfolge,
   in der die Queue sie geliefert hat. Gesucht wird die Position von X.
2. *Stapel des `batch-writer`:* spielt keine Rolle. Der Puffer wird aus der Zustell-Queue
   gefüllt, nicht aus der Datenbank.
3. *Neuverbindung an einer anderen Instanz:* funktioniert, denn jede Instanz hat jede
   Nachricht in ihrem Puffer, sofern sie zu der Zeit lief. Für die Nachrichten eines
   einzelnen Absenders ist die Reihenfolge in allen Instanz-Queues dieselbe. Nur bei zwei
   Absendern, die fast gleichzeitig senden, kann sie sich zwischen den Instanzen
   unterscheiden (2.4).
4. *Nahtstelle zwischen Nachholen und live:* siehe nächster Absatz.

**Die Nahtstelle: ein Schloss pro Raum.** Während eine Instanz nachholt, kommen neue
Nachrichten an. Damit dabei nichts fehlt und nichts doppelt kommt, haben die beiden
Methoden von `RoomStream`, die den Puffer berühren, dasselbe Schloss — sie sind
`synchronized`:

- `publish(nachricht)` — aufgerufen vom Listener: Nachricht in den Puffer legen, dann an
  alle Streams des Raums senden.
- `subscribe(stream, lastEventId)` — aufgerufen von `/stream`: alle Nachrichten nach
  `lastEventId` aus dem Puffer an den neuen Stream senden, **dann** den Stream in die Liste
  eintragen.

Weil beide Methoden nie gleichzeitig laufen, liegt jede Nachricht entweder schon im Puffer,
wenn `subscribe` ihn liest, und wird nachgeholt — oder sie kommt erst danach, wenn der
Stream schon eingetragen ist, und wird live zugestellt. Beides gleichzeitig oder keins von
beiden ist nicht möglich. Die Reihenfolge stimmt auch an der Nahtstelle: Solange
nachgeholt wird, wartet `publish`, und die nächste Live-Nachricht kann sich nicht zwischen
die nachgeholten schieben.

Damit liegt die Zusage „genau die 50, keine doppelt" beim **Server**. Der Client muss dafür
nichts tun, und das Prüfskript tut auch nichts.

**Wer sonst noch an die Liste der Streams geht.** Drei Arten von Threads arbeiten mit
derselben Liste: der Listener-Thread (`publish`), die Threads von Tomcat (`subscribe` und über
die Rückrufe aus 3.2.6 auch `unsubscribe`) und der Thread des Lebenszeichens
(`sendHeartbeat`). Deshalb gehen **alle vier** Methoden durch dasselbe Schloss. Liefe das
Lebenszeichen daran vorbei, könnte es die Liste durchlaufen, während `subscribe` oder
`unsubscribe` sie gerade ändert — das ergäbe eine `ConcurrentModificationException`.

Zusätzlich senden `publish` und `sendHeartbeat` über eine **Kopie** der Liste. Grund: Scheitert
das Senden an einen Stream, wird er ausgetragen, und das kann im selben Thread mitten in der
Schleife passieren. Ein `synchronized`-Schloss hält den eigenen Thread nicht auf. Ohne Kopie gäbe
es deshalb trotz Schloss eine `ConcurrentModificationException`.

*Nachteil:* Während eine Instanz nachholt, wartet der Listener. Bei höchstens 200 kurzen
Ereignissen sind das Millisekunden. Weil es ein Schloss **pro Raum** ist, warten andere
Räume nur, wenn der Listener gerade eine Nachricht genau dieses Raums zustellen will.

**Puffergrösse N = 200 pro Raum.**

- *Warum 200:* S7 verlangt 50. 200 ist das Vierfache und deckt auch einen kurzen
  Netzunterbruch in einem belebten Raum ab. Mehr würde das Nachholen im Schloss verlängern
  und den Speicher vergrössern, ohne dass ein Szenario es verlangt.
- *Wenn X schon aus dem Puffer gefallen ist:* X gilt als unbekannt, der Stream beginnt live,
  und eine Log-Zeile hält es fest (2.3). Der Client hat eine Lücke und kann sie nicht
  erkennen — für den Dienst sieht eine zu alte ID genau so aus wie eine, die es nie gab.
- *Speicher, geschätzt, nicht gemessen:* Ein Eintrag besteht aus der UUID und dem Body als
  Text. Der Body der Beispielnachricht aus 2.2 hat rund 220 Zeichen. Mit dem Overhead der
  Java-Objekte sind das grob 0,3 bis 0,5 KB pro Eintrag, also höchstens rund **100 KB pro
  Raum** bei vollem Puffer. 100 Räume brauchen damit etwa 10 MB, 1000 Räume etwa 100 MB.
- *Was dabei offen bleibt:* `content` hat keine Längenbegrenzung
  ([`SendMessageRequest.java:24`](../chat-service/src/main/java/ch/benedict/m321/chatservice/dto/SendMessageRequest.java#L24)),
  lange Texte brauchen entsprechend mehr. Und ein Raum bekommt seinen Puffer bei der ersten
  Nachricht und behält ihn, solange die Instanz läuft. Der Speicher wächst also mit der Zahl
  der Räume, die seit dem Start etwas gesendet haben. Für den Unterricht mit einer Handvoll
  Räume spielt das keine Rolle. Für einen Lasttest mit zufälligen Raum-IDs wäre es ein
  offener Punkt.

**Grenzen:**

- Eine frisch gestartete Instanz hat leere Puffer. Verbindet ein Client dort neu, ist seine
  ID unbekannt.
- Bei einer Neuverbindung an einer anderen Instanz gilt die Einschränkung aus 2.4 für fast
  gleichzeitige Absender.

#### 3.2.6 Weitere Fälle

**Client trennt die Verbindung.** `SseEmitter` meldet das Ende über drei Rückrufe:
`onCompletion`, `onTimeout` und `onError`. Alle drei rufen `RoomStream.unsubscribe` auf, das
durch das Schloss des Raums geht (3.2.5). Der Puffer des Raums bleibt, denn er wird für das
Nachholen gebraucht.

Bricht die Verbindung ab, merkt der Server es erst beim Schreiben — und oft nicht beim ersten
Mal. Die Daten landen zuerst im TCP-Puffer des Betriebssystems. Erst wenn die Gegenseite mit
einem `RST` antwortet oder die TCP-Zeitgrenzen des Betriebssystems ablaufen, scheitert ein
späteres Schreiben. Hat der Client sauber getrennt, geschieht das in der Regel beim ersten oder
zweiten Schreiben danach. Bricht die Leitung stumm ab, kann es Minuten dauern. Gemessen ist
beides nicht. Das Lebenszeichen sorgt nur dafür, dass auch in ruhigen Räumen überhaupt
geschrieben wird.

**Ein Fehler beim Senden an einen Client** — etwa weil die Verbindung gerade abgerissen ist —
wird **pro Client** abgefangen. Dieser eine Stream wird ausgetragen, alle anderen bekommen die
Nachricht trotzdem. Eine Exception verlässt die Listener-Methode nie. Das ist zwingend: Der
Listener-Container des `chat-service` läuft mit den Voreinstellungen von Spring Boot, und dort
würde eine Nachricht, deren Verarbeitung mit einer Exception endet, zurück in die Queue
gelegt — und beim nächsten Versuch wieder scheitern, endlos. Als zweite Absicherung steht
`default-requeue-rejected` auf `false` (Abschnitt 4.1).

**Eine ungültige Nachricht in der Zustell-Queue.**

- *Kein gültiges JSON:* Die Umwandlung scheitert mit einer `MessageConversionException`. Spring
  wertet das als endgültigen Fehler und lehnt die Nachricht ohne Zurücklegen ab. Weil die
  Zustell-Queue keine Dead-Letter-Route hat (3.2.4), ist sie damit weg. Der Log zeigt eine
  Warnung.
- *Gültiges JSON, aber `id` oder `roomId` fehlt:* Die `ChatMessage` des `chat-service` prüft
  ihre Felder nicht, anders als die Kopie im `batch-writer`. Der Listener prüft deshalb selbst:
  Fehlt eines der beiden Felder, schreibt er eine Warnung und kehrt normal zurück. Die Nachricht
  ist damit bestätigt und verworfen. Ohne diese Prüfung liefe eine `NullPointerException`
  durch die Methode.

**Der Broker ist beim Start nicht erreichbar.** Im Normalfall deckt das `depends_on` mit
`condition: service_healthy` ab
([`docker-compose.yml:26-28`](../docker-compose.yml#L26-L28)). Fehlt der Broker trotzdem, startet
der Dienst, und der Listener-Container versucht es alle 5 s erneut. `/stream` nimmt
Verbindungen an, auf denen bis dahin nur das Lebenszeichen kommt. Queue und Bindung entstehen
mit der ersten Verbindung. Ein `POST /messages` wird in dieser Zeit mit `503` abgelehnt, wie
bisher
([`MessageExceptionHandler.java:21-27`](../chat-service/src/main/java/ch/benedict/m321/chatservice/controller/MessageExceptionHandler.java#L21-L27)).
Startet der Broker neu, ist die Zustell-Queue weg, denn sie ist nicht `durable`. Beim
Wiederverbinden legt `RabbitAdmin` sie neu an, wie in 3.2.4. Was während des Neustarts
gesendet wurde, fehlt live und im Puffer.

**Viele Clients im selben Raum.** Der Listener schickt jede Nachricht der Reihe nach an alle
Streams des Raums. Weil `data:` der unveränderte Body ist, wird pro Nachricht nichts neu
umgewandelt — es ist derselbe Text für alle. Schwachstelle: Es gibt nur einen
Listener-Thread. Ist ein Client so langsam, dass sein TCP-Puffer voll ist, wartet der Thread beim
Schreiben auf ihn — und mit ihm alle Räume dieser Instanz. Für diese Ausbaustufe ist das
hingenommen.

---

## 4. Queues

| Name | Typ / Eigenschaften | Angelegt von | Gelesen von | Lebensdauer | Grenze | Bei Grenze | Begründung |
|---|---|---|---|---|---|---|---|
| `chat.persist` | Classic Queue, `durable`, Dead-Letter über `""` nach `chat.dlq` | `chat-service` und `batch-writer`, mit gleichen Eigenschaften | `batch-writer`, alle Instanzen an derselben Queue (Competing Consumers) | Dauerhaft, überlebt einen Broker-Neustart | Keine | — | Auf dem Schreibweg darf nichts verloren gehen. Ein Rückstau muss warten können (spec-batch-writer.md §3.2.4). Unverändert |
| `chat.dlq` | Classic Queue, `durable` | `chat-service` und `batch-writer` | Niemand, nur von Hand zur Analyse | Dauerhaft | Keine | — | Kaputte Nachrichten des Schreibwegs werden aufbewahrt (spec-batch-writer.md §3.3). Unverändert. Die Zustell-Queues leiten **nicht** hierher (3.2.4) |
| Zustell-Queue pro Instanz: `chat.delivery.<HOSTNAME>.<UUID>` | Classic Queue, **nicht** `durable`, `auto-delete`, `x-expires = 10000`, `x-max-length = 900`, `x-overflow = drop-head`, keine Dead-Letter-Route. Gebunden an `chat.delivery` mit Routing-Key `""` | Jede `chat-service`-Instanz beim Start, samt Bindung, als `@Bean` in `RabbitConfig` | Nur die eigene Instanz: ein Thread, Prefetch 50 | Solange die Instanz verbunden ist. Danach sofort gelöscht (`auto-delete`), spätestens 10 s nach dem letzten Gebrauch (`x-expires`) | 900 wartende Nachrichten, mit den 50 unbestätigten höchstens 950 | Die älteste Nachricht wird verworfen | Live-Kopien sind nur für die eigene Instanz nützlich. Nach einem Broker-Neustart legt die Instanz die Queue neu an, `durable` bringt also nichts. Zahlen: 3.2.2 und 3.2.4 |

**Wie jede Instanz einen eindeutigen Namen bekommt.** Der Name wird **einmal beim Start**
gebildet:

```
chat.delivery.<HOSTNAME>.<zufällige UUID>
z. B. chat.delivery.3f9c1a7b2e44.9b2d6c1e-5a0f-4c3b-9e8d-1f2a3b4c5d6e
```

- Die **UUID** macht den Namen eindeutig. Sie wird beim Start mit `UUID.randomUUID()`
  erzeugt und gilt, bis der Prozess endet. Damit sind auch zwei Instanzen verschieden, die
  denselben Hostnamen haben — zwei Spring-Kontexte in derselben JVM, wie in einem Test mit
  zwei Instanzen, oder zwei Starts auf demselben Rechner ausserhalb von Docker.
- Der **Hostname** ist nur für Menschen da. Docker setzt `HOSTNAME` in jedem Container auf die
  kurze Container-ID. So lässt sich in `rabbitmqctl list_queues` ablesen, welche Queue zu
  welchem Container gehört — das braucht die Abnahme von S6. Ausserhalb von Docker fehlt die
  Variable, unter Windows zum Beispiel. Dann steht dort `local`.
- Der fertige Name wird beim Start auf `INFO` ins Log geschrieben.

**Warum das bei `--scale` wichtig ist:** Alle Instanzen laufen mit demselben Image und
derselben Konfiguration. Ein Name aus der Konfiguration wäre in allen Instanzen gleich. Alle
würden sich dann an **eine** Queue hängen, und jede Nachricht ginge nur an eine Instanz
(3.2.2). Ein Test mit zwei Instanzen wäre dann aus dem falschen Grund grün oder rot.

Der Exchange `chat.delivery` bleibt, wie er ist
([`RabbitConfig.java:46-49`](../chat-service/src/main/java/ch/benedict/m321/chatservice/config/RabbitConfig.java#L46-L49)).

### 4.1 Neue Umgebungsvariablen und feste Werte

**In `.env.example` kommt nichts dazu.** Die einzige neue Variable ist `HOSTNAME`, und die setzt
Docker selbst in jedem Container. Gelesen wird sie in `application.yml` als `${HOSTNAME:local}`.

**Feste Werte**, alle in der Konfiguration des `chat-service`, keiner als Umgebungsvariable:

| Wert | Wo | Begründung |
|---|---|---|
| `concurrency: 1` | `spring.rabbitmq.listener.simple` | Reihenfolge (3.2.1) |
| `prefetch: 50` | `spring.rabbitmq.listener.simple` | Höchststand in S6 (3.2.4) |
| `default-requeue-rejected: false` | `spring.rabbitmq.listener.simple` | Keine Endlosschleife bei Fehlern (3.2.6) |
| `x-max-length = 900`, `x-overflow = drop-head` | Queue-Bean in `RabbitConfig` | 3.2.4 |
| `x-expires = 10000`, `auto-delete`, nicht `durable` | Queue-Bean in `RabbitConfig` | 3.2.2 |
| Puffer: 200 Nachrichten pro Raum | Konstante in `RoomStream` | 3.2.5 |
| Lebenszeichen alle 15 s | Konstante in `HeartbeatSender` | 2.2 |
| Zeitlimit des `SseEmitter`: `0` (nie) | Konstante in `StreamController` | 2.2 |

Begründung wie in spec-batch-writer.md §4.4: Niemand hat verlangt, diese Werte im Betrieb zu
ändern. Eine Stellschraube, die niemand dreht, ist eine Abstraktion auf Vorrat.

Die drei Listener-Werte gelten für alle Listener des `chat-service`. Das ist richtig so, denn
`DeliveryListener` ist der einzige.

**Neue Abhängigkeiten: keine.** `SseEmitter` kommt mit `spring-boot-starter-web`, der Listener
mit `spring-boot-starter-amqp`, `@Scheduled` für das Lebenszeichen gehört zum Spring-Kern. Für
`@Scheduled` braucht es nur `@EnableScheduling` an einer Konfigurationsklasse.

---

## 5. Abnahmekriterien

Die Szenarien laufen **in dieser Reihenfolge auf demselben Stack, ohne Aufräumen
dazwischen**. Zählungen in der Datenbank sind deshalb Differenzen, wie in spec-batch-writer.md
§5. Zählungen im Stream sind absolut: Jeder Lauf schreibt seinen Stream in eine eigene Datei.

**Kein Port ist offen.** Alle Messungen laufen in einem `curlimages/curl`-Container im Netz
`chat-net`. Dort ist jede Instanz über ihren Containernamen erreichbar, zum Beispiel
`it3c-m321-chat-service-1`. Der Dienstname `chat-service` landet bei einer beliebigen Instanz.

### Hilfsbefehle

Vorbereitung (Git Bash oder eine Linux-Shell, im Wurzelverzeichnis):

```bash
cp .env.example .env
set -a; . ./.env; set +a

ROOM_A=aaaaaaaa-0000-0000-0000-000000000001
ROOM_B=bbbbbbbb-0000-0000-0000-000000000002
```

```bash
# Namen der laufenden chat-service-Container, einer pro Zeile
instances() {
  docker compose ps --format '{{.Name}}' chat-service | sort
}

# Einem Raum an einer bestimmten Instanz zuhören: listen <Instanz> <Raum> <Sekunden> [Last-Event-ID]
# -N schaltet die Pufferung von curl aus, --max-time beendet den Befehl.
listen() {
  docker run --rm --network chat-net curlimages/curl:latest -sN --max-time "$3" \
    ${4:+-H "Last-Event-ID: $4"} \
    "http://$1:8080/stream?roomId=$2"
}

# Nachrichten senden: send <Lauf> <Anzahl> <Raum> [Ziel] [Absender]
# Ziel ist ein Containername, ohne Angabe der Dienstname chat-service.
# Absender ohne Angabe: anna. Im Text stehen Lauf, Absender und Nummer, zum Beispiel
# "S3 ben Nachricht 17" — so lässt sich jeder Absender für sich prüfen.
send() {
  local ZIEL=${4:-chat-service}
  local ABSENDER=${5:-anna}
  docker run --rm --network chat-net curlimages/curl:latest sh -c "
    for i in \$(seq 1 $2); do
      curl -s -o /dev/null -X POST http://$ZIEL:8080/messages \
        -H 'Content-Type: application/json' \
        -d '{\"roomId\":\"$3\",\"senderId\":\"$ABSENDER\",\"senderName\":\"$ABSENDER\",
             \"content\":\"$1 $ABSENDER Nachricht '\$i'\"}';
    done"
}

# Alle Queues mit Tiefe (wartend + unbestätigt) und Zahl der Consumer
queues() {
  docker compose exec -T rabbitmq rabbitmqctl -q list_queues name messages consumers
}

# Nur die Zustell-Queues
delivery_queues() {
  queues | grep '^chat\.delivery\.'
}

# Bindungen am Exchange chat.delivery
delivery_bindings() {
  docker compose exec -T rabbitmq rabbitmqctl -q list_bindings source_name destination_name \
    | awk '$1 == "chat.delivery"'
}

# Zeilen in der Tabelle message zählen (wie in spec-batch-writer.md §5)
psql_count() {
  docker compose exec -T postgres \
    psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t -A -c "select count(*) from message;"
}

# Reihenfolge eines Absenders: in_order <Datei> <Lauf> <Absender>
# Zieht nur die Nummern dieses Absenders aus dem Stream und vergleicht sie mit ihrer
# Sortierung ohne Doppelte. Nachrichten anderer Absender dürfen dazwischen liegen (3.2.1).
in_order() {
  grep -o "$2 $3 Nachricht [0-9]*" "$1" | awk '{print $4}' > reihenfolge.txt
  local ANZAHL=$(wc -l < reihenfolge.txt)
  if sort -n -u reihenfolge.txt | diff -q - reihenfolge.txt > /dev/null; then
    echo "$3: $ANZAHL Nachrichten, Reihenfolge ok"
  else
    echo "$3: $ANZAHL Nachrichten, Reihenfolge FALSCH"
  fi
}
```

`grep '^data:'` sucht **ohne** Leerzeichen nach dem Doppelpunkt, so wie `SseEmitter` schreibt
(2.2).

### S1 — `mvn clean test` läuft in einem Lauf grün

```bash
mvn -q clean test
echo "Exit-Code: $?"
```

**Erfüllt, wenn:** Exit-Code `0`, und der Lauf enthält Tests des `chat-service` mit echtem
RabbitMQ (Testcontainers, wie die bestehenden Tests), die mindestens abdecken:

- Eine gesendete Nachricht erscheint im Stream ihres Raums, mit `id:` und dem unveränderten
  Body in `data:` (2.2, 3.1).
- Ein Stream eines anderen Raums bekommt sie nicht (3.2.1).
- Zwei Instanzen in derselben JVM legen zwei verschiedene Zustell-Queues an, und beide
  bekommen jede Nachricht (Abschnitt 4).
- `Last-Event-ID` liefert genau die Nachrichten nach X, keine doppelt, in der richtigen
  Reihenfolge (3.2.5).
- Eine fehlende oder ungültige `roomId` ergibt `400` (2.2).

### S2 — frischer Klon startet, kein Dienst veröffentlicht einen Port

```bash
docker compose up -d --build
docker compose ps --format '{{.Service}}\t{{.Status}}\t{{.Ports}}'
docker compose ps --format '{{.Ports}}' | grep -c -- '->'                  # erwartet: 0
grep -nE '^[[:space:]]+ports:' docker-compose.yml ; echo "Exit-Code: $?"   # erwartet: 1
```

**Erfüllt, wenn:** alle Dienste `running` bzw. `healthy` sind, keine Zeile der Spalte `Ports`
eine Zuordnung `...->...` enthält (Zählung `0`) und `docker-compose.yml` keinen
`ports:`-Schlüssel hat (Exit-Code `1`).

### S3 — zwei Instanzen, Raum A bekommt alle 400, Raum B keine

```bash
docker compose up -d --scale chat-service=2
sleep 20
delivery_queues            # erwartet: 2 Zeilen, consumers je 1
delivery_bindings          # erwartet: 2 Zeilen

I1=$(instances | sed -n 1p)
I2=$(instances | sed -n 2p)

# An jeder Instanz ein Client für Raum A und einer für Raum B
listen "$I1" "$ROOM_A" 60 > s3-a-i1.txt &
listen "$I2" "$ROOM_A" 60 > s3-a-i2.txt &
listen "$I1" "$ROOM_B" 60 > s3-b-i1.txt &
listen "$I2" "$ROOM_B" 60 > s3-b-i2.txt &
sleep 5

# Über jede Instanz 200 Nachrichten an Raum A, gleichzeitig, von zwei Absendern
send S3 200 "$ROOM_A" "$I1" anna &
send S3 200 "$ROOM_A" "$I2" ben &
wait

for DATEI in s3-a-i1.txt s3-a-i2.txt; do
  echo "== $DATEI"
  grep -c '^data:' "$DATEI"                    # erwartet: 400
  grep '^id:' "$DATEI" | sort -u | wc -l       # erwartet: 400
  in_order "$DATEI" S3 anna                    # erwartet: anna: 200 Nachrichten, Reihenfolge ok
  in_order "$DATEI" S3 ben                     # erwartet: ben: 200 Nachrichten, Reihenfolge ok
done
cat s3-b-i1.txt s3-b-i2.txt | grep -c '^data:' # erwartet: 0
```

**Erfüllt, wenn:** zwei Zustell-Queues mit je einem Consumer an `chat.delivery` gebunden sind,
**beide** A-Clients je genau `400` Ereignisse mit `400` verschiedenen IDs bekommen, darin von
jedem Absender genau `200` in seiner Reihenfolge, und die B-Clients zusammen `0`.

Jeder A-Client bekommt 200 Nachrichten, die über seine eigene Instanz gesendet wurden, und 200
über die andere. Das zeigt den Weg von Instanz zu Instanz über den Broker (3.1). Die Reihenfolge
wird pro Absender geprüft, weil die beiden Schleifen gleichzeitig laufen und sich ihre
Nachrichten beliebig mischen dürfen (3.2.1).

### S4 — nach dem Herunterskalieren bleibt nach 30 s eine Zustell-Queue

```bash
docker compose up -d --scale chat-service=1
sleep 30
delivery_queues                    # erwartet: genau 1 Zeile, consumers 1
delivery_bindings | wc -l          # erwartet: 1
docker inspect -f '{{.Config.Hostname}}' "$(instances)"   # steckt im Namen der übrigen Queue
queues | grep -E '^chat\.(persist|dlq)[[:space:]]'        # erwartet: 2 Zeilen, chat.persist und chat.dlq
```

**Erfüllt, wenn:** nach 30 s genau eine Zustell-Queue mit einem Consumer existiert, genau eine
Bindung an `chat.delivery` besteht, der Name der Queue den Hostnamen der laufenden Instanz
enthält und `chat.persist` und `chat.dlq` weiterhin bestehen. Das Aufräumen betrifft nur die
Zustell-Queue der gestoppten Instanz. Die beiden dauerhaften Queues des Schreibwegs sind
`durable` und nicht `auto-delete` und hängen an keiner Instanz (Abschnitt 4).

### S5 — Zustellung ohne Datenbank in 5 s, danach alles in der Tabelle

```bash
I1=$(instances | sed -n 1p)
VORHER=$(psql_count)                          # zählen, solange Postgres noch läuft
docker compose stop batch-writer postgres

listen "$I1" "$ROOM_A" 40 > s5.txt &
sleep 5
send S5 100 "$ROOM_A"
sleep 5                                       # 5 s nach dem letzten POST
grep -c '^data:' s5.txt                       # erwartet: 100
wait

docker compose start postgres batch-writer
sleep 60
NACHHER=$(psql_count)
echo "Neue Zeilen: $((NACHHER - VORHER))"     # erwartet: 100
```

**Erfüllt, wenn:** 5 s nach dem letzten `POST` alle `100` Nachrichten im Stream stehen, während
Postgres und `batch-writer` gestoppt sind, und nach dem Neustart genau `100` neue Zeilen in der
Tabelle stehen.

### S6 — hängende Instanz: ihre Queue nie über 1000

```bash
docker compose up -d --scale chat-service=2
sleep 20
I1=$(instances | sed -n 1p)
I2=$(instances | sed -n 2p)
H2=$(docker inspect -f '{{.Config.Hostname}}' "$I2")     # Hostname steckt im Queue-Namen

listen "$I1" "$ROOM_A" 300 > s6-gesund.txt &
sleep 5
docker pause "$I2"                       # NICHT docker compose pause — das friert beide ein

send S6 5000 "$ROOM_A" "$I1" &
SEND_PID=$!
MAX=0
while kill -0 "$SEND_PID" 2>/dev/null; do
  TIEFE=$(delivery_queues | awk -v h="$H2" 'index($1, h) {print $2}')
  [ "${TIEFE:-0}" -gt "$MAX" ] && MAX=$TIEFE
  sleep 2
done
echo "Höchste Tiefe der hängenden Queue: $MAX"   # erwartet: höchstens 1000 (nach 3.2.4: 950)

sleep 10
grep -c '^data:' s6-gesund.txt                   # erwartet: 5000
in_order s6-gesund.txt S6 anna                   # erwartet: anna: 5000 Nachrichten, Reihenfolge ok

docker unpause "$I2"
sleep 20
delivery_queues                                  # erwartet: 2 Zeilen, consumers je 1
```

**Erfüllt, wenn:** die Zustell-Queue der pausierten Instanz in keiner Messung über `1000` steht,
der Client an der gesunden Instanz alle `5000` Nachrichten in der richtigen Reihenfolge bekommt
und nach dem Fortsetzen wieder zwei Zustell-Queues mit je einem Consumer existieren.

Der Stream `s6-gesund.txt` läuft im Hintergrund bis zum Ende seiner 300 s weiter. Das stört die
folgenden Schritte nicht; S7 wartet deshalb gezielt nur auf den eigenen Stream.

Verschwindet die Queue der pausierten Instanz während der Messung, ist das kein Fehler: Nach
rund 60 s ohne Heartbeat schliesst der Broker die Verbindung, und `auto-delete` löscht die Queue
(3.2.4). `TIEFE` ist dann leer und zählt als `0`. Die letzte Messung zeigt, dass die Instanz die
Queue nach dem Fortsetzen neu angelegt hat.

### S7 — Neuverbindung mit `Last-Event-ID` holt genau die 50 fehlenden

```bash
I1=$(instances | sed -n 1p)
I2=$(instances | sed -n 2p)

# 1. Der Client hört an Instanz 1 zu, bekommt eine Nachricht und trennt.
listen "$I1" "$ROOM_A" 8 > s7-vorher.txt &
LISTEN_PID=$!              # nur auf diesen Stream warten, nicht auf den aus S6
sleep 4
send S7a 1 "$ROOM_A"
wait "$LISTEN_PID"         # --max-time ist abgelaufen, die Verbindung ist getrennt
grep -c '^data:' s7-vorher.txt                             # erwartet: 1
LETZTE_ID=$(grep '^id:' s7-vorher.txt | tail -1 | cut -c4-)
echo "Last-Event-ID: $LETZTE_ID"

# 2. Die 10 s bis zur Neuverbindung laufen ab dem Trennen.
#    In diese Zeit fallen die 50 Nachrichten; das Senden dauert wenige Sekunden.
sleep 10 &
PAUSE_PID=$!
send S7b 50 "$ROOM_A"
wait "$PAUSE_PID"

# 3. Neu verbinden mit Last-Event-ID: an derselben und gleichzeitig an der anderen Instanz.
#    Nach dem Nachholen kommt eine neue Nachricht, die in beiden Streams live ankommen muss.
listen "$I1" "$ROOM_A" 15 "$LETZTE_ID" > s7-gleiche.txt &
P1=$!
listen "$I2" "$ROOM_A" 15 "$LETZTE_ID" > s7-andere.txt &
P2=$!
sleep 5
send S7c 1 "$ROOM_A"
wait "$P1" "$P2"

for DATEI in s7-gleiche.txt s7-andere.txt; do
  echo "== $DATEI"
  grep -c '^data:' "$DATEI"                                # erwartet: 51
  grep '^id:' "$DATEI" | sort -u | wc -l                   # erwartet: 51
  grep -c 'S7a' "$DATEI"                                   # erwartet: 0
  in_order "$DATEI" S7b anna                               # erwartet: anna: 50 Nachrichten, Reihenfolge ok
  grep '^data:' "$DATEI" | tail -1 | grep -c 'S7c'         # erwartet: 1
done
```

**Erfüllt, wenn:**

- der Client vor dem Trennen genau `1` Nachricht bekommen hat,
- nach der Neuverbindung an **beiden** Instanzen genau `51` Ereignisse mit `51` verschiedenen
  IDs ankommen: die `50` verpassten in der gesendeten Reihenfolge und danach die eine neue,
- keines davon die Nachricht ist, die der Client schon hatte (`S7a`), und
- das letzte Ereignis die neue Nachricht `S7c` ist. Das zeigt, dass der Stream nach dem
  Nachholen live weiterläuft.

Die Neuverbindung an der anderen Instanz zeigt, dass der Puffer dank Fanout in jeder Instanz
vollständig ist (3.2.5). Die Prüfung des Lehrers verbindet sich mit jeder Instanz einzeln.
Die Neuverbindung muss deshalb an beiden Instanzen genau die 50 fehlenden liefern.

### S8 — Quelltext hält sich an CLAUDE.md, `.env` nicht im Repo

```bash
# 1. Keine Streams und keine forEach-Lambdas — erwartet: keine Ausgabe
grep -rnE '\.stream\(|\bStream\.|Collectors|\.forEach\(' chat-service/src

# 2. Ein Kommentar direkt über jeder Klasse und jeder Methode — erwartet: keine Ausgabe
awk '
  /^[[:space:]]*@/ { next }
  {
    isType   = ($0 ~ /^(public |final |abstract )*(class|record|interface|enum) /)
    isMethod = ($0 ~ /^    [A-Za-z][^=;]*[(]/ && $0 !~ /;[[:space:]]*$/) || ($0 ~ /^    public [A-Z][A-Za-z0-9]* [{]/)
    if ((isType || isMethod) && prev !~ /[*]\/[[:space:]]*$/) print FILENAME ":" FNR ": " $0
    prev = $0
  }' $(git ls-files 'chat-service/*.java')

# 3. .env ist nicht versioniert und wird ignoriert
git ls-files --error-unmatch .env ; echo "Exit-Code: $?"     # erwartet: Fehler, Exit-Code 1
git check-ignore -v .env                                      # erwartet: Treffer in .gitignore
grep -rn "password\|secret" chat-service/src/main/resources/  # erwartet: nur Platzhalter
```

**Erfüllt, wenn:**

- die ersten beiden Befehle nichts ausgeben,
- `git ls-files --error-unmatch .env` fehlschlägt und `git check-ignore` die Datei in
  [`.gitignore`](../.gitignore) nachweist,
- die `grep`-Suche nur Platzhalter der Form `${RABBITMQ_PASSWORD:guest}` findet. `guest` ist der
  bekannte Standardwert von RabbitMQ und gilt nur beim Start ausserhalb von Docker.

Was kein Befehl prüfen kann, prüft das Durchlesen nach [`../CLAUDE.md`](../CLAUDE.md):
Bezeichner und Log-Meldungen auf Englisch, Kommentare auf Deutsch, ein Ergebnis pro Zeile,
sprechende Namen, keine Interfaces mit nur einer Implementierung, `@Slf4j` und
`@RequiredArgsConstructor`, `record` für Datenklassen.

---

## 6. Anhang: Belege

Mehrere Aussagen dieser Spezifikation stützen sich auf Standardwerte von Bibliotheken. Jede
lässt sich mit einem Befehl nachprüfen. Die Jars liegen nach `mvn clean test` (S1) im lokalen
Maven-Repository. `javap` gehört zum JDK.

### A. Bibliotheken

```bash
M2=~/.m2/repository
```

**A0 — Welche Versionen Spring Boot 3.5.16 festlegt.** Alle Befehle unten beziehen sich auf
genau diese Versionen.

```bash
grep -E '<(spring-framework|spring-amqp|rabbit-amqp-client|tomcat)\.version>' \
  "$M2/org/springframework/boot/spring-boot-dependencies/3.5.16/spring-boot-dependencies-3.5.16.pom"
```

Erwartet: `spring-framework` 6.2.19, `spring-amqp` 3.2.12, `rabbit-amqp-client` 5.25.0,
`tomcat` 10.1.55.

**A1 — `SseEmitter` schreibt `id:` und `data:` ohne Leerzeichen (2.2).**

```bash
javap -c -p -cp "$M2/org/springframework/spring-webmvc/6.2.19/spring-webmvc-6.2.19.jar" \
  'org.springframework.web.servlet.mvc.method.annotation.SseEmitter$SseEventBuilderImpl' \
  | grep -E '// String .*(id|data):'
```

Erwartet: drei Zeilen mit `String id:`, `String data:` und `String \ndata:`.

**A2 — Tomcat bricht asynchrone Anfragen nach 30 s ab (2.2).**

```bash
javap -c -p -cp "$M2/org/apache/tomcat/embed/tomcat-embed-core/10.1.55/tomcat-embed-core-10.1.55.jar" \
  org.apache.catalina.connector.Connector | grep -B1 'Field asyncTimeout:J' | head -2
```

Erwartet: `long 30000l`, direkt danach `putfield ... asyncTimeout`.

**A3 — Prefetch 250 und Wiederverbinden alle 5 s bei Spring AMQP (3.2.4).**

```bash
javap -constants -cp "$M2/org/springframework/amqp/spring-rabbit/3.2.12/spring-rabbit-3.2.12.jar" \
  org.springframework.amqp.rabbit.listener.AbstractMessageListenerContainer \
  | grep -E 'DEFAULT_PREFETCH_COUNT|DEFAULT_RECOVERY_INTERVAL'
```

Erwartet: `DEFAULT_PREFETCH_COUNT = 250` und `DEFAULT_RECOVERY_INTERVAL = 5000l`.

**A4 — Der Java-Client von RabbitMQ fordert einen Heartbeat von 60 s an (3.2.4).**

```bash
javap -constants -cp "$M2/com/rabbitmq/amqp-client/5.25.0/amqp-client-5.25.0.jar" \
  com.rabbitmq.client.ConnectionFactory | grep 'DEFAULT_HEARTBEAT'
```

Erwartet: `DEFAULT_HEARTBEAT = 60`.

**A5 — Spring Boot fährt standardmässig `graceful` herunter (3.2.2).**

```bash
unzip -p "$M2/org/springframework/boot/spring-boot-autoconfigure/3.5.16/spring-boot-autoconfigure-3.5.16.jar" \
  META-INF/spring-configuration-metadata.json | grep -A4 '"name": "server.shutdown"' | grep defaultValue
```

Erwartet: `"defaultValue": "graceful"`.

### B. RabbitMQ-Dokumentation

**B1 — Queue-Längengrenze** (<https://www.rabbitmq.com/docs/maxlength>), zitiert in 3.2.4:

- *„Messages unacknowledged by consumers do not count toward the limit."*
- `drop-head` verwirft die ältesten Nachrichten, `reject-publish` die neuesten.
- Bei `reject-publish` und mehreren Queues: *„The message will still be published to all other
  queues which can enqueue it."*

**B2 — Ablauf einer Queue mit `x-expires`** (<https://www.rabbitmq.com/docs/ttl>), zitiert in
3.2.2: Eine Queue gilt als unbenutzt, wenn sie keine Consumer hat, nicht kürzlich neu angelegt
wurde und kein `basic.get` lief. Der Broker sagt zu, dass er sie nach Ablauf löscht, aber nicht,
wie schnell danach.
