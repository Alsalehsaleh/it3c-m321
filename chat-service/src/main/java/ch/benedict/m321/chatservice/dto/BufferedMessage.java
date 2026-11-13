package ch.benedict.m321.chatservice.dto;

import java.util.UUID;

/**
 * Eine Nachricht im Puffer eines Raums: ihre id und der Body genau so, wie er
 * vom Broker kam.
 *
 * Der Body bleibt Text und wird nie neu in JSON geschrieben. So sieht eine
 * nachgeholte Nachricht Zeichen für Zeichen gleich aus wie eine live
 * zugestellte und wie die Kopie in chat.persist (Spec 2.1).
 */
public record BufferedMessage(UUID id, String body) {
}
