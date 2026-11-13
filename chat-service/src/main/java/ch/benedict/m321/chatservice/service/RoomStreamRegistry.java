package ch.benedict.m321.chatservice.service;

import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Hält zu jedem Raum genau ein RoomStream-Objekt.
 *
 * Listener und StreamController holen sich den Raum hier. Nur so arbeiten
 * beide mit demselben Puffer und demselben Schloss. Ein Raum entsteht beim
 * ersten Bedarf, denn Räume verwaltet niemand (Spec 1.3).
 */
@Component
public class RoomStreamRegistry {

    private final Map<UUID, RoomStream> roomStreams = new HashMap<>();

    /**
     * Gibt den RoomStream des Raums zurück und legt ihn beim ersten Mal an.
     *
     * Die Methode ist synchronized: Fragen zwei Threads gleichzeitig nach
     * einem neuen Raum, sähen ohne Schloss beide "noch keiner da" und legten
     * je einen an. Ein Schloss um die ganze Methode ist die einfachste Lösung,
     * die man laut vorlesen kann. Sie hält kaum auf, weil die Methode nur in
     * der Map nachschaut; gesendet wird erst danach, ausserhalb dieses Schlosses.
     */
    public synchronized RoomStream getOrCreate(UUID roomId) {
        RoomStream roomStream = roomStreams.get(roomId);
        if (roomStream == null) {
            roomStream = new RoomStream();
            roomStreams.put(roomId, roomStream);
        }
        return roomStream;
    }
}
