package ch.benedict.m321.chatservice.service;

import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Prüft, dass es zu jedem Raum genau ein RoomStream-Objekt gibt. Gäbe es zwei,
 * hätte derselbe Raum zwei Puffer und zwei Schlösser, und Listener und
 * Controller würden aneinander vorbeiarbeiten (Spec 3.1).
 */
class RoomStreamRegistryTest {

    /** Dieselbe roomId ergibt dasselbe Objekt, eine andere ein anderes. */
    @Test
    void returnsSameRoomStreamForSameRoom() {
        RoomStreamRegistry registry = new RoomStreamRegistry();
        UUID roomA = UUID.randomUUID();
        UUID roomB = UUID.randomUUID();

        RoomStream firstCall = registry.getOrCreate(roomA);
        RoomStream secondCall = registry.getOrCreate(roomA);
        RoomStream otherRoom = registry.getOrCreate(roomB);

        assertSame(firstCall, secondCall);
        assertNotSame(firstCall, otherRoom);
    }

    /**
     * Fragen zwei Threads gleichzeitig nach einem neuen Raum, dürfen nicht zwei
     * RoomStream-Objekte entstehen. Das passiert real, wenn ein Client sich
     * anmeldet, während der Listener die erste Nachricht des Raums zustellt.
     */
    @Test
    void createsOnlyOneRoomStreamWhenTwoThreadsAskAtOnce() throws InterruptedException {
        for (int attempt = 0; attempt < 200; attempt++) {
            RoomStreamRegistry registry = new RoomStreamRegistry();
            UUID roomId = UUID.randomUUID();
            CountDownLatch startSignal = new CountDownLatch(1);
            RoomStream[] results = new RoomStream[2];

            Thread first = new Thread(() -> results[0] = askAfterSignal(registry, roomId, startSignal));
            Thread second = new Thread(() -> results[1] = askAfterSignal(registry, roomId, startSignal));
            first.start();
            second.start();
            startSignal.countDown();
            first.join();
            second.join();

            assertSame(results[0], results[1]);
        }
    }

    /** Wartet auf das Startsignal, damit beide Threads möglichst gleichzeitig fragen. */
    private RoomStream askAfterSignal(RoomStreamRegistry registry, UUID roomId, CountDownLatch startSignal) {
        try {
            startSignal.await();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            return null;
        }
        return registry.getOrCreate(roomId);
    }
}
