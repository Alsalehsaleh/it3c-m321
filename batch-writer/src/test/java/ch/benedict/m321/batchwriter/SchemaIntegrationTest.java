package ch.benedict.m321.batchwriter;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Prüft, dass das Init-Skript die Tabelle message so anlegt, wie PLANUNG.md §3.7 und
 * Spec 4.1 es verlangen.
 *
 * Der Test-Postgres führt dieselbe Datei aus wie der Postgres im Stack
 * (postgres/init/01-schema.sql). Was hier grün ist, gilt deshalb auch dort.
 */
class SchemaIntegrationTest extends IntegrationTestBase {

    /** Die sechs Spalten mit Typ, in der Reihenfolge aus PLANUNG.md §3.7, alle NOT NULL. */
    @Test
    void messageTableHasTheColumnsFromThePlanning() {
        List<Map<String, Object>> columns = jdbcTemplate.queryForList("""
                select column_name, data_type, is_nullable
                from information_schema.columns
                where table_name = 'message'
                order by ordinal_position
                """);

        List<String> actualColumns = new ArrayList<>();
        for (Map<String, Object> column : columns) {
            String description = column.get("column_name") + " " + column.get("data_type")
                    + " " + column.get("is_nullable");
            actualColumns.add(description);
        }

        List<String> expectedColumns = List.of(
                "id uuid NO",
                "room_id uuid NO",
                "sender_id character varying NO",
                "sender_name character varying NO",
                "content text NO",
                "sent_at timestamp with time zone NO");
        assertEquals(expectedColumns, actualColumns);
    }

    /** Der Primärschlüssel auf id trägt ON CONFLICT, der Index die Leseabfrage pro Raum. */
    @Test
    void messageTableHasPrimaryKeyAndIndex() {
        List<String> indexDefinitions = jdbcTemplate.queryForList(
                "select indexdef from pg_indexes where tablename = 'message' order by indexname",
                String.class);

        assertEquals(2, indexDefinitions.size());

        String roomIndex = indexDefinitions.get(0);
        assertTrue(roomIndex.contains("idx_message_room_sent"), roomIndex);
        assertTrue(roomIndex.contains("(room_id, sent_at DESC)"), roomIndex);

        String primaryKey = indexDefinitions.get(1);
        assertTrue(primaryKey.contains("message_pkey"), primaryKey);
        assertTrue(primaryKey.contains("(id)"), primaryKey);
    }
}
