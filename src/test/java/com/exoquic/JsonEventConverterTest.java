package com.exoquic;

import com.exoquic.pgoutput.config.Operation;
import com.exoquic.pgoutput.core.PostgresType;
import com.exoquic.pgoutput.core.ReplicationMessage;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import jakarta.json.bind.Jsonb;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
class JsonEventConverterTest {

    @Inject
    JsonEventConverter converter;
    
    @Inject
    Jsonb jsonb;

    @Test
    void testInsertOperationConversion() {
        ReplicationMessage message = createMockInsertMessage();
        String json = converter.convertToJson(message);
        
        // Parse JSON back to verify structure
        Map<String, Object> eventMap = jsonb.fromJson(json, Map.class);
        
        assertEquals("created", eventMap.get("type"));
        assertTrue(eventMap.containsKey("data"));
        
        Map<String, Object> data = (Map<String, Object>) eventMap.get("data");
        assertEquals(1, ((Number) data.get("id")).intValue());
        assertEquals("John Doe", data.get("name"));
        assertEquals("john@example.com", data.get("email"));
    }

    @Test
    void testUpdateOperationConversion() {
        ReplicationMessage message = createMockUpdateMessage();
        String json = converter.convertToJson(message);
        
        Map<String, Object> eventMap = jsonb.fromJson(json, Map.class);
        
        assertEquals("updated", eventMap.get("type"));
        assertTrue(eventMap.containsKey("data"));
        
        Map<String, Object> data = (Map<String, Object>) eventMap.get("data");
        assertEquals(1, ((Number) data.get("id")).intValue());
        assertEquals("Jane Doe", data.get("name"));
        assertEquals("jane@example.com", data.get("email"));
    }

    @Test
    void testDeleteOperationConversion() {
        ReplicationMessage message = createMockDeleteMessage();
        String json = converter.convertToJson(message);
        
        Map<String, Object> eventMap = jsonb.fromJson(json, Map.class);
        
        assertEquals("deleted", eventMap.get("type"));
        assertTrue(eventMap.containsKey("data"));
        
        Map<String, Object> data = (Map<String, Object>) eventMap.get("data");
        assertEquals(1, ((Number) data.get("id")).intValue());
        assertEquals("John Doe", data.get("name"));
        assertEquals("john@example.com", data.get("email"));
    }

    private ReplicationMessage createMockInsertMessage() {
        return new MockReplicationMessage(
            Operation.INSERT,
            "public.users",
            null, // no old tuple for INSERT
            Arrays.asList(
                new MockColumn("id", 1),
                new MockColumn("name", "John Doe"),
                new MockColumn("email", "john@example.com")
            )
        );
    }

    private ReplicationMessage createMockUpdateMessage() {
        return new MockReplicationMessage(
            Operation.UPDATE,
            "public.users",
            Arrays.asList(
                new MockColumn("id", 1),
                new MockColumn("name", "John Doe"),
                new MockColumn("email", "john@example.com")
            ),
            Arrays.asList(
                new MockColumn("id", 1),
                new MockColumn("name", "Jane Doe"),
                new MockColumn("email", "jane@example.com")
            )
        );
    }

    private ReplicationMessage createMockDeleteMessage() {
        return new MockReplicationMessage(
            Operation.DELETE,
            "public.users",
            Arrays.asList(
                new MockColumn("id", 1),
                new MockColumn("name", "John Doe"),
                new MockColumn("email", "john@example.com")
            ),
            null // no new tuple for DELETE
        );
    }

    private static class MockReplicationMessage implements ReplicationMessage {
        private final Operation operation;
        private final String table;
        private final List<Column> oldTuple;
        private final List<Column> newTuple;

        public MockReplicationMessage(Operation operation, String table, 
                                    List<Column> oldTuple, List<Column> newTuple) {
            this.operation = operation;
            this.table = table;
            this.oldTuple = oldTuple;
            this.newTuple = newTuple;
        }

        @Override
        public Operation getOperation() {
            return operation;
        }

        @Override
        public Instant getCommitTime() {
            return Instant.now();
        }

        @Override
        public OptionalLong getTransactionId() {
            return OptionalLong.of(12345L);
        }

        @Override
        public String getTable() {
            return table;
        }

        @Override
        public List<Column> getOldTupleList() {
            return oldTuple;
        }

        @Override
        public List<Column> getNewTupleList() {
            return newTuple;
        }

        @Override
        public boolean isLastEventForLsn() {
            return true;
        }
    }

    private static class MockColumn implements ReplicationMessage.Column {
        private final String name;
        private final Object value;

        public MockColumn(String name, Object value) {
            this.name = name;
            this.value = value;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public PostgresType getType() {
            return null; // Not needed for this test
        }

        @Override
        public String getTypeExpression() {
            return null; // Not needed for this test
        }

        @Override
        public boolean isOptional() {
            return false;
        }

        @Override
        public Object getValue() {
            return value;
        }

        @Override
        public boolean isNull() {
            return value == null;
        }

        @Override
        public boolean isToastedColumn() {
            return false;
        }
    }
}