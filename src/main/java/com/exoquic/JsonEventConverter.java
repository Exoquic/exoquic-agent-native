package com.exoquic;

import com.exoquic.pgoutput.config.Operation;
import com.exoquic.pgoutput.core.ReplicationMessage;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.json.bind.Jsonb;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class JsonEventConverter {
    
    @Inject
    Jsonb jsonb;
    
    public String convertToJson(ReplicationMessage message) {
        Map<String, Object> eventMap = new HashMap<>();
        
        String operationType = mapOperationType(message.getOperation());
        eventMap.put("type", operationType);
        
        Map<String, Object> dataMap = new HashMap<>();
        
        List<ReplicationMessage.Column> columns = getColumnsForOperation(message);
        if (columns != null) {
            for (ReplicationMessage.Column column : columns) {
                if (column.isToastedColumn()) {
                    continue; // Skip unchanged TOAST columns
                }
                
                String columnName = column.getName();
                Object value = column.getValue();
                
                if (column.isNull()) {
                    dataMap.put(columnName, null);
                } else {
                    dataMap.put(columnName, value);
                }
            }
        }
        
        eventMap.put("data", dataMap);
        
        return jsonb.toJson(eventMap);
    }
    
    private String mapOperationType(Operation operation) {
        switch (operation) {
            case INSERT:
                return "created";
            case UPDATE:
                return "updated";
            case DELETE:
                return "deleted";
            default:
                return operation.name().toLowerCase();
        }
    }
    
    private List<ReplicationMessage.Column> getColumnsForOperation(ReplicationMessage message) {
        switch (message.getOperation()) {
            case INSERT:
            case UPDATE:
                return message.getNewTupleList();
            case DELETE:
                return message.getOldTupleList();
            default:
                return null;
        }
    }
}