package com.exoquic;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.websocket.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;

@ApplicationScoped
public class WebSocketEventBroadcaster {
    
    private static final Logger logger = LoggerFactory.getLogger(WebSocketEventBroadcaster.class);
    
    private final Map<String, Set<Session>> tableListeners = new ConcurrentHashMap<>();
    
    public void addSession(Session session, String tableName) {
        tableListeners.computeIfAbsent(tableName, k -> new CopyOnWriteArraySet<>()).add(session);
        logger.info("Added WebSocket session {} for table {}", session.getId(), tableName);
    }
    
    public void removeSession(Session session) {
        tableListeners.values().forEach(sessions -> sessions.remove(session));
        logger.info("Removed WebSocket session {}", session.getId());
    }
    
    public void broadcastTableEvent(String tableName, String eventJson) {
        Set<Session> sessions = tableListeners.get(tableName);
        if (sessions == null || sessions.isEmpty()) {
            logger.debug("No WebSocket listeners for table: {}", tableName);
            return;
        }
        
        logger.info("Broadcasting event to {} WebSocket clients for table: {}", sessions.size(), tableName);
        
        Set<Session> sessionsCopy = new CopyOnWriteArraySet<>(sessions);
        
        for (Session session : sessionsCopy) {
            try {
                if (session.isOpen()) {
                    session.getBasicRemote().sendText(eventJson);
                    logger.debug("Sent event to WebSocket session: {}", session.getId());
                } else {
                    sessions.remove(session);
                    logger.debug("Removed closed WebSocket session: {}", session.getId());
                }
            } catch (IOException e) {
                logger.error("Failed to send event to WebSocket session {}: {}", session.getId(), e.getMessage());
                sessions.remove(session);
            }
        }
        
        if (sessions.isEmpty()) {
            tableListeners.remove(tableName);
            logger.debug("Removed empty listener set for table: {}", tableName);
        }
    }
    
    public int getListenerCount(String tableName) {
        Set<Session> sessions = tableListeners.get(tableName);
        return sessions != null ? sessions.size() : 0;
    }
    
    public int getTotalListenerCount() {
        return tableListeners.values().stream()
            .mapToInt(Set::size)
            .sum();
    }
}