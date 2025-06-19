package com.exoquic;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.websocket.*;
import jakarta.websocket.server.PathParam;
import jakarta.websocket.server.ServerEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@ServerEndpoint("/ws/table-events/{tableName}")
@ApplicationScoped
public class TableEventsWebSocket {
    
    private static final Logger logger = LoggerFactory.getLogger(TableEventsWebSocket.class);
    
    @Inject
    ApiKeyService apiKeyService;
    
    @Inject
    WebSocketEventBroadcaster eventBroadcaster;
    
    private final Map<Session, String> sessionToTable = new ConcurrentHashMap<>();
    private final Map<Session, String> authenticatedSessions = new ConcurrentHashMap<>();
    
    @OnOpen
    public void onOpen(Session session, @PathParam("tableName") String tableName) {
        logger.info("WebSocket connection opened for table: {} from {}", tableName, session.getId());
        sessionToTable.put(session, tableName);
        
        try {
            session.getBasicRemote().sendText("{\"type\":\"auth_required\",\"message\":\"Please provide API key\"}");
        } catch (IOException e) {
            logger.error("Failed to send auth challenge to session {}: {}", session.getId(), e.getMessage());
        }
    }
    
    @OnMessage
    public void onMessage(String message, Session session) {
        try {
            if (message.startsWith("{") && message.contains("\"apiKey\"")) {
                handleAuthentication(message, session);
            } else {
                handleRawApiKeyAuth(message, session);
            }
        } catch (Exception e) {
            logger.error("Error processing message from session {}: {}", session.getId(), e.getMessage());
            try {
                session.getBasicRemote().sendText("{\"type\":\"error\",\"message\":\"Invalid message format\"}");
            } catch (IOException ioEx) {
                logger.error("Failed to send error message to session {}: {}", session.getId(), ioEx.getMessage());
            }
        }
    }
    
    private void handleAuthentication(String message, Session session) throws IOException {
        String apiKey = extractApiKeyFromJson(message);
        String clientId = extractClientIdFromJson(message);
        
        if (apiKey != null && clientId != null) {
            if (apiKeyService.validateApiKey(clientId, apiKey)) {
                authenticatedSessions.put(session, clientId);
                eventBroadcaster.addSession(session, sessionToTable.get(session));
                session.getBasicRemote().sendText("{\"type\":\"auth_success\",\"message\":\"Authentication successful\"}");
                logger.info("Client {} authenticated successfully for table {}", clientId, sessionToTable.get(session));
            } else {
                session.getBasicRemote().sendText("{\"type\":\"auth_failed\",\"message\":\"Invalid API key\"}");
                logger.warn("Authentication failed for client {} on session {}", clientId, session.getId());
            }
        } else {
            session.getBasicRemote().sendText("{\"type\":\"auth_failed\",\"message\":\"Missing apiKey or clientId in message\"}");
        }
    }
    
    private void handleRawApiKeyAuth(String apiKey, Session session) throws IOException {
        String clientId = session.getId();
        
        if (apiKeyService.validateApiKey(clientId, apiKey)) {
            authenticatedSessions.put(session, clientId);
            eventBroadcaster.addSession(session, sessionToTable.get(session));
            session.getBasicRemote().sendText("{\"type\":\"auth_success\",\"message\":\"Authentication successful\"}");
            logger.info("Client {} authenticated successfully for table {}", clientId, sessionToTable.get(session));
        } else {
            session.getBasicRemote().sendText("{\"type\":\"auth_failed\",\"message\":\"Invalid API key\"}");
            logger.warn("Authentication failed for client {} on session {}", clientId, session.getId());
        }
    }
    
    private String extractApiKeyFromJson(String json) {
        String apiKeyPattern = "\"apiKey\"\\s*:\\s*\"([^\"]+)\"";
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(apiKeyPattern);
        java.util.regex.Matcher matcher = pattern.matcher(json);
        return matcher.find() ? matcher.group(1) : null;
    }
    
    private String extractClientIdFromJson(String json) {
        String clientIdPattern = "\"clientId\"\\s*:\\s*\"([^\"]+)\"";
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(clientIdPattern);
        java.util.regex.Matcher matcher = pattern.matcher(json);
        return matcher.find() ? matcher.group(1) : null;
    }
    
    @OnClose
    public void onClose(Session session, CloseReason reason) {
        String tableName = sessionToTable.remove(session);
        String clientId = authenticatedSessions.remove(session);
        eventBroadcaster.removeSession(session);
        
        logger.info("WebSocket connection closed for table: {} client: {} reason: {}", 
            tableName, clientId, reason.getReasonPhrase());
    }
    
    @OnError
    public void onError(Session session, Throwable throwable) {
        String tableName = sessionToTable.get(session);
        String clientId = authenticatedSessions.get(session);
        
        logger.error("WebSocket error for table: {} client: {} error: {}", 
            tableName, clientId, throwable.getMessage(), throwable);
    }
}