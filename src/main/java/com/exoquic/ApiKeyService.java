package com.exoquic;

import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

@ApplicationScoped
public class ApiKeyService {
    
    private static final Logger logger = LoggerFactory.getLogger(ApiKeyService.class);
    private static final String HASH_ALGORITHM = "SHA-256";
    private static final int SALT_LENGTH = 32;
    
    @ConfigProperty(name = "websocket.apikey.storage.path", defaultValue = "./api-keys.dat")
    String apiKeyStoragePath;
    
    private final Map<String, String> apiKeyHashes = new HashMap<>();
    private final SecureRandom secureRandom = new SecureRandom();
    
    public void initialize() {
        loadApiKeys();
    }
    
    public String generateAndStoreApiKey(String clientId) {
        try {
            byte[] keyBytes = new byte[32];
            secureRandom.nextBytes(keyBytes);
            String apiKey = Base64.getUrlEncoder().withoutPadding().encodeToString(keyBytes);
            
            byte[] salt = new byte[SALT_LENGTH];
            secureRandom.nextBytes(salt);
            
            String hashedKey = hashApiKey(apiKey, salt);
            String saltB64 = Base64.getEncoder().encodeToString(salt);
            String storedValue = saltB64 + ":" + hashedKey;
            
            apiKeyHashes.put(clientId, storedValue);
            saveApiKeys();
            
            logger.info("Generated and stored API key for client: {}", clientId);
            return apiKey;
            
        } catch (Exception e) {
            logger.error("Failed to generate API key for client {}: {}", clientId, e.getMessage(), e);
            throw new RuntimeException("Failed to generate API key", e);
        }
    }
    
    public boolean validateApiKey(String clientId, String providedApiKey) {
        String storedValue = apiKeyHashes.get(clientId);
        if (storedValue == null) {
            return false;
        }
        
        try {
            String[] parts = storedValue.split(":");
            if (parts.length != 2) {
                logger.warn("Invalid stored API key format for client: {}", clientId);
                return false;
            }
            
            byte[] salt = Base64.getDecoder().decode(parts[0]);
            String storedHash = parts[1];
            String providedHash = hashApiKey(providedApiKey, salt);
            
            return storedHash.equals(providedHash);
            
        } catch (Exception e) {
            logger.error("Error validating API key for client {}: {}", clientId, e.getMessage(), e);
            return false;
        }
    }
    
    public void revokeApiKey(String clientId) {
        apiKeyHashes.remove(clientId);
        saveApiKeys();
        logger.info("Revoked API key for client: {}", clientId);
    }
    
    private String hashApiKey(String apiKey, byte[] salt) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance(HASH_ALGORITHM);
        digest.update(salt);
        digest.update(apiKey.getBytes());
        byte[] hash = digest.digest();
        return Base64.getEncoder().encodeToString(hash);
    }
    
    private void loadApiKeys() {
        Path path = Paths.get(apiKeyStoragePath);
        if (!Files.exists(path)) {
            logger.info("API key storage file does not exist, starting with empty key store");
            return;
        }
        
        try {
            Files.lines(path).forEach(line -> {
                String[] parts = line.split("=", 2);
                if (parts.length == 2) {
                    apiKeyHashes.put(parts[0], parts[1]);
                }
            });
            logger.info("Loaded {} API keys from storage", apiKeyHashes.size());
        } catch (IOException e) {
            logger.error("Failed to load API keys from storage: {}", e.getMessage(), e);
        }
    }
    
    private void saveApiKeys() {
        Path path = Paths.get(apiKeyStoragePath);
        try {
            if (path.getParent() != null) {
                Files.createDirectories(path.getParent());
            }
            
            StringBuilder content = new StringBuilder();
            apiKeyHashes.forEach((clientId, hashedKey) -> 
                content.append(clientId).append("=").append(hashedKey).append("\n"));
            
            Files.write(path, content.toString().getBytes(), 
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            
            logger.debug("Saved {} API keys to storage", apiKeyHashes.size());
        } catch (IOException e) {
            logger.error("Failed to save API keys to storage: {}", e.getMessage(), e);
        }
    }
}