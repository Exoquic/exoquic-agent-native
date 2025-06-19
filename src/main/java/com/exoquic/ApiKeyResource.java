package com.exoquic;

import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

@Path("/api/apikey")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ApiKeyResource {
    
    private static final Logger logger = LoggerFactory.getLogger(ApiKeyResource.class);
    
    @Inject
    ApiKeyService apiKeyService;
    
    @ConfigProperty(name = "websocket.admin.key", defaultValue = "admin-change-me")
    String adminKey;
    
    @POST
    @Path("/generate")
    public Response generateApiKey(@HeaderParam("Admin-Key") String providedAdminKey, ApiKeyRequest request) {
        if (!adminKey.equals(providedAdminKey)) {
            logger.warn("Unauthorized API key generation attempt");
            return Response.status(Response.Status.UNAUTHORIZED)
                .entity(Map.of("error", "Invalid admin key"))
                .build();
        }
        
        if (request.clientId == null || request.clientId.trim().isEmpty()) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity(Map.of("error", "clientId is required"))
                .build();
        }
        
        try {
            String apiKey = apiKeyService.generateAndStoreApiKey(request.clientId);
            logger.info("Generated API key for client: {}", request.clientId);
            
            return Response.ok(Map.of(
                "clientId", request.clientId,
                "apiKey", apiKey,
                "message", "API key generated successfully"
            )).build();
            
        } catch (Exception e) {
            logger.error("Failed to generate API key for client {}: {}", request.clientId, e.getMessage(), e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                .entity(Map.of("error", "Failed to generate API key"))
                .build();
        }
    }
    
    @DELETE
    @Path("/revoke/{clientId}")
    public Response revokeApiKey(@HeaderParam("Admin-Key") String providedAdminKey, @PathParam("clientId") String clientId) {
        if (!adminKey.equals(providedAdminKey)) {
            logger.warn("Unauthorized API key revocation attempt for client: {}", clientId);
            return Response.status(Response.Status.UNAUTHORIZED)
                .entity(Map.of("error", "Invalid admin key"))
                .build();
        }
        
        try {
            apiKeyService.revokeApiKey(clientId);
            logger.info("Revoked API key for client: {}", clientId);
            
            return Response.ok(Map.of(
                "clientId", clientId,
                "message", "API key revoked successfully"
            )).build();
            
        } catch (Exception e) {
            logger.error("Failed to revoke API key for client {}: {}", clientId, e.getMessage(), e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                .entity(Map.of("error", "Failed to revoke API key"))
                .build();
        }
    }
    
    public static class ApiKeyRequest {
        public String clientId;
    }
}