package com.exoquic;

import com.exoquic.pgoutput.config.PgOutputConfig;
import com.exoquic.pgoutput.core.PgOutputMessageDecoder;
import com.exoquic.pgoutput.core.ReplicationMessage;
import com.exoquic.pgoutput.core.TypeRegistry;
import io.quarkus.runtime.Startup;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import org.postgresql.PGProperty;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import jakarta.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


@ApplicationScoped
@Startup
public class PostgresReplication {

    @ConfigProperty(name = "postgres.host")
    String dbHost;

    @ConfigProperty(name = "postgres.port", defaultValue = "5432")
    String dbPort;

    @ConfigProperty(name = "postgres.database")
    String dbName;

    @ConfigProperty(name = "postgres.user")
    String dbUser;

    @ConfigProperty(name = "postgres.password")
    String dbPassword;

    @ConfigProperty(name = "exoquic.endpoint")
    String endpoint;

    @ConfigProperty(name = "exoquic.api.key")
    String apiKey;

    @ConfigProperty(name = "replication.slot.name", defaultValue = "realtime_slot")
    String slotName;

    @ConfigProperty(name = "publication.name", defaultValue = "realtime_publication")
    String publicationName;

    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);
    private final Object shutdownLock = new Object();
    private final HttpClient httpClient = HttpClient.newHttpClient();
    private PgConnection replicationConnection;
    private final TransactionBuffer transactionBuffer;

    private final Logger logger = LoggerFactory.getLogger(PostgresReplication.class);

    @Inject
    JsonEventConverter jsonEventConverter;
    
    @Inject
    WebSocketEventBroadcaster webSocketEventBroadcaster;
    
    @Inject
    ApiKeyService apiKeyService;

    public PostgresReplication() {
        this.transactionBuffer = new TransactionBuffer(this::handleCommittedTransaction);
        
        // Add shutdown hook for graceful termination
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown hook triggered - initiating graceful shutdown");
            gracefulShutdown();
        }, "PostgresReplication-ShutdownHook"));
    }

    @PostConstruct
    public void start() {
        try {
            apiKeyService.initialize();
            executor.submit(this::startReplication);
        } catch (Exception e) {
            logger.error("Failed to start replication: {}", e.getMessage(), e);
        }
    }

    private void startReplication() {
        try {
            ValidationResult validationResult = setupReplication();
            if (!validationResult.isSuccess()) {
                logger.error("setupReplication failed with the following errors: {}", validationResult.errors);
            }
            consumeReplicationStream();
        } catch (Exception e) {
            logger.error("Replication failed: {}", e.getMessage(), e);
        }
    }

    private ValidationResult setupReplication() throws SQLException {
        ValidationResult result = new ValidationResult();
        Connection conn = null;
        int attempts = 0;
        boolean configurationComplete = false;

        while (!configurationComplete) {
            try {
                if (attempts > 0) {
                    logger.info("Waiting 3 seconds before retrying configuration (attempt {})", attempts + 1);
                    Thread.sleep(3000);
                }

                // Try to connect or reconnect
                if (conn == null || conn.isClosed() || !conn.isValid(1)) {
                    try {
                        if (conn != null && !conn.isClosed()) {
                            conn.close();
                        }
                    } catch (SQLException e) {
                        // Ignore errors while closing connection
                    }

                    conn = connectWithRetry();
                    if (conn == null) {
                        logger.warn("Failed to connect on attempt {}", attempts + 1);
                        attempts++;
                        continue;
                    }
                    logger.info("Successfully connected to PostgreSQL");
                }

                // Check user permissions first
                try {
                    validatePermissions(conn, result);
                    if (!result.isSuccess()) {
                        return result; // Exit early if permissions are insufficient
                    }
                } catch (SQLException e) {
                    logger.warn("Error checking permissions (attempt {}): {}", attempts + 1, e.getMessage());
                    attempts++;
                    continue;
                }

                // Configure WAL settings
                try {
                    validateWALSettings(conn, result);
                } catch (SQLException e) {
                    logger.warn("Error validating WAL settings (attempt {}): {}", attempts + 1, e.getMessage());
                    attempts++;
                    continue;
                }

                // Check/create publication
                try {
                    validatePublication(conn, result);
                } catch (SQLException e) {
                    logger.warn("Error validating publication (attempt {}): {}", attempts + 1, e.getMessage());
                    attempts++;
                    continue;
                }

                // Check/create replication slot
                try {
                    validateReplicationSlot(conn, result);
                } catch (SQLException e) {
                    logger.warn("Error validating replication slot (attempt {}): {}", attempts + 1, e.getMessage());
                    attempts++;
                    continue;
                }

                // Set REPLICA IDENTITY FULL for tables without primary keys
                try {
                    validateReplicaIdentity(conn, result);
                } catch (SQLException e) {
                    logger.warn("Error validating replica identity (attempt {}): {}", attempts + 1, e.getMessage());
                    attempts++;
                    continue;
                }

                // Generate connection info
                try {
                    generateConnectionInfo(conn, result);
                } catch (SQLException e) {
                    logger.warn("Error generating connection info (attempt {}): {}", attempts + 1, e.getMessage());
                    attempts++;
                    continue;
                }

                // Initialize replication components
                try {
                    initializeReplicationComponents(result);
                } catch (SQLException e) {
                    logger.warn("Error initializing replication components (attempt {}): {}", attempts + 1, e.getMessage());
                    attempts++;
                    continue;
                }

                configurationComplete = true;

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                result.addError("Configuration interrupted: " + e.getMessage());
                return result;
            } catch (Exception e) {
                logger.warn("Unexpected error during configuration (attempt {}): {}", attempts + 1, e.getMessage(), e);
                attempts++;
            } finally {
                if (configurationComplete && conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        logger.error("Error closing database connection", e);
                    }
                }
            }
        }

        return result;
    }

    private void consumeReplicationStream() throws SQLException {
        // Check connection health before starting
        if (replicationConnection == null || replicationConnection.isClosed()) {
            throw new SQLException("Replication connection is null or closed");
        }
        
        logger.info("Starting replication stream with connection: {}", replicationConnection);
        Properties props = new Properties();
        props.setProperty("user", dbUser);
        props.setProperty("password", dbPassword);
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "9.4");
        PGProperty.REPLICATION.set(props, "database");
        PGProperty.PREFER_QUERY_MODE.set(props, "simple");

        String url = String.format("jdbc:postgresql://%s:%s/%s", dbHost, dbPort, dbName);

        Connection metadataConnection;
        try {
            metadataConnection = DriverManager.getConnection(url, props);
            logger.info("Created metadata connection successfully");
        } catch (SQLException e) {
            logger.info("Failed to initialize replication components: " + e.getMessage());
            throw e;
        }

        ChainedLogicalStreamBuilder streamBuilder = replicationConnection
                .getReplicationAPI()
                .replicationStream()
                .logical()
                .withSlotName(slotName)
                .withSlotOption("publication_names", publicationName)
                .withSlotOption("proto_version", "1")
                .withStatusInterval(20, TimeUnit.SECONDS);

        PgOutputConfig pgOutputConfig = PgOutputConfig.builder()
                .databaseConnection(metadataConnection)
                .publicationName(publicationName)
                .includeTransactionMetadata(true)
                .build();
        TypeRegistry typeRegistry = TypeRegistry.create(metadataConnection);
        PgOutputMessageDecoder pgOutputMessageDecoder = new PgOutputMessageDecoder(pgOutputConfig, typeRegistry);

        try (PGReplicationStream stream = streamBuilder.start()) {
            running.set(true);
            logger.info("🚀 Replication stream started! Stream: {}", stream);

            while (running.get() && !shutdownRequested.get()) {
                // Read next WAL message (blocks until available)
                ByteBuffer msg = stream.readPending();

                if (msg == null) {
                    Thread.sleep(10);
                    continue;
                }

                // Check for shutdown before processing message
                if (shutdownRequested.get()) {
                    logger.info("Shutdown requested, stopping message processing");
                    break;
                }

                Optional<ReplicationMessage> replicationMessage = pgOutputMessageDecoder.decode(msg);
                replicationMessage.ifPresent(message -> handleReplicationMessage(message, stream.getLastReceiveLSN()));

                logger.info("Got message with {} bytes, LastReceiveLSN: {}",
                    msg.remaining(), stream.getLastReceiveLSN());

                // Acknowledge the message
                stream.setAppliedLSN(stream.getLastReceiveLSN());
                stream.setFlushedLSN(stream.getLastReceiveLSN());
                
                logger.info("Acknowledged message, LSN: {}", stream.getLastReceiveLSN());
            }
        } catch (Exception e) {
            logger.error("Error in replication stream: {}", e.getMessage(), e);
        }
    }

    private void handleReplicationMessage(ReplicationMessage message, LogSequenceNumber lsn) {
        long txId = message.getTransactionId().orElse(-1L);
        
        switch (message.getOperation()) {
            case BEGIN:
                logger.debug("Transaction BEGIN: {}", txId);
                transactionBuffer.handleBegin(txId, message.getCommitTime(), lsn);
                break;

            case COMMIT:
                logger.debug("Transaction COMMIT: {}", txId);
                transactionBuffer.handleCommit(txId, message.getCommitTime(), lsn);
                break;

            case INSERT:
            case UPDATE:
            case DELETE:
                logger.debug("Transaction event: {} {} on table {}", txId, message.getOperation(), message.getTable());
                transactionBuffer.handleEvent(txId, message, lsn);
                break;

            default:
                logger.debug("Unhandled operation: {}", message.getOperation());
                break;
        }
    }

    private void handleCommittedTransaction(Transaction transaction) {
        logger.info("Processing committed transaction: {} with {} events", 
            transaction.getTransactionId(), transaction.getEventCount());
        
        List<Transaction.TransactionEvent> orderedEvents = transaction.getOrderedEvents();
        
        for (Transaction.TransactionEvent event : orderedEvents) {
            ReplicationMessage message = event.getMessage();
            
            logger.debug("Processing event: {} on table {} (TX: {})", 
                message.getOperation(), message.getTable(), transaction.getTransactionId());

            try {
                String jsonPayload = jsonEventConverter.convertToJson(message);
                
                sendToPlatform(jsonPayload);
                broadcastToWebSocketClients(message.getTable(), jsonPayload);
                
                logger.info("Sent {} event for table {} to platform and {} WebSocket clients", 
                    message.getOperation(), message.getTable(), 
                    webSocketEventBroadcaster.getListenerCount(message.getTable()));
                    
            } catch (Exception e) {
                logger.error("Failed to process event for table {}: {}", 
                    message.getTable(), e.getMessage(), e);
            }
        }
    }


    
    private void sendToPlatform(String jsonPayload) {
        try {
            logger.info("Json payload: " + jsonPayload);
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(endpoint + "/api/event"))
                    .header("Content-Type", "application/json")
                    .header("Authorization", "Bearer " + apiKey)
                    .POST(HttpRequest.BodyPublishers.ofString(jsonPayload))
                    .build();

            httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                    .thenAccept(response -> {
                        if (response.statusCode() != 200) {
                            logger.error("Platform rejected event: {} - {}", response.statusCode(), response.body());
                        }
                    })
                    .exceptionally(throwable -> {
                        logger.error("Error sending to platform: {}", throwable.getMessage(), throwable);
                        return null;
                    });

        } catch (Exception e) {
            logger.error("Error creating event payload: {}", e.getMessage(), e);
        }
    }
    
    private void broadcastToWebSocketClients(String tableName, String jsonPayload) {
        try {
            webSocketEventBroadcaster.broadcastTableEvent(tableName, jsonPayload);
        } catch (Exception e) {
            logger.error("Error broadcasting to WebSocket clients for table {}: {}", tableName, e.getMessage(), e);
        }
    }

    /**
     * Initialize replication components including connection, schema, type registry, and message decoder.
     */
    private void initializeReplicationComponents(ValidationResult result) throws SQLException {
        // Create a connection with replication properties
        Properties props = new Properties();
        props.setProperty("user", dbUser);
        props.setProperty("password", dbPassword);
        PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "9.4");
        PGProperty.REPLICATION.set(props, "database");
        PGProperty.PREFER_QUERY_MODE.set(props, "simple");
        
        String url = String.format("jdbc:postgresql://%s:%s/%s", dbHost, dbPort, dbName);
        
        try {
            Connection conn = DriverManager.getConnection(url, props);
            replicationConnection = (PgConnection) conn;
            result.addInfo("Created replication connection successfully");
        } catch (SQLException e) {
            result.addError("Failed to initialize replication components: " + e.getMessage());
            throw e;
        }
    }

    @PreDestroy
    public void stop() {
        gracefulShutdown();
    }

    private void gracefulShutdown() {
        synchronized (shutdownLock) {
            if (shutdownRequested.getAndSet(true)) {
                logger.info("Graceful shutdown already in progress");
                return;
            }
        }

        logger.info("Starting graceful shutdown of PostgreSQL replication...");
        
        // Stop accepting new messages
        running.set(false);
        
        // Wait for in-flight transactions to complete
        waitForInFlightTransactions();
        
        // Close connections
        closeConnections();
        
        // Shutdown executor
        shutdownExecutor();
        
        logger.info("Graceful shutdown completed");
    }

    private void waitForInFlightTransactions() {
        logger.info("Waiting for in-flight transactions to complete...");
        
        long startTime = System.currentTimeMillis();
        long timeoutMs = 30000; // 30 seconds timeout
        
        while (transactionBuffer.getActiveTransactionCount() > 0) {
            long elapsed = System.currentTimeMillis() - startTime;
            if (elapsed > timeoutMs) {
                logger.warn("Timeout waiting for {} in-flight transactions to complete after {}ms", 
                    transactionBuffer.getActiveTransactionCount(), elapsed);
                break;
            }
            
            logger.debug("Waiting for {} active transactions ({}ms elapsed)", 
                transactionBuffer.getActiveTransactionCount(), elapsed);
            
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Interrupted while waiting for transactions to complete");
                break;
            }
        }
        
        if (transactionBuffer.getActiveTransactionCount() == 0) {
            logger.info("All in-flight transactions completed successfully");
        } else {
            logger.warn("Proceeding with shutdown despite {} active transactions", 
                transactionBuffer.getActiveTransactionCount());
        }
    }

    private void closeConnections() {
        if (replicationConnection != null) {
            try {
                replicationConnection.close();
                logger.info("Closed replication connection");
            } catch (SQLException e) {
                logger.error("Error closing replication connection", e);
            }
        }
        
        if (httpClient != null) {
            try {
                httpClient.close();
                logger.info("Closed HTTP client");
            } catch (Exception e) {
                logger.error("Error closing HTTP client", e);
            }
        }
    }

    private void shutdownExecutor() {
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                    logger.warn("Executor did not terminate within 10 seconds, forcing shutdown");
                    executor.shutdownNow();
                    if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                        logger.error("Executor did not terminate after forced shutdown");
                    }
                }
                logger.info("Executor terminated successfully");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Interrupted while waiting for executor to terminate");
                executor.shutdownNow();
            }
        }
    }


    /**
     * Connects to the postgresql server, retries forever.
     */
    private Connection connectWithRetry() {
        String url = String.format("jdbc:postgresql://%s:%s/%s",
                dbHost, dbPort, dbName);

        Exception lastException = null;

        // Loops until successfully connected to the postgres server.
        while (true) {
            try {
                logger.info("Attempting to connect to PostgreSQL..");
                Connection conn = DriverManager.getConnection(url, dbUser, dbPassword);
                conn.setAutoCommit(true);
                logger.info("Successfully connected to PostgreSQL");
                return conn;
            } catch (SQLException e) {
                lastException = e;
                logger.error("Failed to connect to postgres", e);

                try {
                    Thread.sleep(3000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        logger.error("Failed to connect", lastException);
        return null;
    }

    /**
     * Validates user permissions.
     */
    private void validatePermissions(Connection conn, ValidationResult result) throws SQLException {
        // Check if user has replication permission
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT rolreplication FROM pg_roles WHERE rolname = current_user")) {
            if (rs.next() && !rs.getBoolean(1)) {
                result.addError("Current user does not have replication permission");
                return;
            }
        }

        // Check if user has required permissions on the schema
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT has_schema_privilege(current_user, 'public', 'USAGE')")) {
            if (rs.next() && !rs.getBoolean(1)) {
                result.addError("Current user does not have USAGE permission on public schema");
                return;
            }
        }

        result.addInfo("User permissions validated successfully");
    }

    /**
     * Validates and configures WAL settings, waiting for them to be properly set.
     * Handles connection issues during PostgreSQL restart.
     */
    private void validateWALSettings(Connection initialConn, ValidationResult result) throws SQLException {
        boolean walSettingsValid = false;
        int attempts = 0;
        Connection conn = initialConn;

        try {
            // First attempt to set parameters with initial connection
            checkAndSetWALParameters(conn, result, true);
        } catch (SQLException e) {
            logger.warn("Error during initial WAL parameter check: {}", e.getMessage());
            // Continue to retry loop
        }

        while (!walSettingsValid) {
            try {
                if (attempts > 0) {
                    logger.info("Waiting 3 seconds before checking WAL settings again");
                    Thread.sleep(3000);

                    // Try to reconnect if connection is closed or invalid
                    if (conn == null || conn.isClosed() || !conn.isValid(1)) {
                        try {
                            if (conn != null && !conn.isClosed()) {
                                conn.close();
                            }
                        } catch (SQLException e) {
                            // Ignore errors while closing connection
                        }

                        try {
                            conn = connectWithRetry();
                            if (conn != null) {
                                logger.info("Successfully reconnected to PostgreSQL");
                            }
                        } catch (Exception e) {
                            logger.debug("Failed to reconnect to PostgreSQL: {}", e.getMessage());
                            attempts++;
                            continue; // Skip this attempt if we can't connect
                        }
                    }
                }

                if (conn == null) {
                    logger.debug("No connection available, skipping attempt {}", attempts + 1);
                    attempts++;
                    continue;
                }

                attempts++;
                walSettingsValid = checkAndSetWALParameters(conn, result, false);

                if (walSettingsValid) {
                    result.addInfo("WAL settings validated successfully after " + attempts + " attempts");
                }

            } catch (SQLException e) {
                logger.debug("Database error during WAL check (attempt {}): {}", attempts, e.getMessage());
                // Continue the loop to retry
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting to check WAL settings", e);
            }
        }
    }

    /**
     * Checks and optionally sets WAL parameters.
     * Returns true if all parameters are valid.
     */
    private boolean checkAndSetWALParameters(Connection conn, ValidationResult result, boolean setParameters) throws SQLException {
        boolean allValid = true;

        // Check wal_level
        String walLevel = getPostgresParameter(conn, "wal_level");
        if (!"logical".equals(walLevel)) {
            allValid = false;
            if (setParameters) {
                setPostgresParameter(conn, "wal_level", "logical", result);
            }
            logger.info("Waiting for wal_level to be set to 'logical' (current: {})", walLevel);
        }

        // Check max_replication_slots
        int maxReplicationSlots = Integer.parseInt(getPostgresParameter(conn, "max_replication_slots"));
        if (maxReplicationSlots < 5) {
            allValid = false;
            if (setParameters) {
                setPostgresParameter(conn, "max_replication_slots", "5", result);
            }
            logger.info("Waiting for max_replication_slots to be >= 5 (current: {})", maxReplicationSlots);
        }

        // Check max_wal_senders
        int maxWalSenders = Integer.parseInt(getPostgresParameter(conn, "max_wal_senders"));
        if (maxWalSenders < 5) {
            allValid = false;
            if (setParameters) {
                setPostgresParameter(conn, "max_wal_senders", "5", result);
            }
            logger.info("Waiting for max_wal_senders to be >= 5 (current: {})", maxWalSenders);
        }

        return allValid;
    }

    /**
     * Validates and creates publication if needed.
     */
    private void validatePublication(Connection conn, ValidationResult result) throws SQLException {

        // Check if publication exists
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT 1 FROM pg_publication WHERE pubname = '" + publicationName + "'")) {
            if (!rs.next()) {
                // Create publication for all tables
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("CREATE PUBLICATION " + publicationName + " FOR ALL TABLES");
                    result.addInfo("Created publication: " + publicationName);
                }
            } else {
                result.addInfo("Publication already exists: " + publicationName);
            }
        }
    }

    /**
     * Validates and creates replication slot if needed.
     */
    private void validateReplicationSlot(Connection conn, ValidationResult result) throws SQLException {
        // Check if slot exists
        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = '" + slotName + "'")) {
            if (!rs.next()) {
                // Create replication slot
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("SELECT pg_create_logical_replication_slot('" + slotName + "', 'pgoutput')");
                    result.addInfo("Created replication slot: " + slotName);
                }
            } else {
                result.addInfo("Replication slot already exists: " + slotName);
            }
        }
    }

    /**
     * Sets REPLICA IDENTITY FULL for tables without primary keys.
     */
    private void validateReplicaIdentity(Connection conn, ValidationResult result) throws SQLException {
        List<String> tablesWithoutPK = new ArrayList<>();

        try (ResultSet rs = conn.createStatement().executeQuery(
                "SELECT schemaname, tablename FROM pg_tables t " +
                        "WHERE schemaname = 'public' " +
                        "AND NOT EXISTS (" +
                        "    SELECT 1 FROM information_schema.table_constraints c " +
                        "    WHERE c.table_schema = t.schemaname " +
                        "    AND c.table_name = t.tablename " +
                        "    AND c.constraint_type = 'PRIMARY KEY'" +
                        ")")) {

            while (rs.next()) {
                String tableName = rs.getString("tablename");
                tablesWithoutPK.add(tableName);

                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("ALTER TABLE public." + tableName + " REPLICA IDENTITY FULL");
                    result.addInfo("Set REPLICA IDENTITY FULL for table: " + tableName);
                }
            }
        }

        if (!tablesWithoutPK.isEmpty()) {
            result.addWarning("Tables without primary keys (REPLICA IDENTITY FULL set): " + String.join(", ", tablesWithoutPK));
        }
    }

    /**
     * Generates connection information summary.
     */
    private void generateConnectionInfo(Connection conn, ValidationResult result) throws SQLException {
        String listenAddresses = getPostgresParameter(conn, "listen_addresses");
        String port = getPostgresParameter(conn, "port");

        // If listen_addresses is '*', use the connection host
        if ("*".equals(listenAddresses)) {
            listenAddresses = dbHost;
        }

        result.addInfo(String.format("""
            
            Exoquic Connection Information:
            ===========================
            Host: %s
            Port: %s
            Database: %s
            Username: %s
            Replication Slot: %s
            Publication: %s
            """,
                listenAddresses,
                port,
                dbName,
                dbUser,
                slotName,
                publicationName
        ));
    }

    /**
     * Gets a PostgreSQL parameter value.
     */
    private String getPostgresParameter(Connection conn, String paramName) throws SQLException {
        try (ResultSet rs = conn.createStatement().executeQuery("SHOW " + paramName)) {
            if (rs.next()) {
                return rs.getString(1);
            }
            throw new SQLException("Parameter not found: " + paramName);
        }
    }

    /**
     * Sets a PostgreSQL parameter value.
     */
    private void setPostgresParameter(Connection conn, String paramName, String value, ValidationResult result) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("ALTER SYSTEM SET " + paramName + " = '" + value + "'");
            result.addInfo("Changed " + paramName + " to: " + value);
        }
    }

    /**
     * Validation result class.
     */
    public static class ValidationResult {
        private final List<String> messages = new ArrayList<>();
        private final List<String> warnings = new ArrayList<>();
        private final List<String> errors = new ArrayList<>();

        public void addInfo(String message) {
            messages.add("INFO: " + message);
        }

        public void addWarning(String message) {
            warnings.add("WARNING: " + message);
        }

        public void addError(String message) {
            errors.add("ERROR: " + message);
        }

        public boolean isSuccess() {
            return errors.isEmpty();
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();

            // Add errors first
            errors.forEach(error -> sb.append(error).append('\n'));

            // Add warnings
            warnings.forEach(warning -> sb.append(warning).append('\n'));

            // Add info messages
            messages.forEach(message -> sb.append(message).append('\n'));

            return sb.toString();
        }
    }

}
