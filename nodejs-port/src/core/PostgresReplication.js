import pg from 'pg';
import { EventEmitter } from 'events';
import { TransactionBuffer } from './TransactionBuffer.js';
import { JsonEventConverter } from './JsonEventConverter.js';
import { Config } from './Config.js';
import { PostgresReplicationConnection } from './PostgresReplicationConnection.js';
import { LSNTracker } from './LSNTracker.js';
import { PgOutputMessageDecoder } from '../pgoutput/core/PgOutputMessageDecoder.js';
import { PgOutputConfig } from '../pgoutput/config/PgOutputConfig.js';
import { TypeRegistry } from '../pgoutput/core/TypeRegistry.js';
import { Operation } from '../pgoutput/config/Operation.js';

const { Client } = pg;

export class PostgresReplication extends EventEmitter {
  constructor() {
    super();
    this.config = new Config();
    this.running = false;
    this.shutdownRequested = false;
    this.replicationClient = null;
    this.metadataClient = null;
    this.jsonEventConverter = new JsonEventConverter();
    this.lsnTracker = new LSNTracker();
    
    this.transactionBuffer = new TransactionBuffer(this.handleCommittedTransaction.bind(this));

    // Add graceful shutdown handlers
    process.on('SIGINT', () => this.gracefulShutdown());
    process.on('SIGTERM', () => this.gracefulShutdown());
  }

  async start() {
    try {
      console.info('Starting PostgreSQL replication...');
      await this.setupReplication();
      await this.consumeReplicationStream();
    } catch (error) {
      console.error('Replication failed:', error);
      throw error;
    }
  }

  async setupReplication() {
    console.info('Setting up replication...');
    
    // Create metadata connection
    this.metadataClient = new Client({
      connectionString: this.config.getConnectionString()
    });
    await this.metadataClient.connect();

    // Validate permissions and setup
    await this.validatePermissions();
    await this.validateWALSettings();
    await this.validatePublication();
    await this.validateReplicationSlot();
    await this.validateReplicaIdentity();

    // Initialize replication components
    await this.initializeReplicationComponents();

    console.info('Replication setup completed successfully');
  }

  async initializeReplicationComponents() {
    // Create type registry
    this.typeRegistry = await TypeRegistry.create(this.metadataClient);
    
    // Create pgoutput config
    this.pgOutputConfig = PgOutputConfig.builder()
      .databaseConnection(this.metadataClient)
      .publicationName(this.config.replication.publicationName)
      .includeTransactionMetadata(true)
      .build();
      
    // Create message decoder
    this.messageDecoder = new PgOutputMessageDecoder(this.pgOutputConfig, this.typeRegistry);
    
    console.info('Initialized replication components successfully');
  }

  async consumeReplicationStream() {
    console.info('Starting replication stream...');

    // Create native replication connection
    this.replicationConnection = new PostgresReplicationConnection(this.config);
    
    // Set up event handlers
    this.replicationConnection.on('copyData', (data) => {
      if (this.shutdownRequested) return;
      this.handleCopyData(data);
    });

    this.replicationConnection.on('error', (error) => {
      console.error('Replication connection error:', error);
      this.emit('error', error);
    });

    this.replicationConnection.on('close', () => {
      console.info('Replication connection closed');
      if (!this.shutdownRequested) {
        // Attempt to reconnect
        setTimeout(() => {
          if (!this.shutdownRequested) {
            console.info('Attempting to reconnect...');
            this.consumeReplicationStream().catch(console.error);
          }
        }, 5000);
      }
    });

    try {
      // Connect to PostgreSQL
      await this.replicationConnection.connect();
      console.info('Connected to PostgreSQL for replication');

      // Start logical replication
      await this.replicationConnection.startReplication(
        this.config.replication.slotName,
        this.config.replication.publicationName
      );

      this.running = true;
      console.info('🚀 Replication stream started!');

      // Start status updates
      this.startStatusUpdates();

      // Keep the service running
      return new Promise((resolve, reject) => {
        const checkShutdown = () => {
          if (this.shutdownRequested) {
            resolve();
          } else {
            setTimeout(checkShutdown, 1000);
          }
        };
        checkShutdown();
      });

    } catch (error) {
      console.error('Error starting replication stream:', error);
      throw error;
    }
  }

  startStatusUpdates() {
    // Send status updates every 10 seconds to keep the connection alive
    this.statusInterval = setInterval(() => {
      if (!this.shutdownRequested && this.replicationConnection) {
        try {
          this.replicationConnection.sendStandbyStatus(
            this.lsnTracker.getLastReceived(),
            this.lsnTracker.getLastFlushed(),
            this.lsnTracker.getLastApplied()
          );
          console.debug('Sent standby status update');
        } catch (error) {
          console.debug('Error sending status update:', error);
        }
      }
    }, 10000);
  }

  handleCopyData(data) {
    try {
      // Use the proper pgoutput message decoder
      const message = this.messageDecoder.decode(data);
      if (message) {
        // Extract LSN from the message or use a default
        const lsn = this.extractLSNFromMessage(message) || '0/0';
        this.handleReplicationMessage(message, lsn);
      }
    } catch (error) {
      console.error('Error handling copy data:', error);
    }
  }

  extractLSNFromMessage(message) {
    // In a real implementation, LSN would come from the WAL stream context
    // For now, we'll generate a simple incrementing LSN
    if (!this.currentLSN) {
      this.currentLSN = { high: 0, low: 0 };
    }
    
    this.currentLSN.low += 1;
    if (this.currentLSN.low > 0xFFFFFFFF) {
      this.currentLSN.high += 1;
      this.currentLSN.low = 0;
    }
    
    return `${this.currentLSN.high.toString(16).toUpperCase()}/${this.currentLSN.low.toString(16).toUpperCase()}`;
  }

  handleReplicationMessage(message, lsn = '0/0') {
    // Update LSN tracking
    this.lsnTracker.updateReceived(lsn);
    
    const txId = message.getTransactionId ? message.getTransactionId() : (message.transactionId || -1);

    switch (message.getOperation()) {
      case Operation.BEGIN:
        console.debug(`Transaction BEGIN: ${txId} at LSN: ${lsn}`);
        this.transactionBuffer.handleBegin(txId, message.getCommitTime(), lsn);
        break;

      case Operation.COMMIT:
        console.debug(`Transaction COMMIT: ${txId} at LSN: ${lsn}`);
        this.transactionBuffer.handleCommit(txId, message.getCommitTime(), lsn);
        // Mark as applied after successful commit
        this.lsnTracker.updateApplied(lsn);
        this.lsnTracker.updateFlushed(lsn);
        break;

      case Operation.INSERT:
      case Operation.UPDATE:
      case Operation.DELETE:
        console.debug(`Transaction event: ${txId} ${message.getOperation()} on table ${message.getTable()} at LSN: ${lsn}`);
        this.transactionBuffer.handleEvent(txId, message, lsn);
        break;

      default:
        console.debug(`Unhandled operation: ${message.getOperation()}`);
        break;
    }
  }

  handleCommittedTransaction(transaction) {
    console.info(`Processing committed transaction: ${transaction.getTransactionId()} with ${transaction.getEventCount()} events`);
    
    const orderedEvents = transaction.getOrderedEvents();
    
    for (const event of orderedEvents) {
      const message = event.getMessage();
      
      console.debug(`Processing event: ${message.getOperation()} on table ${message.getTable()} (TX: ${transaction.getTransactionId()})`);

      try {
        const jsonPayload = this.jsonEventConverter.convertToJson(message);
        this.sendToPlatform(jsonPayload);
        
        console.info(`Sent ${message.getOperation()} event for table ${message.getTable()} to platform`);
      } catch (error) {
        console.error(`Failed to process event for table ${message.getTable()}:`, error);
      }
    }
  }

  async sendToPlatform(jsonPayload) {
    try {
      console.info('Json payload:', jsonPayload);
      
      const response = await fetch(`${this.config.exoquic.endpoint}/api/event`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${this.config.exoquic.apiKey}`
        },
        body: jsonPayload
      });

      if (!response.ok) {
        const body = await response.text();
        console.error(`Platform rejected event: ${response.status} - ${body}`);
      }
    } catch (error) {
      console.error('Error sending to platform:', error);
    }
  }

  async validatePermissions() {
    const result = await this.metadataClient.query(
      "SELECT rolreplication FROM pg_roles WHERE rolname = current_user"
    );
    
    if (!result.rows[0]?.rolreplication) {
      throw new Error('Current user does not have replication permission');
    }

    const schemaResult = await this.metadataClient.query(
      "SELECT has_schema_privilege(current_user, 'public', 'USAGE')"
    );
    
    if (!schemaResult.rows[0]?.has_schema_privilege) {
      throw new Error('Current user does not have USAGE permission on public schema');
    }

    console.info('User permissions validated successfully');
  }

  async validateWALSettings() {
    const walLevel = await this.getPostgresParameter('wal_level');
    if (walLevel !== 'logical') {
      await this.setPostgresParameter('wal_level', 'logical');
      console.warn('Set wal_level to logical - PostgreSQL restart may be required');
    }

    const maxReplicationSlots = parseInt(await this.getPostgresParameter('max_replication_slots'));
    if (maxReplicationSlots < 5) {
      await this.setPostgresParameter('max_replication_slots', '5');
      console.warn('Set max_replication_slots to 5 - PostgreSQL restart may be required');
    }

    const maxWalSenders = parseInt(await this.getPostgresParameter('max_wal_senders'));
    if (maxWalSenders < 5) {
      await this.setPostgresParameter('max_wal_senders', '5');
      console.warn('Set max_wal_senders to 5 - PostgreSQL restart may be required');
    }

    console.info('WAL settings validated successfully');
  }

  async validatePublication() {
    const result = await this.metadataClient.query(
      "SELECT 1 FROM pg_publication WHERE pubname = $1",
      [this.config.replication.publicationName]
    );

    if (result.rows.length === 0) {
      await this.metadataClient.query(
        `CREATE PUBLICATION ${this.config.replication.publicationName} FOR ALL TABLES`
      );
      console.info(`Created publication: ${this.config.replication.publicationName}`);
    } else {
      console.info(`Publication already exists: ${this.config.replication.publicationName}`);
    }
  }

  async validateReplicationSlot() {
    const result = await this.metadataClient.query(
      "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1",
      [this.config.replication.slotName]
    );

    if (result.rows.length === 0) {
      await this.metadataClient.query(
        "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
        [this.config.replication.slotName]
      );
      console.info(`Created replication slot: ${this.config.replication.slotName}`);
    } else {
      console.info(`Replication slot already exists: ${this.config.replication.slotName}`);
    }
  }

  async validateReplicaIdentity() {
    const result = await this.metadataClient.query(`
      SELECT schemaname, tablename FROM pg_tables t 
      WHERE schemaname = 'public' 
      AND NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints c 
        WHERE c.table_schema = t.schemaname 
        AND c.table_name = t.tablename 
        AND c.constraint_type = 'PRIMARY KEY'
      )
    `);

    for (const row of result.rows) {
      const tableName = row.tablename;
      await this.metadataClient.query(`ALTER TABLE public.${tableName} REPLICA IDENTITY FULL`);
      console.info(`Set REPLICA IDENTITY FULL for table: ${tableName}`);
    }

    if (result.rows.length > 0) {
      console.warn(`Tables without primary keys (REPLICA IDENTITY FULL set): ${result.rows.map(r => r.tablename).join(', ')}`);
    }
  }

  async getPostgresParameter(paramName) {
    const result = await this.metadataClient.query(`SHOW ${paramName}`);
    return result.rows[0][paramName];
  }

  async setPostgresParameter(paramName, value) {
    await this.metadataClient.query(`ALTER SYSTEM SET ${paramName} = '${value}'`);
    console.info(`Changed ${paramName} to: ${value}`);
  }

  async gracefulShutdown() {
    if (this.shutdownRequested) {
      console.info('Graceful shutdown already in progress');
      return;
    }

    this.shutdownRequested = true;
    console.info('Starting graceful shutdown of PostgreSQL replication...');
    
    this.running = false;
    
    // Clear status interval
    if (this.statusInterval) {
      clearInterval(this.statusInterval);
    }
    
    // Wait for in-flight transactions
    await this.waitForInFlightTransactions();
    
    // Close connections
    await this.closeConnections();
    
    console.info('Graceful shutdown completed');
    process.exit(0);
  }

  async waitForInFlightTransactions() {
    console.info('Waiting for in-flight transactions to complete...');
    
    const startTime = Date.now();
    const timeoutMs = 30000; // 30 seconds timeout
    
    while (this.transactionBuffer.getActiveTransactionCount() > 0) {
      const elapsed = Date.now() - startTime;
      if (elapsed > timeoutMs) {
        console.warn(`Timeout waiting for ${this.transactionBuffer.getActiveTransactionCount()} in-flight transactions to complete after ${elapsed}ms`);
        break;
      }
      
      console.debug(`Waiting for ${this.transactionBuffer.getActiveTransactionCount()} active transactions (${elapsed}ms elapsed)`);
      
      await new Promise(resolve => setTimeout(resolve, 100));
    }
    
    if (this.transactionBuffer.getActiveTransactionCount() === 0) {
      console.info('All in-flight transactions completed successfully');
    } else {
      console.warn(`Proceeding with shutdown despite ${this.transactionBuffer.getActiveTransactionCount()} active transactions`);
    }
  }

  async closeConnections() {
    if (this.replicationConnection) {
      try {
        this.replicationConnection.close();
        console.info('Closed replication connection');
      } catch (error) {
        console.error('Error closing replication connection:', error);
      }
    }
    
    if (this.metadataClient) {
      try {
        await this.metadataClient.end();
        console.info('Closed metadata connection');
      } catch (error) {
        console.error('Error closing metadata connection:', error);
      }
    }
  }
}