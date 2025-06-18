import net from 'net';
import crypto from 'crypto';
import { EventEmitter } from 'events';

export class PostgresReplicationConnection extends EventEmitter {
  constructor(config) {
    super();
    this.config = config;
    this.socket = null;
    this.buffer = Buffer.alloc(0);
    this.connected = false;
    this.authenticated = false;
    this.inCopyMode = false;
    this.lastLSN = '0/0';
  }

  async connect() {
    return new Promise((resolve, reject) => {
      const url = new URL(this.config.getConnectionString());
      const host = url.hostname;
      const port = parseInt(url.port) || 5432;

      this.socket = net.createConnection({ host, port }, () => {
        console.info(`Connected to PostgreSQL at ${host}:${port}`);
        this.connected = true;
        this.startAuthentication().then(resolve).catch(reject);
      });

      this.socket.on('data', (data) => {
        this.handleData(data);
      });

      this.socket.on('error', (error) => {
        console.error('Socket error:', error);
        this.emit('error', error);
        reject(error);
      });

      this.socket.on('close', () => {
        console.info('Socket closed');
        this.connected = false;
        this.emit('close');
      });
    });
  }

  async startAuthentication() {
    // Send startup message for replication connection
    const startupMessage = this.createStartupMessage();
    this.socket.write(startupMessage);

    return new Promise((resolve, reject) => {
      const authTimeout = setTimeout(() => {
        reject(new Error('Authentication timeout'));
      }, 10000);

      const onAuth = () => {
        clearTimeout(authTimeout);
        this.removeListener('authComplete', onAuth);
        this.removeListener('error', onError);
        resolve();
      };

      const onError = (error) => {
        clearTimeout(authTimeout);
        this.removeListener('authComplete', onAuth);
        this.removeListener('error', onError);
        reject(error);
      };

      this.once('authComplete', onAuth);
      this.once('error', onError);
    });
  }

  createStartupMessage() {
    const params = {
      user: new URL(this.config.getConnectionString()).username,
      database: new URL(this.config.getConnectionString()).pathname.substring(1),
      replication: 'database',
      application_name: 'exoquic-agent-nodejs'
    };

    let paramString = '';
    for (const [key, value] of Object.entries(params)) {
      paramString += key + '\0' + value + '\0';
    }
    paramString += '\0'; // Terminate with null byte

    const paramBuffer = Buffer.from(paramString, 'utf8');
    const length = 4 + 4 + paramBuffer.length; // length + protocol version + params
    
    const message = Buffer.allocUnsafe(4 + length);
    message.writeUInt32BE(length, 0);
    message.writeUInt32BE(196608, 4); // Protocol version 3.0 (0x30000)
    paramBuffer.copy(message, 8);
    
    console.debug('Startup message:', message.toString('hex'));
    return message;
  }

  handleData(data) {
    console.debug(`Received ${data.length} bytes:`, data.toString('hex').substring(0, 100));
    this.buffer = Buffer.concat([this.buffer, data]);
    
    while (this.buffer.length > 0) {
      if (!this.processMessage()) {
        break; // Need more data
      }
    }
  }

  processMessage() {
    if (this.buffer.length < 1) {
      return false; // Need at least message type
    }

    if (this.inCopyMode) {
      return this.processCopyData();
    }

    const messageType = this.buffer[0];
    
    // Skip null bytes or invalid message types
    if (messageType === 0) {
      this.buffer = this.buffer.subarray(1);
      return true;
    }

    const messageTypeChar = String.fromCharCode(messageType);
    
    if (this.buffer.length < 5) {
      return false; // Need at least type + length
    }

    const length = this.buffer.readUInt32BE(1);
    const totalLength = 1 + length;

    console.debug(`Processing message type '${messageTypeChar}' (0x${messageType.toString(16)}), length: ${length}, total: ${totalLength}`);

    if (this.buffer.length < totalLength) {
      console.debug(`Need more data: have ${this.buffer.length}, need ${totalLength}`);
      return false; // Need more data
    }

    const messageBuffer = this.buffer.subarray(0, totalLength);
    this.buffer = this.buffer.subarray(totalLength);

    console.debug(`Message data:`, messageBuffer.subarray(5).toString('hex').substring(0, 100));
    this.handleMessage(messageTypeChar, messageBuffer.subarray(5)); // Skip type + length
    return true;
  }

  processCopyData() {
    if (this.buffer.length < 1) {
      return false;
    }

    const messageType = String.fromCharCode(this.buffer[0]);
    
    if (messageType === 'd') { // CopyData
      if (this.buffer.length < 5) {
        return false;
      }
      
      const length = this.buffer.readUInt32BE(1);
      const totalLength = 1 + length;

      if (this.buffer.length < totalLength) {
        return false;
      }

      const data = this.buffer.subarray(5, totalLength);
      this.buffer = this.buffer.subarray(totalLength);
      
      console.debug(`Received CopyData: ${data.length} bytes`);
      this.emit('copyData', data);
      return true;
    } else if (messageType === 'c') { // CopyDone
      if (this.buffer.length < 5) {
        return false;
      }
      
      this.buffer = this.buffer.subarray(5);
      this.inCopyMode = false;
      console.debug('Received CopyDone');
      this.emit('copyDone');
      return true;
    } else {
      // Handle other messages normally in COPY mode
      this.inCopyMode = false;
      return this.processMessage();
    }
  }

  handleMessage(type, data) {
    switch (type) {
      case 'R': // Authentication
        this.handleAuthentication(data);
        break;
      case 'S': // ParameterStatus
        this.handleParameterStatus(data);
        break;
      case 'Z': // ReadyForQuery
        this.handleReadyForQuery(data);
        break;
      case 'E': // ErrorResponse
        this.handleError(data);
        break;
      case 'N': // NoticeResponse
        this.handleNotice(data);
        break;
      case 'W': // CopyBothResponse
        this.handleCopyBothResponse(data);
        break;
      case 'K': // BackendKeyData
        this.handleBackendKeyData(data);
        break;
      default:
        console.debug(`Unhandled message type: ${type}`);
        break;
    }
  }

  handleAuthentication(data) {
    if (data.length < 4) return;
    
    const authType = data.readUInt32BE(0);
    
    switch (authType) {
      case 0: // AuthenticationOk
        console.info('Authentication successful');
        this.authenticated = true;
        break;
      case 3: // AuthenticationCleartextPassword
        this.sendPassword();
        break;
      case 5: // AuthenticationMD5Password
        const salt = data.subarray(4, 8);
        this.sendMD5Password(salt);
        break;
      default:
        this.emit('error', new Error(`Unsupported authentication type: ${authType}`));
        break;
    }
  }

  sendPassword() {
    const password = new URL(this.config.getConnectionString()).password;
    const message = this.createMessage('p', Buffer.from(password + '\0', 'utf8'));
    this.socket.write(message);
  }

  sendMD5Password(salt) {
    const url = new URL(this.config.getConnectionString());
    const username = url.username;
    const password = url.password;
    
    // MD5 hash: md5(md5(password + username) + salt)
    const inner = crypto.createHash('md5').update(password + username).digest('hex');
    const outer = crypto.createHash('md5').update(inner + salt.toString('binary')).digest('hex');
    const hash = 'md5' + outer;
    
    const message = this.createMessage('p', Buffer.from(hash + '\0', 'utf8'));
    this.socket.write(message);
  }

  handleParameterStatus(data) {
    const params = data.toString('utf8').split('\0');
    if (params.length >= 2) {
      console.debug(`Parameter: ${params[0]} = ${params[1]}`);
    }
  }

  handleReadyForQuery(data) {
    const status = String.fromCharCode(data[0]);
    console.debug(`Ready for query, status: ${status}`);
    
    if (this.authenticated && !this.inCopyMode) {
      this.emit('authComplete');
    }
  }

  handleError(data) {
    const message = this.parseErrorMessage(data);
    console.error('PostgreSQL error:', message);
    this.emit('error', new Error(message));
  }

  handleNotice(data) {
    const message = this.parseErrorMessage(data);
    console.info('PostgreSQL notice:', message);
  }

  handleCopyBothResponse(data) {
    console.info('PostgreSQL entered COPY BOTH mode - replication stream ready');
    console.debug('CopyBothResponse data:', data.toString('hex'));
    this.inCopyMode = true;
    this.emit('copyMode');
  }

  handleBackendKeyData(data) {
    if (data.length >= 8) {
      const processId = data.readUInt32BE(0);
      const secretKey = data.readUInt32BE(4);
      console.debug(`Backend key data: process=${processId}, secret=${secretKey}`);
    }
  }

  parseErrorMessage(data) {
    let message = '';
    let i = 0;
    
    while (i < data.length) {
      const field = String.fromCharCode(data[i]);
      i++;
      
      if (field === '\0') break;
      
      let value = '';
      while (i < data.length && data[i] !== 0) {
        value += String.fromCharCode(data[i]);
        i++;
      }
      i++; // Skip null terminator
      
      if (field === 'M') { // Message
        message = value;
      }
    }
    
    return message;
  }

  createMessage(type, data) {
    const length = 4 + data.length;
    const message = Buffer.allocUnsafe(1 + length);
    message.writeUInt8(type.charCodeAt(0), 0);
    message.writeUInt32BE(length, 1);
    data.copy(message, 5);
    return message;
  }

  async startReplication(slotName, publicationName, startLSN = '0/0') {
    // Wait a moment for the connection to be fully ready
    await new Promise(resolve => setTimeout(resolve, 100));
    
    const query = `START_REPLICATION SLOT ${slotName} LOGICAL ${startLSN} (proto_version '1', publication_names '${publicationName}')`;
    
    console.info(`Starting replication with query: ${query}`);
    
    return new Promise((resolve, reject) => {
      // Create a simple query message following PostgreSQL protocol
      const queryBuffer = Buffer.from(query, 'utf8');
      const message = Buffer.allocUnsafe(5 + queryBuffer.length + 1);
      
      let offset = 0;
      message.writeUInt8(0x51, offset); // 'Q' = Query message type
      offset += 1;
      message.writeUInt32BE(4 + queryBuffer.length + 1, offset); // Length (including this field but not type)
      offset += 4;
      queryBuffer.copy(message, offset); // Query string
      offset += queryBuffer.length;
      message.writeUInt8(0, offset); // Null terminator
      
      console.debug(`Sending START_REPLICATION query (${message.length} bytes):`, message.toString('hex'));
      this.socket.write(message);

      const timeout = setTimeout(() => {
        this.removeListener('copyMode', onCopyMode);
        this.removeListener('error', onError);
        reject(new Error('Timeout waiting for replication to start'));
      }, 30000);

      const onCopyMode = () => {
        clearTimeout(timeout);
        this.removeListener('copyMode', onCopyMode);
        this.removeListener('error', onError);
        console.info('Successfully entered COPY mode for replication');
        resolve();
      };

      const onError = (error) => {
        clearTimeout(timeout);
        this.removeListener('copyMode', onCopyMode);
        this.removeListener('error', onError);
        console.error('Error starting replication:', error);
        reject(error);
      };

      this.once('copyMode', onCopyMode);
      this.once('error', onError);
    });
  }

  sendStandbyStatus(receivedLSN, flushedLSN, appliedLSN) {
    // Convert LSN strings to BigInt
    const receivedLSNBig = this.lsnToBigInt(receivedLSN);
    const flushedLSNBig = this.lsnToBigInt(flushedLSN);
    const appliedLSNBig = this.lsnToBigInt(appliedLSN);
    
    // Standby status update message
    const message = Buffer.allocUnsafe(1 + 4 + 1 + 8 + 8 + 8 + 8 + 1);
    let offset = 0;
    
    message.writeUInt8(0x64, offset); // 'd' for CopyData
    offset += 1;
    
    message.writeUInt32BE(4 + 1 + 8 + 8 + 8 + 8 + 1, offset); // Length
    offset += 4;
    
    message.writeUInt8(0x72, offset); // 'r' for standby status update
    offset += 1;
    
    // Write LSNs
    message.writeBigUInt64BE(receivedLSNBig, offset); // Received LSN
    offset += 8;
    message.writeBigUInt64BE(flushedLSNBig, offset); // Flushed LSN
    offset += 8;
    message.writeBigUInt64BE(appliedLSNBig, offset); // Applied LSN
    offset += 8;
    
    const now = Date.now() * 1000; // Convert to microseconds
    message.writeBigUInt64BE(BigInt(now), offset); // Timestamp
    offset += 8;
    
    message.writeUInt8(0, offset); // Reply requested flag
    
    this.socket.write(message);
  }

  lsnToBigInt(lsn) {
    const [high, low] = lsn.split('/').map(s => parseInt(s, 16));
    return (BigInt(high) << 32n) | BigInt(low);
  }

  close() {
    if (this.socket) {
      this.socket.end();
    }
  }
}