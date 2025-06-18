import dotenv from 'dotenv';

dotenv.config();

export class Config {
  constructor() {
    this.postgres = {
      host: process.env.POSTGRES_HOST || 'localhost',
      port: parseInt(process.env.POSTGRES_PORT) || 5432,
      database: process.env.POSTGRES_DATABASE || 'postgres',
      user: process.env.POSTGRES_USER || 'postgres',
      password: process.env.POSTGRES_PASSWORD || 'password'
    };

    this.exoquic = {
      endpoint: process.env.EXOQUIC_ENDPOINT || 'https://api.exoquic.com',
      apiKey: process.env.EXOQUIC_API_KEY || ''
    };

    this.replication = {
      slotName: process.env.REPLICATION_SLOT_NAME || 'realtime_slot',
      publicationName: process.env.PUBLICATION_NAME || 'realtime_publication'
    };

    this.logLevel = process.env.LOG_LEVEL || 'info';

    this.validate();
  }

  validate() {
    if (!this.postgres.host) {
      throw new Error('POSTGRES_HOST is required');
    }
    if (!this.postgres.database) {
      throw new Error('POSTGRES_DATABASE is required');
    }
    if (!this.postgres.user) {
      throw new Error('POSTGRES_USER is required');
    }
    if (!this.postgres.password) {
      throw new Error('POSTGRES_PASSWORD is required');
    }
    if (!this.exoquic.endpoint) {
      throw new Error('EXOQUIC_ENDPOINT is required');
    }
    if (!this.exoquic.apiKey) {
      throw new Error('EXOQUIC_API_KEY is required');
    }
  }

  getConnectionString() {
    return `postgresql://${this.postgres.user}:${this.postgres.password}@${this.postgres.host}:${this.postgres.port}/${this.postgres.database}`;
  }

  getReplicationConnectionString() {
    return `${this.getConnectionString()}?replication=database`;
  }
}