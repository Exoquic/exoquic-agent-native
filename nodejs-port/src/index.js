import { PostgresReplication } from './core/PostgresReplication.js';

async function main() {
  console.info('Starting Exoquic Agent (Node.js)...');
  
  try {
    const replication = new PostgresReplication();
    await replication.start();
  } catch (error) {
    console.error('Failed to start replication:', error);
    process.exit(1);
  }
}

main().catch(console.error);