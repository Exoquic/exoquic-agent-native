export class LSNTracker {
  constructor() {
    this.lastReceivedLSN = '0/0';
    this.lastFlushedLSN = '0/0';
    this.lastAppliedLSN = '0/0';
  }

  updateReceived(lsn) {
    if (this.compareLSN(lsn, this.lastReceivedLSN) > 0) {
      this.lastReceivedLSN = lsn;
    }
  }

  updateFlushed(lsn) {
    if (this.compareLSN(lsn, this.lastFlushedLSN) > 0) {
      this.lastFlushedLSN = lsn;
    }
  }

  updateApplied(lsn) {
    if (this.compareLSN(lsn, this.lastAppliedLSN) > 0) {
      this.lastAppliedLSN = lsn;
    }
  }

  getLastReceived() {
    return this.lastReceivedLSN;
  }

  getLastFlushed() {
    return this.lastFlushedLSN;
  }

  getLastApplied() {
    return this.lastAppliedLSN;
  }

  // Compare two LSN strings (format: "X/Y")
  compareLSN(lsn1, lsn2) {
    const [high1, low1] = lsn1.split('/').map(s => parseInt(s, 16));
    const [high2, low2] = lsn2.split('/').map(s => parseInt(s, 16));
    
    if (high1 !== high2) {
      return high1 - high2;
    }
    return low1 - low2;
  }

  // Convert LSN string to BigInt for binary protocol
  lsnToBigInt(lsn) {
    const [high, low] = lsn.split('/').map(s => parseInt(s, 16));
    return (BigInt(high) << 32n) | BigInt(low);
  }

  // Convert BigInt to LSN string
  bigIntToLSN(value) {
    const high = Number(value >> 32n);
    const low = Number(value & 0xFFFFFFFFn);
    return `${high.toString(16).toUpperCase()}/${low.toString(16).toUpperCase()}`;
  }
}