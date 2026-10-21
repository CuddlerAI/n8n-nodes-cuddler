'use strict';
// Synthetic public SDK boundary only. Native blob retention is a separate host gate.
const { SyntheticBoundary, clone, sha } = require('./file-entry-harness.cjs');
const { Cuddler } = require('../dist/nodes/Cuddler/Cuddler.node.js');

class UploadBoundary extends SyntheticBoundary {
  constructor(options = {}) {
    super(options); this.blobs = new Map(); this.blobId = 1;
  }
  context(parameters, options = {}) {
    const ctx = super.context(parameters, options);
    ctx.helpers.assertBinaryData = (index, property) => {
      const descriptor = typeof property === 'string' ? ctx.getInputData()[index]?.binary?.[property] : property;
      if (!descriptor || typeof descriptor.mimeType !== 'string') throw new Error('synthetic binary unavailable');
      return descriptor;
    };
    ctx.helpers.getBinaryDataBuffer = async (index, property) => {
      const descriptor = ctx.helpers.assertBinaryData(index, property);
      this.events.push({ kind: 'native-blob-read', id: descriptor.id ?? null });
      if (options.onBlobRead) await options.onBlobRead(descriptor);
      if (descriptor.id) {
        if (!this.blobs.has(descriptor.id)) throw new Error('synthetic native blob expired');
        return Buffer.from(this.blobs.get(descriptor.id));
      }
      return Buffer.from(descriptor.data, 'base64');
    };
    ctx.helpers.prepareBinaryData = async (bytes, fileName, mimeType) => {
      if (options.inlineStorage) return { data: Buffer.from(bytes).toString('base64'), mimeType, fileName };
      const id = 'synthetic-native-blob-' + this.blobId++;
      this.blobs.set(id, Buffer.from(bytes));
      this.events.push({ kind: 'native-blob-write', id, bytes: bytes.length, sha256: sha(bytes) });
      if (options.onBlobWrite) await options.onBlobWrite(id);
      return { data: 'filesystem-v2', id, fileName, mimeType };
    };
    return ctx;
  }
  async invoke(parameters, options = {}) {
    const node = options.defaultTransport ? new Cuddler() : new Cuddler({
      recoveryTransport: this.transport, preparedRead: options.preparedRead, upload: options.upload,
    });
    return node.execute.call(this.context(parameters, options));
  }
}

const binaryItem = (bytes, mimeType = 'image/png') => ({ json: {}, binary: { data: {
  data: Buffer.from(bytes).toString('base64'), mimeType, fileName: 'fixture-input.bin',
} } });

module.exports = { UploadBoundary, binaryItem, clone, sha };
