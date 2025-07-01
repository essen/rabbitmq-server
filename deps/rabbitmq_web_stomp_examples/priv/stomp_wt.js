// STOMP over WebTransport with Datagram Heartbeats
// License: Apache License V2.0
// Adapted from STOMP over WebSocket by Jeff Mesnil and FuseSource, Inc.

const Byte = {
  LF: '\x0A',
  NULL: '\x00'
};

class Frame {
  constructor(command, headers = {}, body = '') {
    this.command = command;
    this.headers = headers;
    this.body = body;
  }

  toString() {
    const lines = [this.command];
    const skipContentLength = this.headers['content-length'] === false;
    if (skipContentLength) {
      delete this.headers['content-length'];
    }
    for (const [name, value] of Object.entries(this.headers)) {
      lines.push(`${name}:${value}`);
    }
    if (this.body && !skipContentLength) {
      lines.push(`content-length:${Frame.sizeOfUTF8(this.body)}`);
    }
    lines.push(Byte.LF + this.body);
    return lines.join(Byte.LF);
  }

  static sizeOfUTF8(s) {
    return s ? encodeURI(s).match(/%..|./g).length : 0;
  }

  static unmarshallSingle(data) {
    const divider = data.search(new RegExp(`${Byte.LF}${Byte.LF}`));
    const headerLines = data.substring(0, divider).split(Byte.LF);
    const command = headerLines.shift();
    const headers = {};
    const trim = (str) => str.replace(/^\s+|\s+$/g, '');
    for (const line of headerLines.reverse()) {
      const idx = line.indexOf(':');
      headers[trim(line.substring(0, idx))] = trim(line.substring(idx + 1));
    }
    let body = '';
    const start = divider + 2;
    if (headers['content-length']) {
      const len = parseInt(headers['content-length']);
      body = data.substring(start, start + len);
    } else {
      for (let i = start; i < data.length; i++) {
        const chr = data.charAt(i);
        if (chr === Byte.NULL) break;
        body += chr;
      }
    }
    return new Frame(command, headers, body);
  }

  static unmarshall(datas) {
    const frames = datas.split(new RegExp(`${Byte.NULL}${Byte.LF}*`));
    const r = { frames: [], partial: '' };
    r.frames = frames.slice(0, -1).map(frame => this.unmarshallSingle(frame));
    const last_frame = frames[frames.length - 1];
    if (last_frame === Byte.LF || last_frame.search(new RegExp(`${Byte.NULL}${Byte.LF}*$`)) !== -1) {
      r.frames.push(this.unmarshallSingle(last_frame));
    } else {
      r.partial = last_frame;
    }
    return r;
  }

  static marshall(command, headers, body) {
    const frame = new Frame(command, headers, body);
    return frame.toString() + Byte.NULL;
  }
}

class Client {
  constructor(transport) {
    this.transport = transport;
    this.counter = 0;
    this.connected = false;
    this.heartbeat = { outgoing: 10000, incoming: 10000 };
    this.maxWebSocketFrameSize = 16 * 1024;
    this.subscriptions = {};
    this.partialData = '';
    this.writer = null;
    this.reader = null;
    this.datagramWriter = null;
    this.datagramReader = null;
    this.serverActivity = Date.now();
  }

  debug(message) {
    if (typeof window !== 'undefined' && window.console) {
      console.log(message);
    }
  }

  async _transmit(command, headers, body) {
    const out = Frame.marshall(command, headers, body);
    this.debug(`>>> ${out}`);
    if (!this.writer) {
      throw new Error('No writable stream available');
    }
    const encoder = new TextEncoder();
    let data = encoder.encode(out);
//    while (data.length > 0) {
//      const chunkSize = Math.min(data.length, this.maxWebSocketFrameSize);
//      const chunk = data.slice(0, chunkSize);
//      data = data.slice(chunkSize);
      try {
        console.log(`Sending: ${data} ${data.buffer}`);
        await this.writer.write(data.buffer); // ({ data: chunk });
        this.debug(`Sent chunk of ${data.length} bytes, remaining = 0`); // ${data.length}`);
      } catch (e) {
        this.debug(`Error sending data: ${e} ${data}`);
        throw e;
      }
//    }
  }

  _setupHeartbeat(headers) {
    if (!headers.version || ![Stomp.VERSIONS.V1_1, Stomp.VERSIONS.V1_2].includes(headers.version)) {
      return;
    }
    const [serverOutgoing, serverIncoming] = headers['heart-beat'].split(',').map(v => parseInt(v));
    if (this.heartbeat.outgoing !== 0 && serverIncoming !== 0) {
      const ttl = Math.max(this.heartbeat.outgoing, serverIncoming);
      this.debug(`send PING every ${ttl}ms via datagrams`);
      this.pinger = Stomp.setInterval(ttl, async () => {
        try {
          await this.datagramWriter.write(new TextEncoder().encode(Byte.LF));
          this.debug('>>> PING (datagram)');
        } catch (e) {
          this.debug(`PING failed: ${e}`);
        }
      });
    }
    if (this.heartbeat.incoming !== 0 && serverOutgoing !== 0) {
      const ttl = Math.max(this.heartbeat.incoming, serverOutgoing);
      this.debug(`check PONG every ${ttl}ms via datagrams`);
      this.ponger = Stomp.setInterval(ttl, () => {
        const delta = Date.now() - this.serverActivity;
        if (delta > ttl * 2) {
          this.debug(`No server activity for ${delta}ms`);
          this.transport.close();
        }
      });
    }
  }

  _parseConnect(...args) {
    let headers = {}, connectCallback, errorCallback;
    if (args.length === 2) {
      [headers, connectCallback] = args;
    } else if (args.length === 3) {
      if (typeof args[1] === 'function') {
        [headers, connectCallback, errorCallback] = args;
      } else {
        headers.login = args[0];
        headers.passcode = args[1];
        connectCallback = args[2];
      }
    } else if (args.length === 4) {
      headers.login = args[0];
      headers.passcode = args[1];
      connectCallback = args[2];
      errorCallback = args[3];
    } else {
      headers.login = args[0];
      headers.passcode = args[1];
      connectCallback = args[2];
      errorCallback = args[3];
      headers.host = args[4];
    }
    return [headers, connectCallback, errorCallback];
  }

  async connect(...args) {
    const [headers, connectCallback, errorCallback] = this._parseConnect(...args);
    this.connectCallback = connectCallback;
    this.debug('Opening WebTransport...');

    try {
      await this.transport.ready;
      this.debug('WebTransport ready');

      await new Promise(r => setTimeout(r, 1000));

      const stream = await this.transport.createBidirectionalStream();
      this.writer = stream.writable.getWriter();
      this.reader = stream.readable.getReader();

      this.datagramWriter = this.transport.datagrams.writable.getWriter();
      this.datagramReader = this.transport.datagrams.readable.getReader();

      this._readStream(errorCallback);
      this._readDatagrams();

      headers['wt-available-protocols'] = Stomp.VERSIONS.supportedVersions();
      headers['heart-beat'] = `${this.heartbeat.outgoing},${this.heartbeat.incoming}`;
      await this._transmit('CONNECT', headers);
    } catch (e) {
      this.debug(`WebTransport connection failed: ${e}`);
      if (errorCallback) errorCallback(e);
    }

    this.transport.closed.then(() => {
      const msg = `Lost connection to ${this.transport.url}`;
      this.debug(msg);
      this._cleanUp();
      if (errorCallback) errorCallback(msg);
    });
  }

  async _readStream(errorCallback) {
    const decoder = new TextDecoder();
    try {
      while (true) {
        const { value, done } = await this.reader.read();
        console.log(`Reading ${value}`);
        if (done) {
          this.debug('Readable stream closed');
          break;
        }
        this.serverActivity = Date.now();
        const data = decoder.decode(value);
        this.debug(`<<< ${data} (stream)`);
        const unmarshalledData = Frame.unmarshall(this.partialData + data);
        this.partialData = unmarshalledData.partial;
        for (const frame of unmarshalledData.frames) {
          console.log(`Frame command ${frame.command}`);
          switch (frame.command) {
            case 'CONNECTED':
              if (frame.headers['wt-protocol']) {
                this.debug(`Negotiated STOMP version: ${frame.headers['wt-protocol']}`);
              }
              this.debug(`connected to server ${frame.headers.server}`);
              this.connected = true;
              this._setupHeartbeat(frame.headers);
              if (this.connectCallback) this.connectCallback(frame);
              break;
            case 'MESSAGE':
              const subscription = frame.headers.subscription;
              const onreceive = this.subscriptions[subscription] || this.onreceive;
              if (onreceive) {
                const messageID = frame.headers['message-id'];
                frame.ack = (headers = {}) => this.ack(messageID, subscription, headers);
                frame.nack = (headers = {}) => this.nack(messageID, subscription, headers);
                onreceive(frame);
              } else {
                this.debug(`Unhandled received MESSAGE: ${frame}`);
              }
              break;
            case 'RECEIPT':
              if (this.onreceipt) this.onreceipt(frame);
              break;
            case 'ERROR':
              console.log("Received error back from CONNECT");
              if (errorCallback) errorCallback(frame);
              break;
            default:
              this.debug(`Unhandled frame: ${frame}`);
          }
        }
      }
    } catch (e) {
      this.debug(`Stream read error: ${e}`);
      if (errorCallback) errorCallback(e);
    }
  }

  async _readDatagrams() {
    const decoder = new TextDecoder();
    try {
      while (true) {
        const { value, done } = await this.datagramReader.read();
        if (done) {
          this.debug('Datagram stream closed');
          break;
        }
        this.serverActivity = Date.now();
        const data = decoder.decode(value);
        if (data === Byte.LF) {
          this.debug('<<< PONG (datagram)');
        } else {
          this.debug(`Unexpected datagram data: ${data}`);
        }
      }
    } catch (e) {
      this.debug(`Datagram read error: ${e}`);
    }
  }

  async disconnect(disconnectCallback, headers = {}) {
    await this._transmit('DISCONNECT', headers);
    this._cleanUp();
    try {
      await this.writer.close();
      await this.datagramWriter.close();
      await this.transport.close();
    } catch (e) {
      this.debug(`Error closing transport: ${e}`);
    }
    if (disconnectCallback) disconnectCallback();
  }

  _cleanUp() {
    this.connected = false;
    if (this.pinger) Stomp.clearInterval(this.pinger);
    if (this.ponger) Stomp.clearInterval(this.ponger);
  }

  async send(destination, headers = {}, body = '') {
    headers.destination = destination;
    await this._transmit('SEND', headers, body);
  }

  async subscribe(destination, callback, headers = {}) {
    if (!headers.id) {
      headers.id = `sub-${this.counter++}`;
    }
    headers.destination = destination;
    this.subscriptions[headers.id] = callback;
    await this._transmit('SUBSCRIBE', headers);
    return {
      id: headers.id,
      unsubscribe: () => this.unsubscribe(headers.id)
    };
  }

  async unsubscribe(id) {
    delete this.subscriptions[id];
    await this._transmit('UNSUBSCRIBE', { id });
  }

  async begin(transaction) {
    const txid = transaction || `tx-${this.counter++}`;
    await this._transmit('BEGIN', { transaction: txid });
    return {
      id: txid,
      commit: () => this.commit(txid),
      abort: () => this.abort(txid)
    };
  }

  async commit(transaction) {
    await this._transmit('COMMIT', { transaction });
  }

  async abort(transaction) {
    await this._transmit('ABORT', { transaction });
  }

  async ack(messageID, subscription, headers = {}) {
    headers['message-id'] = messageID;
    headers.subscription = subscription;
    await this._transmit('ACK', headers);
  }

  async nack(messageID, subscription, headers = {}) {
    headers['message-id'] = messageID;
    headers.subscription = subscription;
    await this._transmit('NACK', headers);
  }
}

const Stomp = {
  VERSIONS: {
    V1_0: '1.0',
    V1_1: '1.1',
    V1_2: '1.2',
    supportedVersions: () => '1.1,1.0'
  },

  client(url) {
    const transport = new WebTransport(url);
    return new Client(transport);
  },

  over(transport) {
    return new Client(transport);
  },

  Frame,

  setInterval(interval, f) {
    return window.setInterval(f, interval);
  },

  clearInterval(id) {
    return window.clearInterval(id);
  }
};

if (typeof exports !== 'undefined' && exports !== null) {
  exports.Stomp = Stomp;
} else if (typeof window !== 'undefined' && window !== null) {
  window.Stomp = Stomp;
} else {
  self.Stomp = Stomp;
}
