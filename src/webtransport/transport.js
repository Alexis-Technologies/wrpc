'use strict';

// The server-side transport of a WebTransport client: ServerWsTransport —
// the same outbound half a WebSocket gets, over a WtSocket — plus the one
// thing the socket needs that a WebSocket never did: to see a stream packet
// BEFORE it is serialized, so a binary stream's end or termination can
// travel as its own WebTransport stream's FIN or RESET instead of as a
// packet on the control stream (streams.js). Registered under 'wt' in the
// ServerTransport table, which is how RpcServer.attachSocket picks it for a
// socket attached with `kind: 'wt'`.

const { ServerTransport } = require('../transport.js');

const ServerWsTransport = ServerTransport.transport.ws;

class ServerWtTransport extends ServerWsTransport {
  kind = 'wt';

  send(obj, code = 200, text = null) {
    const socket = this.connection;
    // Under a codec the mux is off on both ends (the client announced no
    // streams), and control() answers false without looking further.
    if (obj.type === 'stream' && typeof socket.streamControl === 'function' && socket.streamControl(obj)) return true;
    return super.send(obj, code, text);
  }
}

ServerTransport.transport.wt = ServerWtTransport;

module.exports = { ServerWtTransport };
