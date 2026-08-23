'use strict';

// The node-side barrel. `src/client.js` deliberately requires
// ./telemetry/client.js directly instead of this file: requiring the barrel
// would pull the server half into every browser bundle.

const { createServerTelemetry } = require('./server.js');
const { createClientTelemetry } = require('./client.js');
const {
  SCOPE_NAME,
  SPAN_STATUS_ERROR,
  SPAN_KIND_SERVER,
  SPAN_KIND_CLIENT,
  SPAN_KIND_CONSUMER,
  TRACEPARENT,
  TRACESTATE,
} = require('./shared.js');

module.exports = {
  createServerTelemetry,
  createClientTelemetry,
  SCOPE_NAME,
  SPAN_STATUS_ERROR,
  SPAN_KIND_SERVER,
  SPAN_KIND_CLIENT,
  SPAN_KIND_CONSUMER,
  TRACEPARENT,
  TRACESTATE,
};
