'use strict';

// The real OpenTelemetry metrics SDK exporting into memory — the rig the
// telemetry suites share (the SDK is a devDependency; wrpc itself only ever
// sees the injected api). `collect()` answers the CURRENT cumulative
// snapshot: the exporter keeps every flush, and the last batch is the one
// with the values as they are now. Not a *.test.js.

const {
  AggregationTemporality,
  InMemoryMetricExporter,
  MeterProvider,
  PeriodicExportingMetricReader,
} = require('@opentelemetry/sdk-metrics');

const { SCOPE_NAME } = require('../../src/telemetry/index.js');

const createMetrics = () => {
  const exporter = new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE);
  const reader = new PeriodicExportingMetricReader({ exporter, exportIntervalMillis: 3_600_000 });
  const provider = new MeterProvider({ readers: [reader] });
  const collect = async () => {
    await reader.forceFlush();
    const metrics = [];
    const batch = exporter.getMetrics().at(-1);
    for (const scope of batch?.scopeMetrics ?? []) metrics.push(...scope.metrics);
    return metrics;
  };
  return { provider, meter: provider.getMeter(SCOPE_NAME), collect };
};

/** The data point of `name` whose attributes satisfy `match`, or undefined. */
const point = (metrics, name, match = () => true) =>
  metrics.find((metric) => metric.descriptor.name === name)?.dataPoints.find((entry) => match(entry.attributes));

module.exports = { createMetrics, point };
