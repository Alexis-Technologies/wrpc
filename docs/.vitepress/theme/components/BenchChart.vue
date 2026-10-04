<script setup lang="ts">
// A benchmark as horizontal bars, the way fastify.dev/benchmarks shows one:
// one bar per stack, wRPC in the brand green, everything else in a neutral
// gray (the "emphasis" form — one subject, the rest context), the value at
// the bar's end, sorted best first. The numbers come from benchmarks.json,
// which mirrors the table that follows every chart on the page — the table
// is the accessible, searchable view; tests/package/consistency.test.js
// keeps the two equal. No chart library: the docs theme stays dependency-free
// beyond VitePress itself.
import { computed, ref } from 'vue';
import benchmarks from '../benchmarks.json';

type Metric = { key: string; label: string; unit: string; better: 'higher' | 'lower'; digits?: number };
type Row = { id: string; label: string; kind: 'wrpc' | 'other'; [metric: string]: string | number };
type Dataset = { title: string; source: string; note: string; metrics: Metric[]; rows: Row[] };

const props = defineProps<{
  set: string;
  metric?: string;
  // A subset of row ids, in no particular order — the home page shows a few.
  pick?: string;
  title?: string;
}>();

const data = computed<Dataset>(() => {
  const dataset = (benchmarks as Record<string, Dataset>)[props.set];
  if (!dataset) throw new Error(`BenchChart: no dataset "${props.set}" in benchmarks.json`);
  return dataset;
});

const active = ref(props.metric ?? data.value.metrics[0].key);
const metric = computed(() => data.value.metrics.find((m) => m.key === active.value) ?? data.value.metrics[0]);

const rows = computed(() => {
  const ids = props.pick ? new Set(props.pick.split(',').map((id) => id.trim())) : null;
  const key = metric.value.key;
  const present = data.value.rows.filter(
    (row) => (ids === null || ids.has(row.id)) && typeof row[key] === 'number',
  );
  const sign = metric.value.better === 'lower' ? 1 : -1;
  return present.slice().sort((a, b) => sign * ((a[key] as number) - (b[key] as number)));
});

const max = computed(() => Math.max(...rows.value.map((row) => row[metric.value.key] as number)));
// The reference a tooltip compares against: the first wRPC row in the data's
// own order — the default configuration — not whichever sorts best, so the
// "×" keeps one meaning when the metric changes.
const reference = computed(() => {
  const shown = new Set(rows.value);
  return data.value.rows.find((row) => row.kind === 'wrpc' && shown.has(row)) ?? null;
});

const format = (value: number) =>
  value.toLocaleString('en-US', { maximumFractionDigits: metric.value.digits ?? 0 });

const share = (row: Row) => Math.max(0.004, (row[metric.value.key] as number) / max.value);

// "1.84× wRPC — own WebSocket": the row against the reference row, in the
// metric's own unit (for latency, lower is the better number).
const versus = (row: Row) => {
  const ref = reference.value;
  if (!ref || ref === row) return '';
  const ratio = (row[metric.value.key] as number) / (ref[metric.value.key] as number);
  return `${ratio.toFixed(2)}× ${ref.label}`;
};

const hovered = ref<string | null>(null);
</script>

<template>
  <figure class="bench-chart" :aria-label="`${props.title ?? data.title}: ${metric.label}, ${metric.unit}`">
    <figcaption class="bench-head">
      <span class="bench-title">{{ props.title ?? data.title }}</span>
      <span class="bench-sub">{{ metric.unit }} · {{ metric.better }} is better</span>
    </figcaption>

    <div v-if="data.metrics.length > 1" class="bench-tabs" role="tablist" aria-label="Metric">
      <button
        v-for="m in data.metrics"
        :key="m.key"
        type="button"
        role="tab"
        class="bench-tab"
        :aria-selected="m.key === active"
        @click="active = m.key"
      >
        {{ m.label }}
      </button>
    </div>

    <ol class="bench-rows">
      <li
        v-for="row in rows"
        :key="row.id"
        class="bench-row"
        :class="[row.kind, { hovered: hovered === row.id }]"
        tabindex="0"
        :aria-label="`${row.label}: ${format(row[metric.key] as number)} ${metric.unit}`"
        @pointerenter="hovered = row.id"
        @pointerleave="hovered = null"
        @focus="hovered = row.id"
        @blur="hovered = null"
      >
        <span class="bench-label">{{ row.label }}</span>
        <span class="bench-plot">
          <span class="bench-bar" :style="{ '--share': share(row) }" />
          <span class="bench-value">{{ format(row[metric.key] as number) }}</span>
          <span v-if="hovered === row.id && versus(row)" class="bench-tip" role="tooltip">
            <strong>{{ format(row[metric.key] as number) }} {{ metric.unit }}</strong>
            <span>{{ versus(row) }}</span>
          </span>
        </span>
      </li>
    </ol>

    <p class="bench-foot">
      {{ data.note }} · <code>{{ data.source }}</code>
    </p>
  </figure>
</template>

<style scoped>
/* Accent and de-emphasis colors, validated (contrast >= 3:1 against the page,
   accent/gray distinguishable under protan/deutan) on both surfaces. */
.bench-chart {
  --bench-accent: #4c8f3d;
  --bench-muted: #8e8e93;
  --bench-label-width: 16rem;
  margin: 20px 0 16px;
  padding: 16px 20px 12px;
  border: 1px solid var(--vp-c-divider);
  border-radius: 12px;
  background: var(--vp-c-bg);
}
:global(.dark) .bench-chart {
  --bench-accent: #5fa04e;
  --bench-muted: #76767c;
}

.bench-head {
  display: flex;
  flex-wrap: wrap;
  align-items: baseline;
  justify-content: space-between;
  gap: 4px 16px;
  margin-bottom: 10px;
}
.bench-title {
  font-weight: 600;
  color: var(--vp-c-text-1);
}
.bench-sub {
  font-size: 13px;
  color: var(--vp-c-text-2);
}

.bench-tabs {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  margin-bottom: 12px;
}
.bench-tab {
  padding: 2px 10px;
  border: 1px solid var(--vp-c-divider);
  border-radius: 999px;
  font-size: 13px;
  color: var(--vp-c-text-2);
  background: transparent;
  cursor: pointer;
}
.bench-tab:hover {
  color: var(--vp-c-text-1);
}
.bench-tab[aria-selected='true'] {
  border-color: var(--vp-c-brand-2);
  color: var(--vp-c-text-1);
  background: var(--vp-c-brand-soft);
}
.bench-tab:focus-visible,
.bench-row:focus-visible {
  outline: 2px solid var(--vp-c-brand-2);
  outline-offset: 2px;
}

.bench-rows {
  list-style: none;
  margin: 0;
  padding: 0;
}
.bench-row {
  display: grid;
  grid-template-columns: var(--bench-label-width) 1fr;
  align-items: center;
  gap: 12px;
  margin: 0;
  padding: 3px 0;
  border-radius: 4px;
  line-height: 1.3;
}
.bench-label {
  font-size: 13px;
  color: var(--vp-c-text-2);
  text-align: right;
}
.bench-row.wrpc .bench-label {
  font-weight: 600;
  color: var(--vp-c-text-1);
}

/* The plot: a 1px baseline the bars grow from, the value right after the
   bar's end — the bar gets the room left once the widest value fits. */
.bench-plot {
  position: relative;
  display: flex;
  align-items: center;
  gap: 6px;
  min-height: 22px;
  border-left: 1px solid var(--vp-c-divider);
}
.bench-bar {
  display: block;
  width: calc((100% - 6.5rem) * var(--share));
  height: 16px;
  border-radius: 0 4px 4px 0;
  background: var(--bench-muted);
  transition: width 0.25s ease;
}
.bench-row.wrpc .bench-bar {
  background: var(--bench-accent);
}
.bench-row.hovered .bench-bar {
  filter: brightness(1.12);
}
.bench-value {
  font-size: 13px;
  font-variant-numeric: tabular-nums;
  white-space: nowrap;
  color: var(--vp-c-text-1);
}

.bench-tip {
  position: absolute;
  z-index: 2;
  bottom: calc(100% + 4px);
  left: 8px;
  display: flex;
  flex-direction: column;
  gap: 2px;
  padding: 6px 10px;
  border: 1px solid var(--vp-c-divider);
  border-radius: 8px;
  font-size: 12px;
  white-space: nowrap;
  color: var(--vp-c-text-2);
  background: var(--vp-c-bg-elv);
  box-shadow: var(--vp-shadow-2);
  pointer-events: none;
}
.bench-tip strong {
  font-size: 13px;
  color: var(--vp-c-text-1);
}

.bench-foot {
  margin: 10px 0 0;
  font-size: 12px;
  line-height: 1.5;
  color: var(--vp-c-text-3);
}

/* Narrow: the label sits above its bar instead of beside it. */
@media (max-width: 640px) {
  .bench-chart {
    padding: 14px 14px 10px;
  }
  .bench-row {
    grid-template-columns: 1fr;
    gap: 2px;
    padding: 4px 0;
  }
  .bench-label {
    text-align: left;
  }
}
@media (prefers-reduced-motion: reduce) {
  .bench-bar {
    transition: none;
  }
}
</style>
