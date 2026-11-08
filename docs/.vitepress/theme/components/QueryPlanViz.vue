<script setup lang="ts">
import { ref, computed } from 'vue'

// 3b1b-style: a logical plan, then the optimizer rewrites it one rule at a time.
interface Node {
  id: string
  label: string
  depth: number
}

// Nodes keep stable ids so the tree re-flows (FLIP) when rules reorder them.
const steps: { rule: string; why: string; cost: number; hot: string[]; nodes: Node[] }[] = [
  {
    rule: 'Your SQL, verbatim',
    why: 'The parser maps the query onto an algebra tree top-down: project on top, filter in the middle, join and scans at the bottom. Correct, but it reads everything.',
    cost: 100,
    hot: [],
    nodes: [
      { id: 'p', label: 'SELECT customer, total', depth: 0 },
      { id: 'f', label: 'WHERE year = 2024', depth: 1 },
      { id: 'j', label: 'JOIN on customer_id', depth: 2 },
      { id: 'so', label: 'SCAN orders (all columns, all years)', depth: 3 },
      { id: 'sc', label: 'SCAN customers (all columns)', depth: 3 },
    ],
  },
  {
    rule: 'Predicate pushdown',
    why: 'Filters slide to the scans: orders is filtered while being read instead of after the join. Rows entering the join drop ~70%.',
    cost: 62,
    hot: ['f', 'so'],
    nodes: [
      { id: 'p', label: 'SELECT customer, total', depth: 0 },
      { id: 'j', label: 'JOIN on customer_id', depth: 1 },
      { id: 'f', label: 'WHERE year = 2024', depth: 2 },
      { id: 'so', label: 'SCAN orders', depth: 3 },
      { id: 'sc', label: 'SCAN customers', depth: 2 },
    ],
  },
  {
    rule: 'Column pruning',
    why: 'Only the columns the query actually names survive the scan — 3 of 40 on orders, 1 of 12 on customers. Less I/O, less memory, less shuffle.',
    cost: 34,
    hot: ['so', 'sc'],
    nodes: [
      { id: 'p', label: 'SELECT customer, total', depth: 0 },
      { id: 'j', label: 'JOIN on customer_id', depth: 1 },
      { id: 'f', label: 'WHERE year = 2024', depth: 2 },
      { id: 'so', label: 'SCAN orders (customer_id, total, year)', depth: 3 },
      { id: 'sc', label: 'SCAN customers (customer_id, name)', depth: 2 },
    ],
  },
  {
    rule: 'Join strategy → broadcast',
    why: 'customers is tiny (200 rows). The cost model swaps the shuffle-heavy sort-merge for a broadcast: every worker gets a full copy, no shuffle at all.',
    cost: 12,
    hot: ['j', 'sc'],
    nodes: [
      { id: 'p', label: 'SELECT customer, total', depth: 0 },
      { id: 'j', label: 'BROADCAST JOIN on customer_id', depth: 1 },
      { id: 'f', label: 'WHERE year = 2024', depth: 2 },
      { id: 'so', label: 'SCAN orders (customer_id, total, year)', depth: 3 },
      { id: 'sc', label: 'SCAN customers → replicated to every worker', depth: 2 },
    ],
  },
]

const step = ref(0)
const cur = computed(() => steps[step.value])

const sql = 'SELECT c.name, o.total\nFROM orders o JOIN customers c\n  ON o.customer_id = c.customer_id\nWHERE o.year = 2024'
</script>

<template>
  <div class="viz-card" data-viz="queryplan">
    <div class="viz-head">🌳 The Optimizer, One Rule at a Time — interactive</div>

    <pre class="sql">{{ sql }}</pre>

    <TransitionGroup name="n" tag="div" class="tree">
      <div
        v-for="n in cur.nodes"
        :key="n.id"
        class="node"
        :class="{ hot: cur.hot.includes(n.id) }"
        :style="{ marginLeft: n.depth * 1.4 + 'rem' }"
      >
        {{ n.label }}
      </div>
    </TransitionGroup>

    <div class="cost">
      <div class="cost-label">cost</div>
      <div class="cost-track">
        <div class="cost-fill" :style="{ width: cur.cost + '%' }" />
      </div>
      <div class="cost-num">{{ cur.cost }}</div>
    </div>

    <div class="btns">
      <button @click="step = 0" :disabled="step === 0">⟲ Reset</button>
      <button @click="step++" :disabled="step >= steps.length - 1">
        Next step →
      </button>
    </div>

    <div class="caption">
      <b>Step {{ step }} — {{ cur.rule }}.</b> {{ cur.why }}
    </div>
  </div>
</template>

<style scoped>
.viz-card {
  border: 1px solid var(--vp-c-border);
  border-radius: 12px;
  padding: 1.1rem 1.25rem;
  margin: 1.5rem 0;
  background: var(--vp-c-bg-soft);
}
.viz-head {
  font-weight: 600;
  margin-bottom: 0.5rem;
}
.sql {
  font-size: 0.78rem;
  background: var(--vp-c-bg);
  border: 1px solid var(--vp-c-border);
  border-radius: 8px;
  padding: 0.5rem 0.7rem;
  margin: 0 0 0.7rem;
  color: var(--vp-c-text-2);
}
.tree {
  display: flex;
  flex-direction: column;
  gap: 0.3rem;
  margin-bottom: 0.8rem;
}
.node {
  border: 1.5px solid var(--vp-c-brand-1);
  border-radius: 8px;
  padding: 0.28rem 0.6rem;
  font-size: 0.78rem;
  font-family: var(--vp-font-family-mono, monospace);
  background: var(--vp-c-bg);
  color: var(--vp-c-text-1);
  width: fit-content;
  max-width: 100%;
  transition: all 0.5s ease;
}
.node.hot {
  border-color: var(--vp-c-warning-1, #d4a017);
  box-shadow: 0 0 0 3px color-mix(in srgb, var(--vp-c-warning-1, #d4a017) 25%, transparent);
}
.n-move {
  transition: transform 0.55s cubic-bezier(0.4, 0, 0.2, 1);
}
.cost {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  margin-bottom: 0.7rem;
}
.cost-label {
  font-size: 0.72rem;
  color: var(--vp-c-text-2);
  text-transform: uppercase;
  letter-spacing: 0.05em;
}
.cost-track {
  flex: 1;
  height: 10px;
  border-radius: 5px;
  background: var(--vp-c-bg);
  border: 1px solid var(--vp-c-border);
  overflow: hidden;
}
.cost-fill {
  height: 100%;
  background: linear-gradient(90deg, var(--vp-c-brand-1), var(--vp-c-danger-1, #e05252));
  transition: width 0.7s cubic-bezier(0.4, 0, 0.2, 1);
}
.cost-num {
  font-size: 0.78rem;
  font-weight: 700;
  min-width: 2ch;
  color: var(--vp-c-text-1);
}
.btns {
  display: flex;
  gap: 0.4rem;
  margin-bottom: 0.5rem;
}
.btns button {
  border: 1px solid var(--vp-c-border);
  background: var(--vp-c-bg);
  color: var(--vp-c-text-1);
  border-radius: 8px;
  padding: 0.25rem 0.7rem;
  font-size: 0.82rem;
  cursor: pointer;
}
.btns button:disabled {
  opacity: 0.35;
  cursor: default;
}
.caption {
  font-size: 0.85rem;
  color: var(--vp-c-text-2);
  min-height: 2.4em;
}
.caption b {
  color: var(--vp-c-text-1);
}
</style>
