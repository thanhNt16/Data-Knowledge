<script setup lang="ts">
import { ref, computed } from 'vue'

// 3b1b-style interactive: consistent hashing ring.
// Keys and nodes sit on a hash ring; a key belongs to the first node
// clockwise from it. Add/remove nodes → only keys in the affected arc move.
// Toggle "naive mod-N" to see hash(key) % n reshuffle almost everything.

type Mode = 'ring' | 'mod'

const CANDIDATES = [
  { id: 'A', angle: 15 },
  { id: 'B', angle: 130 },
  { id: 'C', angle: 255 },
  { id: 'D', angle: 75 },
  { id: 'E', angle: 200 },
]
const KEY_ANGLES = [5, 35, 55, 85, 105, 140, 160, 185, 215, 240, 275, 320]
const NKEYS = KEY_ANGLES.length
const PALETTE = ['#e05252', '#3a8ee6', '#4caf7d', '#c9a227', '#9b59b6']

const mode = ref<Mode>('ring')
const active = ref(3) // first N candidates are active
const movedKeys = ref<Set<number>>(new Set())
const movedCount = ref(0)

const nodes = computed(() => CANDIDATES.slice(0, active.value))

function ownerOf(i: number, ns: { id: string; angle: number }[], m: Mode): string {
  if (m === 'mod') return ns[i % ns.length].id
  const a = KEY_ANGLES[i]
  const sorted = [...ns].sort((x, y) => x.angle - y.angle)
  for (const n of sorted) if (n.angle >= a) return n.id
  return sorted[0].id
}

const owners = computed(() => KEY_ANGLES.map((_, i) => ownerOf(i, nodes.value, mode.value)))

function add() {
  if (active.value >= CANDIDATES.length) return
  const prev = owners.value.slice()
  active.value++
  const moved = new Set<number>()
  owners.value.forEach((o, i) => { if (o !== prev[i]) moved.add(i) })
  movedKeys.value = moved
  movedCount.value = moved.size
}
function remove() {
  if (active.value <= 1) return
  const prev = owners.value.slice()
  active.value--
  const moved = new Set<number>()
  owners.value.forEach((o, i) => { if (o !== prev[i]) moved.add(i) })
  movedKeys.value = moved
  movedCount.value = moved.size
}
function pick(m: Mode) {
  if (mode.value === m) return
  const prev = owners.value.slice()
  mode.value = m
  const moved = new Set<number>()
  owners.value.forEach((o, i) => { if (o !== prev[i]) moved.add(i) })
  movedKeys.value = moved
  movedCount.value = moved.size
}

const R = 78
function xy(angle: number, r: number) {
  const rad = ((angle - 90) * Math.PI) / 180
  return { x: 100 + r * Math.cos(rad), y: 100 + r * Math.sin(rad) }
}
const nodeIdx = (id: string) => nodes.value.findIndex(n => n.id === id)
const colorOf = (id: string) => PALETTE[nodeIdx(id) % PALETTE.length]

const verdict = computed(() => {
  if (movedCount.value === 0) return 'No keys moved.'
  const s = `${movedCount.value} of ${NKEYS} keys moved`
  if (mode.value === 'ring')
    return movedCount.value <= Math.ceil(NKEYS / nodes.value.length) + 1
      ? `${s} — only the arc next to the changed node was reshuffled.`
      : `${s}.`
  return `${s} — mod-N remaps nearly every key on any membership change.`
})
</script>

<template>
  <div class="viz-card" data-viz="hashring">
    <div class="viz-head">🍩 Consistent Hashing — interactive</div>

    <div class="btns">
      <button :class="{ on: mode === 'ring' }" @click="pick('ring')">Consistent hash ring</button>
      <button :class="{ on: mode === 'mod' }" @click="pick('mod')">Naive mod-N</button>
      <span class="sp" />
      <button :disabled="active >= CANDIDATES.length" @click="add">+ Add node</button>
      <button :disabled="active <= 1" @click="remove">− Remove node</button>
    </div>

    <div class="row">
      <svg viewBox="0 0 200 200" class="ring" role="img" aria-label="Consistent hashing ring">
        <circle cx="100" cy="100" :r="R" class="circle" />
        <line
          v-for="(o, i) in owners"
          :key="'l' + i"
          :x1="xy(KEY_ANGLES[i], R - 14).x"
          :y1="xy(KEY_ANGLES[i], R - 14).y"
          :x2="xy(nodes[nodeIdx(o)].angle, R).x"
          :y2="xy(nodes[nodeIdx(o)].angle, R).y"
          class="edge"
          :class="{ moved: movedKeys.has(i) }"
          :style="{ stroke: colorOf(o) }"
        />
        <g v-for="n in nodes" :key="n.id">
          <circle
            :cx="xy(n.angle, R).x"
            :cy="xy(n.angle, R).y"
            r="9"
            class="node"
            :style="{ fill: colorOf(n.id) }"
          />
          <text :x="xy(n.angle, R + 16).x" :y="xy(n.angle, R + 16).y" class="nlabel">{{ n.id }}</text>
        </g>
        <circle
          v-for="(o, i) in owners"
          :key="'k' + i"
          :cx="xy(KEY_ANGLES[i], R - 14).x"
          :cy="xy(KEY_ANGLES[i], R - 14).y"
          r="4.5"
          class="key"
          :class="{ moved: movedKeys.has(i) }"
          :style="{ fill: colorOf(o) }"
        />
      </svg>
      <div class="stats">
        <div class="big">{{ movedCount }} / {{ NKEYS }}</div>
        <div class="sub">keys reassigned</div>
        <p class="verdict">{{ verdict }}</p>
      </div>
    </div>

    <div class="caption">
      Each dot is a key; its color shows which node owns it.
      <template v-if="mode === 'ring'">
        On the ring, a key walks clockwise to the first node — so adding node D or E only steals the
        arc just before it (~{{ Math.ceil(NKEYS / nodes.length) }} keys), and removing a node hands
        its arc to the next node clockwise.
      </template>
      <template v-else>
        With <code>hash(key) % n</code>, changing <code>n</code> changes the modulus — almost every
        key lands somewhere new. That is a full reshuffle of cache or partition contents.
      </template>
    </div>
  </div>
</template>

<style scoped>
.viz-card { border: 1px solid var(--vp-c-border); border-radius: 12px; padding: 1.1rem 1.25rem; margin: 1.5rem 0; background: var(--vp-c-bg-soft); }
.viz-head { font-weight: 600; margin-bottom: 0.5rem; }
.btns { display: flex; gap: 0.4rem; align-items: center; flex-wrap: wrap; margin-bottom: 0.8rem; }
.btns button { border: 1px solid var(--vp-c-border); background: var(--vp-c-bg); color: var(--vp-c-text-1); border-radius: 8px; padding: 0.25rem 0.7rem; font-size: 0.82rem; cursor: pointer; transition: all 0.2s; }
.btns button.on { background: var(--vp-c-brand-1); border-color: var(--vp-c-brand-1); color: var(--vp-c-bg); }
.btns button:disabled { opacity: 0.35; cursor: default; }
.btns .sp { flex: 1; }
.row { display: flex; gap: 1.2rem; align-items: center; flex-wrap: wrap; }
.ring { width: 240px; max-width: 100%; flex-shrink: 0; }
.circle { fill: none; stroke: var(--vp-c-border); stroke-width: 1.5; }
.edge { stroke-width: 1; opacity: 0.35; transition: all 0.5s; }
.edge.moved { opacity: 1; stroke-width: 2; }
.node { stroke: var(--vp-c-bg); stroke-width: 2; transition: all 0.5s; }
.nlabel { fill: var(--vp-c-text-2); font-size: 9px; text-anchor: middle; dominant-baseline: middle; }
.key { stroke: var(--vp-c-bg); stroke-width: 1; transition: fill 0.5s, r 0.3s; }
.key.moved { r: 6.5; stroke: var(--vp-c-text-1); }
.stats { min-width: 140px; }
.big { font-size: 1.6rem; font-weight: 700; color: var(--vp-c-text-1); }
.sub { font-size: 0.78rem; color: var(--vp-c-text-2); margin-bottom: 0.5rem; }
.verdict { font-size: 0.82rem; color: var(--vp-c-text-2); margin: 0; }
.caption { margin-top: 0.7rem; font-size: 0.85rem; color: var(--vp-c-text-2); }
.caption code { font-size: 0.8em; }
@media (max-width: 480px) { .row { flex-direction: column; } }
</style>
