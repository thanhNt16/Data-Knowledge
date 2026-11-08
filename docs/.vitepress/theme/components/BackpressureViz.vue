<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted } from 'vue'

// Interactive: producer emits at a set rate into a bounded buffer; a slower
// consumer drains it. Backpressure ON = credit-based flow control: the
// producer may only emit when the buffer has free capacity, so rates match.
// OFF = unbounded optimism: buffer overflows, events are dropped.

const CAPACITY = 10
const TICK_MS = 220
const MAX_PACKETS = 14

const prodRate = ref(4) // attempted emits per tick
const consRate = ref(2) // drains per tick
const bpOn = ref(true)
const running = ref(false)

// visual buffers: emitted (not yet arrived), buffered (in queue), drained (gone)
type P = { id: number; stage: 'net' | 'buf' | 'out' | 'drop' }
const packets = ref<P[]>([])
const bufferLen = ref(0)
const emittedTotal = ref(0)
const droppedTotal = ref(0)
const pendingCredit = ref(0) // fractional credit accumulator (ON mode)
const backlog = ref(0) // sender backlog waiting to ship (ON mode)

let timer: ReturnType<typeof setInterval> | null = null
let nextId = 0
let emitAcc = 0

const credits = computed(() => CAPACITY - bufferLen.value - packets.value.filter(p => p.stage === 'net').length)
const fillPct = computed(() => Math.min(100, (bufferLen.value / CAPACITY) * 100))
const overflow = computed(() => !bpOn.value && bufferLen.value >= CAPACITY)

const effProdRate = computed(() =>
  bpOn.value ? Math.min(prodRate.value, consRate.value) : prodRate.value
)

function tick() {
  // consumer drains
  let toDrain = Math.min(consRate.value, bufferLen.value)
  bufferLen.value -= toDrain
  // packets on the wire land in the buffer
  const arriving = packets.value.filter(p => p.stage === 'net')
  for (const p of arriving) {
    if (bufferLen.value < CAPACITY) {
      p.stage = 'buf'
      bufferLen.value++
    }
  }
  // producer emits
  if (bpOn.value) {
    // credit-based: sender ships only as many buffers as it was granted
    emitAcc += prodRate.value
    let want = Math.floor(emitAcc)
    emitAcc -= want
    const ship = Math.min(want + backlog.value, Math.max(0, credits.value))
    backlog.value += want - Math.min(want, Math.max(0, credits.value))
    if (backlog.value > CAPACITY * 2) backlog.value = CAPACITY * 2
    for (let i = 0; i < ship; i++) packets.value.push({ id: nextId++, stage: 'net' })
    emittedTotal.value += ship
  } else {
    emitAcc += prodRate.value
    let want = Math.floor(emitAcc)
    emitAcc -= want
    for (let i = 0; i < want; i++) {
      emittedTotal.value++
      if (bufferLen.value < CAPACITY) {
        bufferLen.value++
        packets.value.push({ id: nextId++, stage: 'net' })
        // force-land instantly next tick via 'net' — but overflow path needs drop now:
        packets.value[packets.value.length - 1].stage = 'net'
      } else {
        droppedTotal.value++
        packets.value.push({ id: nextId++, stage: 'drop' })
      }
    }
  }
  // aged buffered packets become consumed
  packets.value = packets.value
    .map(p => (p.stage === 'buf' && toDrain-- > 0 ? { ...p, stage: 'out' as const } : p))
    .filter((p, i, arr) => {
      if (p.stage === 'out' && arr.filter(q => q.stage === 'out').indexOf(p) > 3) return false
      if (p.stage === 'drop' && arr.filter(q => q.stage === 'drop').indexOf(p) > 3) return false
      return true
    })
    .slice(-MAX_PACKETS)
}

function toggleRun() {
  running.value = !running.value
  if (running.value) {
    timer = setInterval(tick, TICK_MS)
  } else if (timer) {
    clearInterval(timer)
    timer = null
  }
}

function reset() {
  packets.value = []
  bufferLen.value = 0
  emittedTotal.value = 0
  droppedTotal.value = 0
  backlog.value = 0
  emitAcc = 0
}

onUnmounted(() => { if (timer) clearInterval(timer) })
onMounted(() => { /* SSR-safe: no animation until user presses run */ })

const caption = computed(() =>
  bpOn.value
    ? 'Credit-based flow control: the receiver grants buffer credits upstream; the sender ships only what was credited. Producer self-throttles to the consumer — this is how backpressure propagates to the source in Flink.'
    : 'No flow control: the producer floods a bounded buffer. Once it fills, every new record is dropped — data loss instead of a slowdown.'
)

// visual slot positions inside the buffer bar
function bufX(p: P, idx: number) {
  const inBuf = packets.value.filter(q => q.stage === 'buf').indexOf(p)
  return 30 + (inBuf / CAPACITY) * 40 // % across the buffer band
}
</script>

<template>
  <div class="viz-card" data-viz="backpressure">
    <div class="viz-head">🚰 Backpressure — interactive</div>

    <div class="btns">
      <button @click="toggleRun">{{ running ? '⏸ Pause' : '▶ Run' }}</button>
      <button @click="reset">↺ Reset</button>
      <button :class="{ on: bpOn }" @click="bpOn = !bpOn">
        Backpressure {{ bpOn ? 'ON' : 'OFF' }}
      </button>
    </div>

    <div class="sliders">
      <label>Producer rate <b>{{ prodRate }}</b>/tick
        <input v-model.number="prodRate" type="range" min="1" max="8" />
      </label>
      <label>Consumer rate <b>{{ consRate }}</b>/tick
        <input v-model.number="consRate" type="range" min="1" max="8" />
      </label>
    </div>

    <div class="field">
      <div class="node producer" :class="{ throttled: bpOn && prodRate > consRate }">
        <span>producer</span>
        <span class="rate">{{ effProdRate }}/tick</span>
      </div>
      <div class="arrow" />
      <div class="buffer" :class="{ full: overflow }">
        <div class="fill" :style="{ width: fillPct + '%' }" />
        <span class="buflabel">buffer {{ bufferLen }}/{{ CAPACITY }}</span>
      </div>
      <div class="arrow" />
      <div class="node consumer">
        <span>consumer</span>
        <span class="rate">{{ consRate }}/tick</span>
      </div>
      <div
        v-for="p in packets"
        :key="p.id"
        class="pkt"
        :class="[p.stage, { dropped: p.stage === 'drop' }]"
      />
    </div>

    <div class="stats">
      <span>emitted: <b>{{ emittedTotal }}</b></span>
      <span :class="{ bad: droppedTotal > 0 }">dropped: <b>{{ droppedTotal }}</b></span>
      <span v-if="bpOn">credits left: <b>{{ credits }}</b></span>
    </div>

    <div class="caption">{{ caption }}</div>
  </div>
</template>

<style scoped>
.viz-card { border: 1px solid var(--vp-c-border); border-radius: 12px; padding: 1.1rem 1.25rem; margin: 1.5rem 0; background: var(--vp-c-bg-soft); }
.viz-head { font-weight: 600; margin-bottom: 0.5rem; }
.btns { display: flex; gap: 0.4rem; flex-wrap: wrap; margin-bottom: 0.6rem; }
.btns button { border: 1px solid var(--vp-c-border); background: var(--vp-c-bg); color: var(--vp-c-text-1); border-radius: 8px; padding: 0.25rem 0.7rem; font-size: 0.82rem; cursor: pointer; }
.btns button.on { background: var(--vp-c-brand-1); border-color: var(--vp-c-brand-1); color: var(--vp-c-bg); }
.sliders { display: flex; gap: 1.2rem; flex-wrap: wrap; font-size: 0.8rem; color: var(--vp-c-text-2); margin-bottom: 0.6rem; }
.sliders label { display: flex; align-items: center; gap: 0.4rem; }
.sliders input { width: 90px; }
.field { position: relative; display: flex; align-items: center; gap: 0.5rem; height: 90px; border-radius: 10px; background: var(--vp-c-bg); border: 1px dashed var(--vp-c-border); padding: 0 12px; overflow: hidden; }
.node { display: flex; flex-direction: column; align-items: center; justify-content: center; width: 20%; height: 60px; border-radius: 8px; border: 1.5px solid var(--vp-c-brand-1); font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.04em; color: var(--vp-c-text-1); }
.node .rate { font-weight: 700; color: var(--vp-c-brand-1); }
.node.throttled { border-color: var(--vp-c-warning-1, #d4a017); }
.node.throttled .rate { color: var(--vp-c-warning-1, #d4a017); }
.arrow { flex: 0 0 5%; border-top: 2px solid var(--vp-c-border); position: relative; }
.arrow::after { content: ''; position: absolute; right: -1px; top: -5px; border-left: 8px solid var(--vp-c-border); border-top: 4px solid transparent; border-bottom: 4px solid transparent; }
.buffer { position: relative; width: 30%; height: 60px; border-radius: 8px; border: 1.5px solid var(--vp-c-text-2); overflow: hidden; }
.buffer.full { border-color: var(--vp-c-danger-1, #e05252); box-shadow: 0 0 0 3px color-mix(in srgb, var(--vp-c-danger-1, #e05252) 25%, transparent); }
.fill { position: absolute; left: 0; top: 0; bottom: 0; background: color-mix(in srgb, var(--vp-c-brand-1) 35%, transparent); transition: width 0.25s; }
.buffer.full .fill { background: color-mix(in srgb, var(--vp-c-danger-1, #e05252) 45%, transparent); }
.buflabel { position: absolute; inset: 0; display: flex; align-items: center; justify-content: center; font-size: 0.72rem; font-weight: 600; color: var(--vp-c-text-1); }
.pkt { position: absolute; width: 7px; height: 7px; border-radius: 50%; background: var(--vp-c-brand-1); transition: all 0.3s; }
.pkt.net { top: 50%; left: 28%; transform: translateY(-50%); animation: fly 0.22s linear forwards; }
@keyframes fly { to { left: 36%; } }
.pkt.buf { top: 62%; left: 42%; }
.pkt.dropped { background: var(--vp-c-danger-1, #e05252); top: 20%; left: 42%; animation: fade 0.8s linear forwards; }
@keyframes fade { to { opacity: 0; transform: translateY(-12px); } }
.stats { display: flex; gap: 1.2rem; margin-top: 0.55rem; font-size: 0.8rem; color: var(--vp-c-text-2); }
.stats b { color: var(--vp-c-text-1); }
.stats .bad b { color: var(--vp-c-danger-1, #e05252); }
.caption { margin-top: 0.35rem; font-size: 0.85rem; color: var(--vp-c-text-2); min-height: 2.4em; }
</style>
