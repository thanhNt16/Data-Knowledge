<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'

// 3b1b-style: 24 rows, hashed to 4 reducers. Skew piles one key onto reducer 0;
// salting spreads the hot key back out. Deterministic assignments, no random.

type Mode = 'even' | 'skew' | 'salt'
const mode = ref<Mode>('even')
const sent = ref(false)
onMounted(() => {
  // let SSR/first paint place dots on the mappers, then release the shuffle
  sent.value = true
})

const N = 24
const HOT = 14 // rows 0..13 share the hot key (~58% of the data)
const NR = 4

const isHot = (i: number) => i < HOT

function reducerFor(i: number, m: Mode): number {
  if (m === 'skew') return isHot(i) ? 0 : 1 + ((i - HOT) % (NR - 1))
  return i % NR // even hashing / salted hot key spread round-robin
}

// slot within the target reducer = arrival order, precomputed deterministically
function slotFor(i: number, m: Mode): number {
  let s = 0
  for (let j = 0; j < i; j++) if (reducerFor(j, m) === reducerFor(i, m)) s++
  return s
}

const counts = computed(() => {
  const c = [0, 0, 0, 0]
  for (let i = 0; i < N; i++) c[reducerFor(i, mode.value)]++
  return c
})
const maxCount = computed(() => Math.max(...counts.value))

function dotStyle(i: number) {
  if (!sent.value) {
    const m = Math.floor(i / 6)
    const s = i % 6
    return { left: `calc(3% + ${(s % 3) * 10}px)`, top: `calc(${18 + m * 25}% + ${Math.floor(s / 3) * 10}px)` }
  }
  const r = reducerFor(i, mode.value)
  const s = slotFor(i, mode.value)
  return { left: `calc(76% + ${(s % 6) * 11}px)`, top: `calc(${18 + r * 25}% + ${Math.floor(s / 6) * 10}px)` }
}

const caption = computed(() => {
  if (mode.value === 'even')
    return 'Hash(key) mod 4 — every reducer gets ~6 rows. All four finish together.'
  if (mode.value === 'skew')
    return `One popular key owns ${HOT} of ${N} rows. Its reducer is a straggler — the job waits for the slowest task.`
  return 'Salt the hot key: append a random bucket id, spread it across all reducers, then roll the partial results back together. Timers equalize.'
})
</script>

<template>
  <div class="viz-card" data-viz="shuffle">
    <div class="viz-head">🔀 The Shuffle — interactive</div>

    <div class="btns">
      <button :class="{ on: mode === 'even' }" @click="mode = 'even'">Balanced keys</button>
      <button :class="{ on: mode === 'skew' }" @click="mode = 'skew'">Skewed key</button>
      <button :class="{ on: mode === 'salt' }" @click="mode = 'salt'">+ Salt</button>
    </div>

    <div class="field">
      <div class="lane-label" style="left: 3%">mappers</div>
      <div class="lane-label" style="left: 76%">reducers</div>

      <div v-for="m in 4" :key="'m' + m" class="mapper" :style="{ top: 18 + (m - 1) * 25 + '%' }" />

      <div
        v-for="r in 4"
        :key="'r' + r + mode"
        class="reducer"
        :class="{ hot: mode === 'skew' && r - 1 === 0 }"
        :style="{ top: 18 + (r - 1) * 25 + '%' }"
      >
        <span class="rcount">{{ counts[r - 1] }}</span>
        <div
          class="timer"
          :style="{ animationDuration: counts[r - 1] * 0.35 + 's' }"
        />
      </div>

      <div
        v-for="i in N"
        :key="i"
        class="dot"
        :class="{ hotrow: isHot(i - 1) }"
        :style="dotStyle(i - 1)"
      />
    </div>

    <div class="jobtime">
      job ends when the slowest reducer ends ≈ <b>{{ (maxCount * 0.35).toFixed(1) }}s</b>
    </div>

    <div class="caption">{{ caption }}</div>
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
.btns {
  display: flex;
  gap: 0.4rem;
  flex-wrap: wrap;
  margin-bottom: 0.7rem;
}
.btns button {
  border: 1px solid var(--vp-c-border);
  background: var(--vp-c-bg);
  color: var(--vp-c-text-1);
  border-radius: 8px;
  padding: 0.25rem 0.7rem;
  font-size: 0.82rem;
  cursor: pointer;
  transition: all 0.2s;
}
.btns button.on {
  background: var(--vp-c-brand-1);
  border-color: var(--vp-c-brand-1);
  color: var(--vp-c-bg);
}
.field {
  position: relative;
  height: 230px;
  border-radius: 10px;
  background: var(--vp-c-bg);
  border: 1px dashed var(--vp-c-border);
  overflow: visible;
}
.lane-label {
  position: absolute;
  top: 4px;
  font-size: 0.68rem;
  color: var(--vp-c-text-2);
  text-transform: uppercase;
  letter-spacing: 0.05em;
}
.mapper {
  position: absolute;
  left: 2%;
  width: 14%;
  height: 20%;
  border-radius: 8px;
  border: 1.5px solid var(--vp-c-text-2);
  opacity: 0.35;
}
.reducer {
  position: absolute;
  left: 74%;
  width: 24%;
  height: 20%;
  border-radius: 8px;
  border: 1.5px solid var(--vp-c-brand-1);
  transition: all 0.4s;
}
.reducer.hot {
  border-color: var(--vp-c-danger-1, #e05252);
  box-shadow: 0 0 0 3px color-mix(in srgb, var(--vp-c-danger-1, #e05252) 25%, transparent);
}
.rcount {
  position: absolute;
  right: 4px;
  top: 2px;
  font-size: 0.72rem;
  font-weight: 700;
  color: var(--vp-c-text-1);
}
.timer {
  position: absolute;
  left: 4px;
  bottom: 4px;
  height: 4px;
  border-radius: 2px;
  background: var(--vp-c-brand-1);
  animation: run linear forwards;
  animation-duration: 2s;
}
.reducer.hot .timer {
  background: var(--vp-c-danger-1, #e05252);
}
@keyframes run {
  from { width: 0; }
  to { width: calc(100% - 8px); }
}
.dot {
  position: absolute;
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: var(--vp-c-brand-1);
  transition: left 0.9s cubic-bezier(0.4, 0, 0.2, 1), top 0.9s cubic-bezier(0.4, 0, 0.2, 1);
  z-index: 2;
}
.dot.hotrow {
  background: var(--vp-c-warning-1, #d4a017);
}
.jobtime {
  margin-top: 0.55rem;
  font-size: 0.8rem;
  color: var(--vp-c-text-2);
}
.jobtime b {
  color: var(--vp-c-text-1);
}
.caption {
  margin-top: 0.35rem;
  font-size: 0.85rem;
  color: var(--vp-c-text-2);
  min-height: 2.4em;
}
</style>
