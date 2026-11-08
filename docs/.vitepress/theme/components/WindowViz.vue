<script setup lang="ts">
import { ref, computed } from 'vue'

// Watermark playthrough: advance the watermark step by step over arriving
// events; tumbling windows fire when WM passes window end, refire on late
// events inside allowed lateness, and side-output beyond the lateness horizon.

const BOUND = 3 // out-of-orderness bound (min): WM = max event time seen - 3
const LAT = 5   // allowed lateness (min) beyond window end

interface Ev { et: number; at: number } // event time / arrival time, minutes since 10:00
const EVENTS: Ev[] = [
  { et: 1,  at: 1  }, { et: 3,  at: 3  }, { et: 5,  at: 5  },
  { et: 7,  at: 8  }, { et: 11, at: 11 }, { et: 13, at: 13 },
  { et: 5,  at: 15 }, // late: W0 already fired, inside lateness horizon -> refire
  { et: 16, at: 17 }, { et: 19, at: 19 },
  { et: 2,  at: 20 }, // very late: W0 purged -> side output
]
const N = EVENTS.length
const WINS = [[0, 10], [10, 20]] // tumbling windows in minutes since 10:00

const step = ref(0)
const lateOn = ref(true)
const mode = ref<'accumulating' | 'retracting'>('accumulating')

interface Row { step: number; label: string; kind: 'fire' | 'refire' | 'retract' | 'late' | 'purge' }

const wmAt = (k: number) => (k === 0 ? 0 : Math.max(...EVENTS.slice(0, k).map(e => e.et)) - BOUND)

// Recompute the whole trace up to the current step: which events are in,
// what each window emitted, what hit the side output.
const state = computed(() => {
  const evState = EVENTS.map(() => 'pending' as 'pending' | 'on-time' | 'late' | 'side')
  const fired = WINS.map(() => false)
  const purged = WINS.map(() => false)
  const panes = WINS.map(() => 0)
  const sums = WINS.map(() => 0)
  const rows: Row[] = []
  let wm = 0
  for (let k = 1; k <= step.value; k++) {
    const e = EVENTS[k - 1]
    const w = e.et < 10 ? 0 : 1
    if (purged[w] || (fired[w] && !lateOn.value)) {
      evState[k - 1] = 'side'
      rows.push({ step: k, label: `[10:0${w},10:${w + 1}0) event t=10:${pad(e.et)} -> side output`, kind: 'late' })
    } else if (fired[w]) {
      evState[k - 1] = 'late'
      panes[w]++
      if (mode.value === 'retracting')
        rows.push({ step: k, label: `[10:0${w},10:${w + 1}0) retract -${sums[w]}`, kind: 'retract' })
      sums[w] += e.et
      rows.push({ step: k, label: `[10:0${w},10:${w + 1}0) v${panes[w]} = ${sums[w]} (refire)`, kind: 'refire' })
    } else {
      evState[k - 1] = 'on-time'
      sums[w] += e.et
    }
    wm = wmAt(k)
    WINS.forEach((win, i) => {
      const lim = win[1] + (lateOn.value ? LAT : 0)
      if (!fired[i] && wm >= win[1] && wm < lim) {
        fired[i] = true
        panes[i]++
        rows.push({ step: k, label: `[10:0${i},10:${i + 1}0) v1 = ${sums[i]} (fires: WM 10:${pad(wm)})`, kind: 'fire' })
      }
      if (fired[i] && !purged[i] && wm >= lim) {
        purged[i] = true
        rows.push({ step: k, label: `[10:0${i},10:${i + 1}0) state purged (WM >= end + ${LAT}m lateness)`, kind: 'purge' })
      }
    })
  }
  return { rows, evState, fired, purged, panes, wm }
})

const pad = (n: number) => String(n).padStart(2, '0')
const x = (m: number) => 20 + (m / 24) * 560 // timeline x for minute m (0..24)

const flash = ref(0)
function advance() { if (step.value < N) { step.value++; flash.value++ } }
function reset() { step.value = 0 }

</script>

<template>
  <div class="viz-card" data-viz="window">
    <div class="viz-head">🪟 Windows &amp; Watermarks — interactive</div>
    <div class="controls">
      <button @click="advance" :disabled="step >= N">Step arrival {{ step < N ? `${step + 1}/${N}` : '— done' }}</button>
      <input type="range" min="0" :max="N" v-model.number="step" aria-label="Arrival step" />
      <button @click="reset">Reset</button>
      <label><input type="checkbox" v-model="lateOn" /> allowed lateness {{ LAT }}m</label>
      <label><input type="checkbox" :checked="mode === 'retracting'" @change="mode = mode === 'retracting' ? 'accumulating' : 'retracting'" /> retractions</label>
    </div>

    <svg viewBox="0 0 600 150" class="tl" role="img" aria-label="Event-time timeline with watermark">
      <!-- window bands -->
      <g v-for="(w, i) in WINS" :key="i">
        <rect :x="x(w[0])" y="34" :width="x(w[1]) - x(w[0])" height="26"
              :class="['band', { fired: state.fired[i], purged: state.purged[i] }]" />
        <text :x="(x(w[0]) + x(w[1])) / 2" y="50" class="wlabel">
          [10:0{{ i }},10:{{ i + 1 }}0){{ state.purged[i] ? ' — purged' : state.fired[i] ? ' — fired' : '' }}
        </text>
        <line v-if="lateOn" :x1="x(w[1])" :x2="x(w[1] + LAT)" y1="34" y2="34" class="horizon" />
      </g>
      <!-- axis -->
      <line :x1="x(0)" :x2="x(24)" y1="66" y2="66" class="axis" />
      <text v-for="t in [0, 10, 20]" :key="t" :x="x(t)" y="78" class="tick">10:{{ pad(t) }}</text>
      <!-- events: event time on axis, arrival time below -->
      <g v-for="(e, i) in EVENTS" :key="'e' + i">
        <template v-if="i < step">
          <line :x1="x(e.at)" :x2="x(e.et)" y1="118" y2="90" class="skewline" />
          <circle :cx="x(e.et)" cy="90" r="5" :class="['ev', state.evState[i]]" />
          <circle :cx="x(e.at)" cy="118" r="3" class="arr" />
          <text :x="x(e.et)" y="104" class="evlab">{{ e.et < 10 ? '0' + e.et : e.et }}</text>
        </template>
      </g>
      <text :x="x(0)" y="94" class="axislab" text-anchor="end">event time</text>
      <text :x="x(0)" y="122" class="axislab" text-anchor="end">arrival</text>
      <!-- watermark -->
      <line :x1="x(state.wm)" :x2="x(state.wm)" y1="28" y2="128" class="wm" :key="flash" />
      <text :x="x(state.wm)" y="22" class="wmlab">WM 10:{{ pad(state.wm) }}</text>
    </svg>

    <table class="out">
      <thead><tr><th>step</th><th>emitted</th></tr></thead>
      <tbody>
        <tr v-for="(r, i) in state.rows" :key="i" :class="r.kind">
          <td>{{ r.step }}</td><td>{{ r.label }}</td>
        </tr>
        <tr v-if="!state.rows.length"><td colspan="2" class="dim">press Step — nothing emitted yet</td></tr>
      </tbody>
    </table>
  </div>
</template>

<style scoped>
.viz-card { border: 1px solid var(--vp-c-border); border-radius: 12px; padding: 1.1rem 1.25rem; margin: 1.5rem 0; background: var(--vp-c-bg-soft); }
.viz-head { font-weight: 600; margin-bottom: 0.6rem; }
.controls { display: flex; flex-wrap: wrap; gap: 0.6rem 1rem; align-items: center; font-size: 0.85rem; margin-bottom: 0.6rem; }
.controls input[type='range'] { flex: 1 1 160px; accent-color: var(--vp-c-brand-1); }
.controls button { border: 1px solid var(--vp-c-border); border-radius: 6px; padding: 0.2rem 0.7rem; background: var(--vp-c-bg); color: var(--vp-c-text-1); cursor: pointer; }
.controls button:disabled { opacity: 0.5; cursor: default; }
.tl { width: 100%; height: auto; display: block; }
.band { fill: var(--vp-c-bg); stroke: var(--vp-c-border); }
.band.fired { fill: var(--vp-c-brand-soft); stroke: var(--vp-c-brand-1); }
.band.purged { opacity: 0.4; stroke-dasharray: 4 3; }
.horizon { stroke: var(--vp-c-warning, #b58900); stroke-dasharray: 3 3; stroke-width: 2; }
.axis { stroke: var(--vp-c-text-3); }
.tick { fill: var(--vp-c-text-3); font-size: 10px; text-anchor: middle; }
.wlabel { fill: var(--vp-c-text-2); font-size: 10px; text-anchor: middle; }
.axislab { fill: var(--vp-c-text-3); font-size: 9px; }
.evlab { fill: var(--vp-c-text-3); font-size: 9px; text-anchor: middle; }
.skewline { stroke: var(--vp-c-text-3); stroke-dasharray: 2 2; opacity: 0.6; }
.arr { fill: var(--vp-c-text-3); }
.ev { fill: var(--vp-c-brand-1); }
.ev.late { fill: var(--vp-c-warning, #b58900); }
.ev.side { fill: var(--vp-c-danger-1, #c0392b); }
.wm { stroke: var(--vp-c-danger-1, #c0392b); stroke-width: 2; animation: wmpop 0.5s; }
.wmlab { fill: var(--vp-c-danger-1, #c0392b); font-size: 10px; font-weight: 600; }
@keyframes wmpop { from { opacity: 0.2; } to { opacity: 1; } }
.out { width: 100%; border-collapse: collapse; font-size: 0.8rem; margin-top: 0.4rem; }
.out th, .out td { text-align: left; padding: 0.15rem 0.5rem; border-bottom: 1px solid var(--vp-c-divider); }
.out th { color: var(--vp-c-text-2); font-weight: 600; }
.out .fire td { color: var(--vp-c-brand-1); font-weight: 600; }
.out .refire td { color: var(--vp-c-warning, #b58900); }
.out .retract td, .out .late td { color: var(--vp-c-danger-1, #c0392b); }
.out .dim { color: var(--vp-c-text-3); }
</style>
