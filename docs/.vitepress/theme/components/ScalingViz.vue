<script setup lang="ts">
import { ref, computed } from 'vue'

// 3b1b-style: one machine that grows vs many small machines that share.
const mode = ref<'up' | 'out'>('up')
const level = ref(1)
const dead = ref(false)

const CEIL = 4 // vertical upgrade steps, then hardware ceiling
const MAXN = 6 // max fleet size

const nodes = computed(() => (mode.value === 'up' ? 1 : level.value))
const alive = computed(() => (dead.value ? nodes.value - 1 : nodes.value))
const atCeil = computed(() => mode.value === 'up' && level.value >= CEIL)

// diminishing throughput per vertical step: 1x, 1.8x, 2.4x, 2.7x
const gain = [1, 1.8, 2.4, 2.7]
const share = computed(() => (alive.value > 0 ? Math.round(100 / alive.value) : 0))

function pick(m: 'up' | 'out') {
  mode.value = m
  dead.value = false
}
function grow() {
  const cap = mode.value === 'up' ? CEIL : MAXN
  if (level.value < cap) level.value++
}
function shrink() {
  if (level.value > 1) level.value--
  dead.value = false
}
function kill() {
  dead.value = !dead.value
}

const caption = computed(() => {
  if (mode.value === 'up') {
    if (dead.value) return 'The one machine is down. Total outage — there is nothing to fail over to.'
    if (atCeil.value)
      return `Hardware ceiling hit: ${gain[level.value - 1]}x throughput, and each upgrade bought less than the last.`
    return `One machine, ${gain[level.value - 1]}x throughput. Bigger box, same story — and a single point of failure.`
  }
  if (dead.value)
    return alive.value > 0
      ? `A node died. The other ${alive.value} absorbed its share — ${share.value}% each. Slower, not down.`
      : 'A fleet of one is still a single point of failure.'
  return `${nodes.value} nodes, ${share.value}% of the load each. Add another cheap box for more headroom.`
})
</script>

<template>
  <div class="viz-card" data-viz="scaling">
    <div class="viz-head">⚖️ Scale Up vs Scale Out — interactive</div>

    <div class="btns">
      <button :class="{ on: mode === 'up' }" @click="pick('up')">Scale up</button>
      <button :class="{ on: mode === 'out' }" @click="pick('out')">Scale out</button>
      <span class="sp" />
      <button @click="shrink">−</button>
      <button @click="grow" :disabled="mode === 'up' ? level >= CEIL : level >= MAXN">+</button>
      <span class="sp" />
      <button class="kill" @click="kill">{{ dead ? 'Restart node' : 'Kill a node' }}</button>
    </div>

    <div class="stage" :class="mode">
      <div v-if="mode === 'up'" class="ceil-label" v-show="atCeil">⛔ hardware ceiling</div>
      <div
        v-for="n in nodes"
        :key="n"
        class="machine"
        :class="{ dead: dead && n === 1, big: mode === 'up' }"
        :style="mode === 'up' ? { height: 30 + level * 22 + 'px' } : {}"
      >
        <div class="fill" :style="{ width: dead && n === 1 ? '0%' : share + '%' }" />
        <span class="tag">{{ dead && n === 1 ? '✕ down' : share + '%' }}</span>
      </div>
      <div v-if="dead && mode === 'up'" class="outage">TOTAL OUTAGE</div>
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
  align-items: center;
  flex-wrap: wrap;
  margin-bottom: 0.8rem;
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
.btns button:disabled {
  opacity: 0.35;
  cursor: default;
}
.btns .kill {
  border-color: var(--vp-c-danger-1, #e05252);
}
.sp {
  flex: 0 0 0.5rem;
}
.stage {
  position: relative;
  display: flex;
  gap: 0.6rem;
  align-items: flex-end;
  min-height: 140px;
  padding: 0.8rem;
  border-radius: 10px;
  background: var(--vp-c-bg);
  border: 1px dashed var(--vp-c-border);
  flex-wrap: wrap;
}
.machine {
  position: relative;
  width: 64px;
  height: 44px;
  border: 2px solid var(--vp-c-brand-1);
  border-radius: 8px;
  overflow: hidden;
  transition: all 0.55s cubic-bezier(0.4, 0, 0.2, 1);
  background: var(--vp-c-bg-soft);
}
.machine.big {
  width: 110px;
}
.fill {
  position: absolute;
  left: 0;
  top: 0;
  bottom: 0;
  background: var(--vp-c-brand-1);
  opacity: 0.55;
  transition: width 0.6s cubic-bezier(0.4, 0, 0.2, 1);
}
.tag {
  position: absolute;
  inset: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 0.72rem;
  font-weight: 600;
  color: var(--vp-c-text-1);
}
.machine.dead {
  border-color: var(--vp-c-danger-1, #e05252);
  animation: flash 0.9s ease;
}
@keyframes flash {
  0%, 100% { background: var(--vp-c-bg-soft); }
  30% { background: var(--vp-c-danger-1, #e05252); }
}
.ceil-label {
  position: absolute;
  top: -12px;
  left: 50%;
  transform: translateX(-50%);
  font-size: 0.7rem;
  color: var(--vp-c-danger-1, #e05252);
  background: var(--vp-c-bg);
  padding: 0 0.4rem;
  border-radius: 6px;
  border: 1px solid var(--vp-c-danger-1, #e05252);
}
.outage {
  align-self: center;
  font-weight: 700;
  font-size: 0.85rem;
  color: var(--vp-c-danger-1, #e05252);
  animation: flash 1.2s ease;
}
.caption {
  margin-top: 0.6rem;
  font-size: 0.85rem;
  color: var(--vp-c-text-2);
  min-height: 1.3em;
}
</style>
