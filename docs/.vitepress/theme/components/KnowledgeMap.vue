<script setup lang="ts">
// Duolingo-style knowledge map — winding path of chapter nodes grouped by
// track sections; node state (new / in-progress / done) from quiz progress in
// localStorage (dek: keys); click navigates to the chapter.
import { ref, computed, onMounted, onUnmounted } from 'vue'
import { withBase } from 'vitepress'
import { nodes as graphNodes, TRACK_META, TRACK_COLORS } from '../data/graph'

// Chapter order per track = sidebar order in docs/.vitepress/config.ts
const ORDER: Record<string, string[]> = {
  intuitions: ['scaling', 'query-plans', 'shuffle-skew'],
  fundamentals: ['storage-architectures', 'file-formats', 'data-modeling', 'normalization'],
  pipelines: ['batch-vs-stream', 'lambda-kappa', 'etl-elt-cdc'],
  sql: ['advanced-queries', 'query-performance', 'analytics-patterns'],
  python: ['advanced-python', 'data-engineering'],
  spark: ['architecture', 'execution', 'optimization', 'advanced-algorithms'],
  streaming: ['kafka', 'flink-beam', 'windows-watermarks'],
  'system-design': ['data-intensive', 'pipeline-design', 'ml-pipelines', 'genai-engineering', 'ml-systems', 'platform-designs'],
  cloud: ['big-three', 'lakehouse-platforms', 'cost-architecture', 'platform-deep-dive'],
  dataops: ['orchestration', 'quality', 'dbt'],
  interview: ['playbook', 'sql-coding', 'system-design', 'rapid-fire', 'databricks-practice']
}

const norm = (s: string) => s.replace(/\.html$/, '').replace(/\/$/, '')
const pretty = (link: string) =>
  link.split('/').pop()!.split('-').map(w => w[0].toUpperCase() + w.slice(1)).join(' ')

// Layout: 560px-wide fixed canvas (horizontal scroll on mobile), nodes zigzag.
const W = 560
const ROW = 78
const TOP = 46
const BOT = 96 // leaves room for a below-node tooltip on the last row
const ZIG = [0, -60, -112, -60, 0, 60, 112, 60]

interface Chap { link: string; key: string; label: string; blurb: string; x: number; y: number }
const chapterNodes = graphNodes.filter(n => n.kind === 'chapter')
const byLink = new Map(chapterNodes.map(n => [norm(n.link), n]))

const sections = TRACK_META.map(t => {
  const links = (ORDER[t.slug] ?? []).map(s => `/${t.slug}/${s}`)
  for (const n of chapterNodes)
    if (n.track === t.slug && !links.some(l => norm(l) === norm(n.link))) links.push(n.link)
  const chaps: Chap[] = links.map((link, i) => ({
    link,
    key: norm(link),
    label: byLink.get(norm(link))?.label ?? pretty(link),
    blurb: byLink.get(norm(link))?.blurb ?? '',
    x: W / 2 + ZIG[i % ZIG.length],
    y: TOP + i * ROW
  }))
  const h = TOP + Math.max(0, chaps.length - 1) * ROW + BOT
  const d = chaps.map((c, i) => {
    if (i === 0) return `M ${c.x} ${c.y}`
    const p = chaps[i - 1]
    const my = (p.y + c.y) / 2
    return `C ${p.x} ${my} ${c.x} ${my} ${c.x} ${c.y}`
  }).join(' ')
  return { ...t, color: TRACK_COLORS[t.slug] ?? '#888', chaps, h, d }
})

// --- progress (localStorage dek:<path>[#N] → aggregated per page) ---
interface P { answered: number; correct: number; total: number }
const prog = ref(new Map<string, P>())
function load() {
  if (typeof localStorage === 'undefined') return
  const m = new Map<string, P>()
  for (let i = 0; i < localStorage.length; i++) {
    const k = localStorage.key(i)
    if (!k?.startsWith('dek:')) continue
    try {
      const p = JSON.parse(localStorage.getItem(k)!) as P
      const base = norm(k.slice(4).replace(/#\d+$/, ''))
      const prev = m.get(base)
      m.set(base, prev
        ? { answered: prev.answered + p.answered, correct: prev.correct + p.correct, total: prev.total + p.total }
        : { answered: p.answered, correct: p.correct, total: p.total })
    } catch { /* skip malformed */ }
  }
  prog.value = m
}

type State = 'new' | 'doing' | 'done'
const stateOf = (c: Chap): State => {
  const p = prog.value.get(c.key)
  if (!p) return 'new'
  if (p.total > 0 && p.correct >= p.total / 2) return 'done'
  return p.answered > 0 ? 'doing' : 'new'
}
const STATE_LABEL: Record<State, string> = { done: '✓ Done', doing: '◐ In progress', new: '○ New' }

const allChaps = sections.flatMap(s => s.chaps)
const stats = computed(() => {
  const done = allChaps.filter(c => stateOf(c) === 'done').length
  return { done, total: allChaps.length, pct: allChaps.length ? Math.round((done / allChaps.length) * 100) : 0 }
})
const trackPct = (s: (typeof sections)[number]) =>
  s.chaps.length ? Math.round((s.chaps.filter(c => stateOf(c) === 'done').length / s.chaps.length) * 100) : 0

// --- current page + tooltip ---
const here = ref('')
const tip = ref<{ sec: string; left: number; top: number; above: boolean; title: string; blurb: string; state: string } | null>(null)
function showTip(sec: string, c: Chap) {
  const above = c.y >= 120
  tip.value = {
    sec,
    left: Math.min(Math.max(c.x, 116), W - 116),
    top: above ? c.y - 30 : c.y + 30,
    above,
    title: c.label,
    blurb: c.blurb,
    state: STATE_LABEL[stateOf(c)]
  }
}
const hideTip = () => { tip.value = null }

let onVis: () => void
onMounted(() => {
  here.value = norm(location.pathname)
  load()
  onVis = () => { if (!document.hidden) load() }
  document.addEventListener('visibilitychange', onVis)
  window.addEventListener('focus', load)
})
onUnmounted(() => {
  document.removeEventListener('visibilitychange', onVis)
  window.removeEventListener('focus', load)
})
</script>

<template>
  <div class="kmap" data-viz="kmap">
    <div class="kmap-top">
      <div class="bar"><i :style="{ width: stats.pct + '%' }" /></div>
      <span class="meta"><b>{{ stats.pct }}%</b> · {{ stats.done }}/{{ stats.total }} chapters done</span>
      <span class="legend">
        <span><i class="dot done" /> Done</span>
        <span><i class="dot doing" /> In progress</span>
        <span><i class="dot new" /> New</span>
      </span>
    </div>

    <div class="kmap-scroll">
      <div class="kmap-inner">
        <section v-for="s in sections" :key="s.slug" class="sec">
          <div class="pill" :style="{ borderColor: s.color, background: s.color + '22' }">
            <span aria-hidden="true">{{ s.icon }}</span>
            <b>{{ s.label }}</b>
            <em :style="{ color: s.color }">{{ trackPct(s) }}%</em>
          </div>
          <div v-if="s.chaps.length" class="trail" :style="{ height: s.h + 'px' }">
            <svg class="path" :viewBox="`0 0 ${W} ${s.h}`" :width="W" :height="s.h" aria-hidden="true">
              <path :d="s.d" fill="none" :stroke="s.color" stroke-width="6" stroke-linecap="round" opacity="0.4" />
            </svg>
            <a
              v-for="c in s.chaps"
              :key="c.key"
              :href="withBase(c.link)"
              class="node"
              :class="[stateOf(c), { here: here === c.key }]"
              :style="{ left: c.x - 24 + 'px', top: c.y - 24 + 'px', '--tc': s.color }"
              @mouseenter="showTip(s.slug, c)"
              @mouseleave="hideTip"
              @focus="showTip(s.slug, c)"
              @blur="hideTip"
            ><span>{{ stateOf(c) === 'done' ? '✓' : c.label.charAt(0) }}</span></a>
            <div
              v-if="tip && tip.sec === s.slug"
              class="tip"
              :class="{ below: !tip.above }"
              :style="{ left: tip.left + 'px', top: tip.top + 'px' }"
            >
              <b>{{ tip.title }}</b>
              <p v-if="tip.blurb">{{ tip.blurb }}</p>
              <em>{{ tip.state }}</em>
            </div>
          </div>
        </section>
      </div>
    </div>

    <p class="note">Progress is stored locally in your browser — nothing leaves your machine.</p>
  </div>
</template>

<style scoped>
.kmap { margin: 1.5rem 0; }
.kmap-top { display: flex; flex-wrap: wrap; align-items: center; gap: 0.5rem 1rem; margin-bottom: 1rem; }
.bar { flex: 1 1 160px; height: 10px; border-radius: 999px; background: var(--vp-c-bg-soft); border: 1px solid var(--vp-c-border); overflow: hidden; }
.bar i { display: block; height: 100%; background: linear-gradient(90deg, var(--vp-c-brand-1), var(--vp-c-green-1)); transition: width 0.4s; }
.meta { font-size: 0.85rem; color: var(--vp-c-text-2); white-space: nowrap; }
.legend { display: flex; gap: 0.9rem; font-size: 0.78rem; color: var(--vp-c-text-2); }
.legend span { display: inline-flex; align-items: center; }
.dot { display: inline-block; width: 12px; height: 12px; border-radius: 50%; margin-right: 5px; }
.dot.done { background: var(--vp-c-brand-1); }
.dot.doing { background: conic-gradient(var(--vp-c-brand-1) 50%, var(--vp-c-border) 0); }
.dot.new { border: 2px dashed var(--vp-c-brand-1); box-sizing: border-box; }

.kmap-scroll { overflow-x: auto; border: 1px solid var(--vp-c-border); border-radius: 14px; background: var(--vp-c-bg-soft); }
.kmap-inner { width: 560px; margin: 0 auto; padding: 0.75rem 0 0.5rem; }
.sec { padding: 0.4rem 12px 0.25rem; }
.pill { display: inline-flex; align-items: center; gap: 0.45rem; padding: 0.28rem 0.9rem; border-radius: 999px; border: 1.5px solid; font-size: 0.9rem; color: var(--vp-c-text-1); }
.pill em { font-style: normal; font-weight: 700; }

.trail { position: relative; }
.path { position: absolute; inset: 0; }
.node {
  position: absolute; width: 48px; height: 48px; border-radius: 50%; z-index: 2;
  display: grid; place-items: center; font-weight: 700; font-size: 1.05rem; text-decoration: none;
  color: var(--tc); background: var(--vp-c-bg); border: 3px dashed var(--tc);
  transition: transform 0.15s;
}
.node:hover { transform: scale(1.12); }
.node.doing { border-style: solid; background: conic-gradient(var(--tc) 0 50%, var(--vp-c-bg-soft) 50%); }
.node.doing::before { content: ''; position: absolute; inset: 5px; border-radius: 50%; background: var(--vp-c-bg); }
.node.done { background: var(--tc); border-style: solid; color: #fff; }
.node.here { box-shadow: 0 0 0 4px var(--vp-c-bg-soft), 0 0 0 7px var(--tc); }

.tip {
  position: absolute; transform: translate(-50%, -100%); width: 232px; z-index: 5;
  background: var(--vp-c-bg); border: 1px solid var(--vp-c-border); border-radius: 10px;
  padding: 0.55rem 0.7rem; font-size: 0.78rem; line-height: 1.4; color: var(--vp-c-text-2);
  box-shadow: 0 6px 20px rgba(0, 0, 0, 0.14); pointer-events: none;
}
.node span { position: relative; z-index: 1; }
.tip b { display: block; color: var(--vp-c-text-1); font-size: 0.85rem; }
.tip p { margin: 0.2rem 0; }
.tip em { font-style: normal; font-weight: 600; color: var(--vp-c-brand-1); }
.note { margin: 0.75rem 0 0; font-size: 0.78rem; color: var(--vp-c-text-3); }
</style>
