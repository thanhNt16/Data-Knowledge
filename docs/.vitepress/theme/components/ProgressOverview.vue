<script setup lang="ts">
import { ref, onMounted } from 'vue'

interface ChapterProgress {
  answered: number
  correct: number
  total: number
  at: number
}

const chapters = ref<{ path: string; p: ChapterProgress }[]>([])

onMounted(() => {
  // aggregate per-card keys (path#N) into one row per page
  const byPage = new Map<string, ChapterProgress>()
  for (let i = 0; i < localStorage.length; i++) {
    const k = localStorage.key(i)
    if (!k || !k.startsWith('dek:')) continue
    const base = k.slice(4).replace(/#\d+$/, '')
    try {
      const p = JSON.parse(localStorage.getItem(k)!) as ChapterProgress
      const prev = byPage.get(base)
      byPage.set(base, prev
        ? { answered: prev.answered + p.answered, correct: prev.correct + p.correct, total: prev.total + p.total, at: Math.max(prev.at, p.at) }
        : p)
    } catch {
      /* skip malformed */
    }
  }
  const out = [...byPage.entries()].map(([path, p]) => ({ path, p }))
  out.sort((a, b) => b.p.at - a.p.at)
  chapters.value = out
})

const totals = (list: { p: ChapterProgress }[]) =>
  list.reduce(
    (acc, { p }) => ({ answered: acc.answered + p.answered, correct: acc.correct + p.correct, total: acc.total + p.total }),
    { answered: 0, correct: 0, total: 0 }
  )

const pretty = (path: string) =>
  decodeURIComponent(path)
    .replace(/^\//, '')
    .replace(/\.html$/, '')
    .replace(/\//g, ' › ')
</script>

<template>
  <div class="progress-overview">
    <h2>📈 Your progress</h2>
    <p v-if="!chapters.length" class="muted">
      No quiz activity yet. Answer quiz questions in any chapter and your progress shows up here (stored locally in your browser).
    </p>
    <template v-else>
      <div class="agg">
        <div class="agg-num">
          <strong>{{ totals(chapters).correct }}</strong>
          <span>correct of {{ totals(chapters).answered }} answered ({{ totals(chapters).total }} questions site-wide)</span>
        </div>
      </div>
      <ul class="chap-list">
        <li v-for="c in chapters" :key="c.path">
          <a :href="c.path">{{ pretty(c.path) }}</a>
          <div class="bar">
            <div class="fill" :style="{ width: (c.p.total ? (c.p.correct / c.p.total) * 100 : 0) + '%' }" />
          </div>
          <span class="score">{{ c.p.correct }}/{{ c.p.total }}</span>
        </li>
      </ul>
    </template>
  </div>
</template>

<style scoped>
.progress-overview { margin: 2.5rem 0; }
.muted { color: var(--vp-c-text-2); font-size: 0.92rem; }
.agg { margin: 0.75rem 0 1.25rem; }
.agg-num strong { font-size: 1.6rem; margin-right: 0.5rem; color: var(--vp-c-brand-1); }
.agg-num span { color: var(--vp-c-text-2); font-size: 0.9rem; }
.chap-list { list-style: none; padding: 0; display: grid; gap: 0.6rem; }
.chap-list li { display: grid; grid-template-columns: minmax(140px, 1fr) 2fr auto; gap: 0.75rem; align-items: center; }
.chap-list a { font-size: 0.85rem; color: var(--vp-c-brand-1); text-decoration: none; }
.bar { height: 8px; border-radius: 999px; background: var(--vp-c-bg-soft); overflow: hidden; border: 1px solid var(--vp-c-border); }
.fill { height: 100%; background: linear-gradient(90deg, var(--vp-c-brand-1), var(--vp-c-green-1)); }
.score { font-size: 0.8rem; color: var(--vp-c-text-2); font-variant-numeric: tabular-nums; }
</style>
