<script setup lang="ts">
import { reactive, computed } from 'vue'
import { useData } from 'vitepress'

const props = defineProps<{
  questions: { q: string; options: string[]; correct: number; explain?: string }[]
}>()
// ponytail: per-card suffix; ProgressOverview aggregates by base path
let cardSeq = 0
const cardId = cardSeq++
const pageKey = () =>
  'dek:' + (typeof location !== 'undefined' ? location.pathname : (page.value.path ?? '')) + (cardId > 0 ? `#${cardId}` : '')
const { page } = useData()


const state = reactive({
  picked: props.questions.map(() => -1),
  done: props.questions.map(() => false)
})

const correctCount = computed(() => state.done.filter((d, i) => d && state.picked[i] === props.questions[i].correct).length)
const answeredCount = computed(() => state.done.filter(Boolean).length)

function answer(qi: number, oi: number) {
  if (state.done[qi]) return
  state.picked[qi] = oi
  state.done[qi] = true
  persist()
}

function persist() {
  try {
    localStorage.setItem(
      pageKey(),
      JSON.stringify({
        answered: answeredCount.value,
        correct: correctCount.value,
        total: props.questions.length,
        at: Date.now()
      })
    )
  } catch {
    /* private mode / SSR guard */
  }
}

function reset() {
  state.picked = props.questions.map(() => -1)
  state.done = props.questions.map(() => false)
  try {
    localStorage.removeItem(pageKey())
  } catch {
    /* ignore */
  }
}
</script>

<template>
  <div class="quiz-card">
    <div class="quiz-head">
      <span class="quiz-badge">🧠 Check yourself</span>
      <span v-if="answeredCount" class="quiz-score">{{ correctCount }} / {{ props.questions.length }} correct</span>
    </div>
    <div v-for="(question, qi) in props.questions" :key="qi" class="quiz-q">
      <p class="quiz-text"><strong>Q{{ qi + 1 }}.</strong> {{ question.q }}</p>
      <div class="quiz-options">
        <button
          v-for="(opt, oi) in question.options"
          :key="oi"
          class="quiz-opt"
          :class="{
            picked: state.picked[qi] === oi,
            right: state.done[qi] && oi === question.correct,
            wrong: state.done[qi] && state.picked[qi] === oi && oi !== question.correct
          }"
          :disabled="state.done[qi]"
          @click="answer(qi, oi)"
        >
          <span class="opt-letter">{{ String.fromCharCode(65 + oi) }}</span>
          <span>{{ opt }}</span>
        </button>
      </div>
      <p v-if="state.done[qi]" class="quiz-explain" :class="state.picked[qi] === question.correct ? 'ok' : 'no'">
        {{ state.picked[qi] === question.correct ? '✅ Correct. ' : '❌ Not quite. ' }}{{ question.explain ?? '' }}
      </p>
    </div>
    <div class="quiz-foot">
      <button class="quiz-reset" @click="reset">↺ Reset quiz</button>
      <span v-if="answeredCount === props.questions.length" class="quiz-done-msg">
        {{ correctCount === props.questions.length ? '🏆 Perfect — chapter mastered.' : 'Review the explanations above, then move on.' }}
      </span>
    </div>
  </div>
</template>

<style scoped>
.quiz-card {
  border: 1px solid var(--vp-c-border);
  border-radius: 12px;
  padding: 1.1rem 1.25rem;
  margin: 2rem 0;
  background: var(--vp-c-bg-soft);
}
.quiz-head { display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.75rem; }
.quiz-badge { font-weight: 600; font-size: 0.9rem; }
.quiz-score { font-size: 0.85rem; color: var(--vp-c-brand-1); font-weight: 600; }
.quiz-q { border-top: 1px dashed var(--vp-c-border); padding-top: 0.9rem; margin-top: 0.9rem; }
.quiz-text { margin: 0 0 0.6rem; }
.quiz-options { display: grid; gap: 0.45rem; }
.quiz-opt {
  display: flex; gap: 0.6rem; align-items: baseline; text-align: left;
  padding: 0.5rem 0.75rem; border-radius: 8px; cursor: pointer; font-size: 0.92rem;
  border: 1px solid var(--vp-c-border); background: var(--vp-c-bg); color: var(--vp-c-text-1);
  transition: border-color 0.15s, background 0.15s;
}
.quiz-opt:not(:disabled):hover { border-color: var(--vp-c-brand-1); }
.quiz-opt:disabled { cursor: default; opacity: 0.85; }
.quiz-opt.right { border-color: var(--vp-c-green-1); background: var(--vp-c-green-soft); }
.quiz-opt.wrong { border-color: var(--vp-c-red-1); background: var(--vp-c-red-soft); }
.quiz-opt.picked:not(.right):not(.wrong) { border-color: var(--vp-c-brand-1); }
.opt-letter { font-weight: 700; font-size: 0.8rem; color: var(--vp-c-text-2); min-width: 1rem; }
.quiz-explain { font-size: 0.88rem; margin: 0.6rem 0 0; padding: 0.6rem 0.75rem; border-radius: 8px; }
.quiz-explain.ok { background: var(--vp-c-green-soft); color: var(--vp-c-text-1); }
.quiz-explain.no { background: var(--vp-c-red-soft); color: var(--vp-c-text-1); }
.quiz-foot { display: flex; gap: 1rem; align-items: center; margin-top: 1rem; }
.quiz-reset {
  font-size: 0.8rem; padding: 0.25rem 0.7rem; border-radius: 999px; cursor: pointer;
  border: 1px solid var(--vp-c-border); background: var(--vp-c-bg); color: var(--vp-c-text-2);
}
.quiz-done-msg { font-size: 0.85rem; color: var(--vp-c-text-2); }
</style>
