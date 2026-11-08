import DefaultTheme from 'vitepress/theme'
import type { Theme } from 'vitepress'
import QuizCard from './components/QuizCard.vue'
import ProgressOverview from './components/ProgressOverview.vue'
import ScalingViz from './components/ScalingViz.vue'
import QueryPlanViz from './components/QueryPlanViz.vue'
import ShuffleViz from './components/ShuffleViz.vue'
import WindowViz from './components/WindowViz.vue'
import HashRingViz from './components/HashRingViz.vue'
import BackpressureViz from './components/BackpressureViz.vue'
import KnowledgeGalaxy from './components/KnowledgeGalaxy.vue'
import KnowledgeMap from './components/KnowledgeMap.vue'
import './style.css'

export default {
  extends: DefaultTheme,
  enhanceApp({ app }) {
    app.component('QuizCard', QuizCard)
    app.component('ProgressOverview', ProgressOverview)
    app.component('ScalingViz', ScalingViz)
    app.component('QueryPlanViz', QueryPlanViz)
    app.component('ShuffleViz', ShuffleViz)
    app.component('WindowViz', WindowViz)
    app.component('HashRingViz', HashRingViz)
    app.component('BackpressureViz', BackpressureViz)
    app.component('KnowledgeGalaxy', KnowledgeGalaxy)
    app.component('KnowledgeMap', KnowledgeMap)
  }
} satisfies Theme
