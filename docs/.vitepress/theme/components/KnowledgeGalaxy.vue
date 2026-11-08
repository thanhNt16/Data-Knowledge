<script setup lang="ts">
// 3D knowledge galaxy — three.js via 3d-force-graph.
// Nodes glow per track cluster (pinned track hubs keep clusters distinct),
// starfield background, orbit controls; hover highlights neighborhood,
// click opens a detail side card (blurb, relations, chapter link, Q&A).
import { ref, computed, onMounted, onBeforeUnmount } from 'vue'
import { nodes, edges, TRACK_COLORS, TRACK_META } from '../data/graph'
import type { GraphNode } from '../data/graph'

type GNode = GraphNode & { x?: number; y?: number; z?: number; fx?: number; fy?: number; fz?: number }
type GLink = { source: string | GNode; target: string | GNode; rel: string }

const el = ref<HTMLElement | null>(null)
const selected = ref<GNode | null>(null)
const paused = ref(false)

const trackLabel = (slug: string) => TRACK_META.find(m => m.slug === slug)?.label ?? slug

const byId = new Map(nodes.map(n => [n.id, n]))
const relations = computed(() => {
  const s = selected.value
  if (!s) return []
  return edges
    .filter(e => e.source === s.id || e.target === s.id)
    .map(e => {
      const out = e.source === s.id
      const other = byId.get(out ? e.target : e.source)
      return { node: other, rel: e.rel, out }
    })
    .filter(r => r.node)
})

let Graph: any = null
let THREE: any = null
let observer: ResizeObserver | null = null
let hoverNode: GNode | null = null
let nodeObjs = new Map<string, any>() // node id → THREE group
let nbNodes = new Set<GNode>()
let nbLinks = new Set<GLink>()
const legendFolded = ref(false)
const CHAPTER_LABEL_DIST = 230

const kindSize: Record<string, { r: number; glow: number }> = {
  track: { r: 9, glow: 34 },
  chapter: { r: 4.5, glow: 17 },
  concept: { r: 2.2, glow: 9 }
}

function deselect() {
  selected.value = null
  updateHighlight()
}

function selectNode(n: GNode, flyTo = true) {
  selected.value = n
  updateHighlight()
  if (flyTo && Graph && n.x !== undefined) {
    Graph.cameraPosition(
      { x: n.x + 40, y: (n.y ?? 0) + 30, z: (n.z ?? 0) + 130 },
      { x: n.x, y: n.y, z: n.z },
      900
    )
  }
}

function pickRelation(r: { node?: GraphNode }) {
  const n = Graph?.graphData().nodes.find((g: GNode) => g.id === r.node!.id)
  if (n) selectNode(n)
}

function idOf(v: string | GNode) { return typeof v === 'string' ? v : v.id }

function updateHighlight() {
  const active = hoverNode ?? selected.value
  nbNodes = new Set()
  nbLinks = new Set()
  if (active) {
    nbNodes.add(active)
    Graph.graphData().links.forEach((l: GLink) => {
      const s = idOf(l.source), t = idOf(l.target)
      if (s === active.id) { nbLinks.add(l); nbNodes.add(l.target as GNode) }
      if (t === active.id) { nbLinks.add(l); nbNodes.add(l.source as GNode) }
    })
  }
  const has = !!active
  Graph.linkColor((l: GLink) => !has
    ? 'rgba(140,160,220,0.15)'
    : nbLinks.has(l) ? 'rgba(170,200,255,0.85)' : 'rgba(140,160,220,0.04)')
  // dim non-neighborhood nodes
  Graph.graphData().nodes.forEach((n: GNode) => {
    const g = nodeObjs.get(n.id)
    if (!g) return
    const dim = has && !nbNodes.has(n)
    g.userData.mesh.material.opacity = dim ? 0.25 : 1
    g.userData.glow.material.opacity = dim ? 0.08 : (n.kind === 'track' ? 0.9 : 0.7)
  })
  updateLabels()
}

function updateLabels() {
  if (!Graph || !THREE) return
  const cam = Graph.camera().position
  const active = hoverNode ?? selected.value
  Graph.graphData().nodes.forEach((n: GNode) => {
    const g = nodeObjs.get(n.id)
    const lbl = g?.userData.label
    if (!lbl || n.kind === 'track') return
    const near = n.x !== undefined &&
      cam.distanceTo(new THREE.Vector3(n.x, n.y, n.z)) < CHAPTER_LABEL_DIST
    lbl.visible = near || (!!active && nbNodes.has(n))
  })
}

function togglePause() {
  paused.value = !paused.value
  paused.value ? Graph.pauseAnimation() : Graph.resumeAnimation()
}

function resetView() {
  Graph.cameraPosition({ x: 0, y: 60, z: 620 }, { x: 0, y: 0, z: 0 }, 900)
}

onMounted(async () => {
  if (!el.value) return
  const [{ default: ForceGraph3D }, three] = await Promise.all([
    import('3d-force-graph'),
    import('three')
  ])
  THREE = three

  // Cluster layout: track hubs pinned on a ring, members jittered around them.
  const R = 330
  const centers = new Map<string, { x: number; y: number; z: number }>()
  TRACK_META.forEach((m, i) => {
    const a = (i / TRACK_META.length) * Math.PI * 2
    centers.set(m.slug, { x: Math.cos(a) * R, y: (i % 3) * 40 - 40, z: Math.sin(a) * R })
  })
  let extra = TRACK_META.length
  const centerOf = (slug: string) => {
    if (!centers.has(slug)) {
      const a = (extra++ / Math.max(TRACK_META.length, 12)) * Math.PI * 2
      centers.set(slug, { x: Math.cos(a) * R, y: 0, z: Math.sin(a) * R })
    }
    return centers.get(slug)!
  }
  const jit = (s: number) => (Math.random() - 0.5) * 2 * s
  const gNodes: GNode[] = nodes.map(n => {
    const c = centerOf(n.track)
    const g: GNode = { ...n, x: c.x + jit(90), y: c.y + jit(90), z: c.z + jit(90) }
    if (n.kind === 'track') { g.x = c.x; g.y = c.y; g.z = c.z; g.fx = c.x; g.fy = c.y; g.fz = c.z }
    return g
  })

  Graph = ForceGraph3D()(el.value)
    .backgroundColor('#05060f')
    .showNavInfo(false)
    .nodeLabel((n: GNode) => `${n.label} — ${n.blurb}`)
    .nodeThreeObject((n: GNode) => buildNodeObject(n))
    .linkColor('rgba(140,160,220,0.15)')
    .linkOpacity(1)
    .linkResolution(2)
    .warmupTicks(100)
    .cooldownTicks(200)
    .onNodeHover((n: GNode | null) => { hoverNode = n; updateHighlight() })
    .onNodeClick((n: GNode) => selectNode(n, false))
    .onBackgroundClick(() => deselect())
    .graphData({ nodes: gNodes, links: edges.map(e => ({ ...e })) })

  // link distance by relation keeps cluster hierarchy readable
  const linkF = Graph.d3Force('link')
  if (linkF) linkF.distance((l: GLink) => ({ contains: 45, 'part of': 55, 'depends on': 75, related: 95 }[l.rel] ?? 60))
  const chargeF = Graph.d3Force('charge')
  if (chargeF) chargeF.strength(-90)

  // Starfield — ~800 points on a large spherical shell.
  const N = 800, pos = new Float32Array(N * 3)
  for (let i = 0; i < N; i++) {
    const r = 1100 + Math.random() * 600
    const th = Math.random() * Math.PI * 2
    const ph = Math.acos(2 * Math.random() - 1)
    pos[i * 3] = r * Math.sin(ph) * Math.cos(th)
    pos[i * 3 + 1] = r * Math.cos(ph) * 0.6
    pos[i * 3 + 2] = r * Math.sin(ph) * Math.sin(th)
  }
  const starGeo = new THREE.BufferGeometry()
  starGeo.setAttribute('position', new THREE.BufferAttribute(pos, 3))
  Graph.scene().add(new THREE.Points(starGeo, new THREE.PointsMaterial({
    color: 0xcdd6ff, size: 2.2, sizeAttenuation: true, transparent: true, opacity: 0.8, depthWrite: false
  })))

  Graph.controls().addEventListener('change', updateLabels)
  updateHighlight()
  resetView()

  observer = new ResizeObserver(() => {
    if (el.value) Graph.width(el.value.clientWidth).height(el.value.clientHeight)
  })
  observer.observe(el.value)
})

// --- three.js node objects -------------------------------------------------
let glowTex: any = null
function getGlowTex() {
  if (glowTex) return glowTex
  const c = document.createElement('canvas')
  c.width = c.height = 128
  const ctx = c.getContext('2d')!
  const grad = ctx.createRadialGradient(64, 64, 0, 64, 64, 64)
  grad.addColorStop(0, 'rgba(255,255,255,1)')
  grad.addColorStop(0.35, 'rgba(255,255,255,0.45)')
  grad.addColorStop(1, 'rgba(255,255,255,0)')
  ctx.fillStyle = grad
  ctx.fillRect(0, 0, 128, 128)
  glowTex = new THREE.CanvasTexture(c)
  return glowTex
}

function makeLabel(text: string, big: boolean) {
  const c = document.createElement('canvas')
  c.width = 512; c.height = 96
  const ctx = c.getContext('2d')!
  ctx.font = `${big ? '700' : '500'} ${big ? 56 : 44}px system-ui, sans-serif`
  ctx.textAlign = 'center'
  ctx.textBaseline = 'middle'
  ctx.shadowColor = 'rgba(0,0,0,0.9)'
  ctx.shadowBlur = 10
  ctx.fillStyle = '#eef2ff'
  ctx.fillText(text, 256, 48)
  const spr = new THREE.Sprite(new THREE.SpriteMaterial({
    map: new THREE.CanvasTexture(c), transparent: true, depthWrite: false
  }))
  spr.scale.set(big ? 60 : 44, big ? 11 : 8.5, 1)
  return spr
}

function buildNodeObject(n: GNode) {
  const color = TRACK_COLORS[n.track] ?? '#94a3b8'
  const { r, glow } = kindSize[n.kind] ?? kindSize.concept
  const group = new THREE.Group()

  const mesh = new THREE.Mesh(
    new THREE.SphereGeometry(r, 20, 16),
    new THREE.MeshBasicMaterial({ color, transparent: true, opacity: 1 })
  )
  group.add(mesh)

  const sprite = new THREE.Sprite(new THREE.SpriteMaterial({
    map: getGlowTex(), color, transparent: true, opacity: n.kind === 'track' ? 0.9 : 0.7,
    blending: THREE.AdditiveBlending, depthWrite: false
  }))
  sprite.scale.set(glow, glow, 1)
  group.add(sprite)

  let label: any = null
  if (n.kind === 'track') {
    label = makeLabel(n.label, true)
    label.position.y = r + 10
    group.add(label)
  } else {
    label = makeLabel(n.label, false)
    label.position.y = r + 7
    label.visible = false
    group.add(label)
  }

  group.userData = { mesh, glow: sprite, label }
  nodeObjs.set(n.id, group)
  return group
}

onBeforeUnmount(() => {
  observer?.disconnect()
  if (Graph) {
    try { Graph.renderer().dispose() } catch { /* already torn down */ }
    Graph._destructor()
    Graph = null
  }
  glowTex = null
  nodeObjs = new Map()
})
</script>

<template>
  <div class="galaxy-wrap">
    <div ref="el" class="galaxy-canvas" data-viz="galaxy"></div>

    <!-- detail side card -->
    <aside v-if="selected" class="galaxy-card">
      <button class="galaxy-close" aria-label="Close" @click="deselect">✕</button>
      <div class="galaxy-card-head">
        <span class="galaxy-badge" :data-kind="selected.kind">{{ selected.kind }}</span>
        <span class="galaxy-chip" :style="{ background: (TRACK_COLORS[selected.track] ?? '#94a3b8') + '33', color: TRACK_COLORS[selected.track] ?? '#94a3b8' }">
          {{ trackLabel(selected.track) }}
        </span>
      </div>
      <h3>{{ selected.label }}</h3>
      <p class="galaxy-blurb">{{ selected.blurb }}</p>

      <div v-if="relations.length" class="galaxy-rels">
        <h4>Relations</h4>
        <button v-for="(r, i) in relations" :key="i" class="galaxy-rel" @click="pickRelation(r)">
          <span class="galaxy-rel-dir">{{ r.out ? '→' : '←' }}</span>
          <span class="galaxy-rel-name">{{ r.node?.label }}</span>
          <span class="galaxy-rel-tag">{{ r.rel }}</span>
        </button>
      </div>
      <div v-if="selected.qa?.length" class="galaxy-qa">
        <h4>Quick check</h4>
        <details v-for="(qa, i) in selected.qa" :key="i">
          <summary>{{ qa.q }}</summary>
          <p>{{ qa.a }}</p>
        </details>
      </div>

      <a v-if="selected.link" class="galaxy-read" :href="selected.link">Read chapter →</a>
    </aside>

    <!-- controls hint + buttons -->
    <div class="galaxy-hint">
      <span>drag rotate · scroll zoom · click node</span>
      <button class="galaxy-btn" @click="resetView">Reset view</button>
      <button class="galaxy-btn" @click="togglePause">{{ paused ? '▶ Resume' : '⏸ Pause' }}</button>
    </div>

    <!-- track legend -->
    <ul class="galaxy-legend" :class="{ folded: legendFolded }">
      <button class="legend-toggle" @click="legendFolded = !legendFolded" :aria-expanded="!legendFolded">
        {{ legendFolded ? '◧ Legend' : '◧ Legend ▾' }}
      </button>
      <template v-if="!legendFolded">
      <li v-for="m in TRACK_META" :key="m.slug">
        <i :style="{ background: TRACK_COLORS[m.slug] ?? '#94a3b8' }"></i>{{ m.label }}
      </li>
      </template>
    </ul>
  </div>
</template>

<style scoped>
.galaxy-wrap {
  position: relative;
  border: 1px solid var(--vp-c-border);
  border-radius: 12px;
  overflow: hidden;
  margin: 1.5rem 0;
  background: #05060f;
}
.galaxy-canvas { height: clamp(420px, 70vh, 720px); }
.galaxy-canvas :deep(canvas) { display: block; }

.galaxy-card {
  position: absolute;
  top: 0.75rem;
  right: 0.75rem;
  bottom: 0.75rem;
  width: min(340px, calc(100% - 1.5rem));
  overflow-y: auto;
  padding: 1rem 1.1rem;
  border: 1px solid rgba(140, 160, 220, 0.25);
  border-radius: 10px;
  background: rgba(10, 13, 28, 0.92);
  backdrop-filter: blur(6px);
  color: #e2e8f0;
  font-size: 0.85rem;
  line-height: 1.5;
}
.galaxy-card h3 { margin: 0.4rem 0 0.3rem; font-size: 1.05rem; color: #f1f5ff; }
.galaxy-card h4 { margin: 0.9rem 0 0.35rem; font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.06em; color: #8b9cc7; }
.galaxy-close {
  position: absolute; top: 0.5rem; right: 0.6rem;
  border: 0; background: none; color: #8b9cc7; font-size: 0.95rem; cursor: pointer;
}
.galaxy-close:hover { color: #e2e8f0; }
.galaxy-card-head { display: flex; gap: 0.45rem; align-items: center; padding-right: 1.2rem; }
.galaxy-badge {
  padding: 0.1rem 0.5rem; border-radius: 999px; font-size: 0.68rem;
  text-transform: uppercase; letter-spacing: 0.05em;
  background: rgba(140, 160, 220, 0.18); color: #aab8e8;
}
.galaxy-badge[data-kind='track'] { background: rgba(232, 121, 249, 0.18); color: #e879f9; }
.galaxy-badge[data-kind='concept'] { background: rgba(96, 165, 250, 0.16); color: #7fb3f5; }
.galaxy-chip { padding: 0.1rem 0.5rem; border-radius: 999px; font-size: 0.68rem; }
.galaxy-blurb { margin: 0.2rem 0 0.4rem; color: #b8c3e0; }

.galaxy-rels { display: flex; flex-direction: column; gap: 0.25rem; }
.galaxy-rels h4 { margin-bottom: 0.15rem; }
.galaxy-rel {
  display: flex; align-items: center; gap: 0.45rem;
  padding: 0.3rem 0.45rem; border: 1px solid rgba(140, 160, 220, 0.18);
  border-radius: 6px; background: rgba(20, 26, 48, 0.6);
  color: #dbe4ff; font-size: 0.8rem; text-align: left; cursor: pointer;
}
.galaxy-rel:hover { border-color: rgba(170, 200, 255, 0.5); background: rgba(30, 38, 66, 0.8); }
.galaxy-rel-dir { color: #7d8fc0; flex: none; }
.galaxy-rel-name { flex: 1; min-width: 0; }
.galaxy-rel-tag { flex: none; font-size: 0.65rem; color: #7d8fc0; font-style: italic; }

.galaxy-qa details {
  border: 1px solid rgba(140, 160, 220, 0.18); border-radius: 6px;
  padding: 0.35rem 0.5rem; margin-bottom: 0.35rem; background: rgba(20, 26, 48, 0.6);
}
.galaxy-qa summary { cursor: pointer; font-size: 0.8rem; color: #dbe4ff; }
.galaxy-qa details p { margin: 0.4rem 0 0.1rem; color: #b8c3e0; font-size: 0.78rem; }

.galaxy-read {
  display: inline-block; margin-top: 0.7rem;
  padding: 0.35rem 0.8rem; border-radius: 6px;
  background: rgba(96, 165, 250, 0.18); color: #9ec2ff;
  text-decoration: none; font-weight: 600; font-size: 0.82rem;
}
.galaxy-read:hover { background: rgba(96, 165, 250, 0.3); }

.galaxy-hint {
  position: absolute; left: 0.75rem; bottom: 0.75rem;
  display: flex; align-items: center; gap: 0.5rem;
  color: #7d8fc0; font-size: 0.72rem;
}
.galaxy-btn {
  border: 1px solid rgba(140, 160, 220, 0.3); border-radius: 6px;
  background: rgba(10, 13, 28, 0.85); color: #aab8e8;
  padding: 0.2rem 0.55rem; font-size: 0.7rem; cursor: pointer;
}
.galaxy-btn:hover { border-color: rgba(170, 200, 255, 0.6); color: #e2e8f0; }

.galaxy-legend {
  position: absolute; right: 0.75rem; bottom: 0.75rem;
  margin: 0; padding: 0.5rem 0.7rem; list-style: none;
  border: 1px solid rgba(140, 160, 220, 0.18); border-radius: 8px;
  background: rgba(10, 13, 28, 0.85);
  font-size: 0.68rem; color: #aab8e8;
  max-height: 45%; overflow-y: auto;
}
.legend-toggle {
  all: unset; cursor: pointer; font-size: 10px; color: #c7d2fe;
  padding: 0 0 0.25rem; letter-spacing: 0.02em;
}
.legend-toggle:hover { color: #fff; }
.galaxy-legend.folded { padding: 0.35rem 0.55rem; }
.galaxy-legend li { display: flex; align-items: center; gap: 0.4rem; padding: 0.1rem 0; white-space: nowrap; }
.galaxy-legend i { width: 8px; height: 8px; border-radius: 50%; flex: none; box-shadow: 0 0 6px currentColor; }

@media (max-width: 640px) {
  .galaxy-legend { display: none; }
  .galaxy-hint span { display: none; }
}
</style>
