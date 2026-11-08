import { defineConfig } from 'vitepress'
import { withMermaid } from 'vitepress-plugin-mermaid'

export default withMermaid(
  defineConfig({
    base: '/Data-Knowledge/',
    markdown: { math: true },
    title: 'Data Engineering Knowledge',
    description:
      'Pareto-focused, visual, interactive knowledge base for leveling up to Senior Data Engineer, Solution Architect, and Cloud Engineer.',
    head: [['link', { rel: 'icon', href: '/favicon.svg' }]],
    themeConfig: {
      // Site-wide search
      search: { provider: 'local' },
      nav: [
        { text: 'Home', link: '/' },
        { text: 'How to Learn', link: '/how-to-learn' },
        { text: 'Intuitions', link: '/intuitions/' },
        { text: 'Interview', link: '/interview/' },
        { text: '🪐 Galaxy', link: '/explore/' },
        { text: '🗺️ Map', link: '/map/' },
        { text: 'Platforms', link: '/fundamentals/' },
        { text: 'Pipelines', link: '/pipelines/' },
        { text: 'SQL', link: '/sql/' },
        { text: 'Python', link: '/python/' },
        { text: 'Spark', link: '/spark/' },
        { text: 'Streaming', link: '/streaming/' },
        { text: 'System Design', link: '/system-design/' },
        { text: 'Cloud', link: '/cloud/' },
        { text: 'DataOps', link: '/dataops/' }
      ],
      outline: { level: [2, 3] },
      docFooter: { prev: 'Previous', next: 'Next' },
      sidebar: {
        '/fundamentals/': [
          {
            text: '1 · Data Platforms',
            items: [
              { text: 'Track Overview', link: '/fundamentals/' },
              { text: 'Storage Architectures', link: '/fundamentals/storage-architectures' },
              { text: 'File & Table Formats', link: '/fundamentals/file-formats' },
              { text: 'Data Modeling', link: '/fundamentals/data-modeling' },
              { text: 'Normalization & Denormalization', link: '/fundamentals/normalization' }
            ]
          }
        ],
        '/pipelines/': [
          {
            text: '2 · Pipelines & Architecture',
            items: [
              { text: 'Track Overview', link: '/pipelines/' },
              { text: 'Batch vs Streaming', link: '/pipelines/batch-vs-stream' },
              { text: 'Lambda & Kappa', link: '/pipelines/lambda-kappa' },
              { text: 'ETL, ELT, CDC & Backfills', link: '/pipelines/etl-elt-cdc' }
            ]
          }
        ],
        '/sql/': [
          {
            text: '3 · SQL Deep Dive',
            items: [
              { text: 'Track Overview', link: '/sql/' },
              { text: 'Advanced Query Craft', link: '/sql/advanced-queries' },
              { text: 'Query Performance', link: '/sql/query-performance' },
              { text: 'Analytics SQL Patterns', link: '/sql/analytics-patterns' }
            ]
          }
        ],
        '/python/': [
          {
            text: '4 · Python Deep Dive',
            items: [
              { text: 'Track Overview', link: '/python/' },
              { text: 'Advanced Python', link: '/python/advanced-python' },
              { text: 'Python for Data Engineering', link: '/python/data-engineering' }
            ]
          }
        ],
        '/spark/': [
          {
            text: '5 · Spark Deep Dive',
            items: [
              { text: 'Track Overview', link: '/spark/' },
              { text: 'Architecture & Execution', link: '/spark/architecture' },
              { text: 'Catalyst, Shuffle & Joins', link: '/spark/execution' },
              { text: 'Performance Optimization', link: '/spark/optimization' },
              { text: 'Advanced Spark Algorithms', link: '/spark/advanced-algorithms' }
            ]
          }
        ],
        '/streaming/': [
          {
            text: '6 · Streaming Systems',
            items: [
              { text: 'Track Overview', link: '/streaming/' },
              { text: 'Kafka Fundamentals', link: '/streaming/kafka' },
              { text: 'Flink & Beam', link: '/streaming/flink-beam' },
              { text: 'Windows, Watermarks & Late Data', link: '/streaming/windows-watermarks' }
            ]
          }
        ],
        '/system-design/': [
          {
            text: '7 · System Design',
            items: [
              { text: 'Track Overview', link: '/system-design/' },
              { text: 'Data-Intensive Foundations', link: '/system-design/data-intensive' },
              { text: 'Designing Reliable Pipelines', link: '/system-design/pipeline-design' },
              { text: 'ML Pipelines in Production', link: '/system-design/ml-pipelines' },
              { text: 'GenAI Engineering', link: '/system-design/genai-engineering' },
              { text: 'ML & GenAI System Design', link: '/system-design/ml-systems' },
              { text: 'Data Platform Design Case Studies', link: '/system-design/platform-designs' }
            ]
          }
        ],
        '/cloud/': [
          {
            text: '8 · Cloud Platforms',
            items: [
              { text: 'Track Overview', link: '/cloud/' },
              { text: 'GCP vs AWS vs Azure', link: '/cloud/big-three' },
              { text: 'Databricks vs Snowflake', link: '/cloud/lakehouse-platforms' },
              { text: 'Cost & Decoupled Architecture', link: '/cloud/cost-architecture' },
              { text: 'Networking, Multi-Region & Landing Zones', link: '/cloud/platform-deep-dive' }
            ]
          }
        ],
        '/dataops/': [
          {
            text: '9 · DataOps & Orchestration',
            items: [
              { text: 'Track Overview', link: '/dataops/' },
              { text: 'Orchestration', link: '/dataops/orchestration' },
              { text: 'Transformation, Quality & Contracts', link: '/dataops/quality' },
              { text: 'dbt in Depth', link: '/dataops/dbt' }
            ]
          }
        ]
        ,
        '/intuitions/': [
          {
            text: '0 · Visual Intuitions',
            items: [
              { text: 'Track Overview', link: '/intuitions/' },
              { text: 'Scaling: Up vs Out', link: '/intuitions/scaling' },
              { text: 'Query Plans & Optimization', link: '/intuitions/query-plans' },
              { text: 'Shuffle, Partitioning & Skew', link: '/intuitions/shuffle-skew' }
            ]
          }
        ],
        '/interview/': [
          {
            text: '10 · Cracking the DE Interview',
            items: [
              { text: 'Track Overview', link: '/interview/' },
              { text: 'The Playbook', link: '/interview/playbook' },
              { text: 'SQL & Coding Rounds', link: '/interview/sql-coding' },
              { text: 'System Design Round', link: '/interview/system-design' },
              { text: 'Rapid Fire', link: '/interview/rapid-fire' },
              { text: 'Databricks Practice Drill', link: '/interview/databricks-practice' }
            ]
          }
        ],
        '/explore/': [
          {
            text: '🪐 Knowledge Galaxy',
            items: [{ text: 'Explore the Graph', link: '/explore/' }]
          }
        ],
        '/map/': [
          {
            text: '🗺️ Knowledge Map',
            items: [{ text: 'Your Learning Path', link: '/map/' }]
          }
        ]
      }
    },
    mermaid: {
      theme: 'default'
    },
    vite: { optimizeDeps: { include: ['mermaid', 'fastdom'] } }
  })
)
