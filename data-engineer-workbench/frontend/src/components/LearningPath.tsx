import { useState, useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import { motion, AnimatePresence } from 'framer-motion'
import { cn } from '../lib/utils'

interface PathItem { module: string; title: string; path: string; required: boolean }
interface LearningPath { id: string; name: string; description: string; color: string; items: PathItem[] }

const PATHS: LearningPath[] = [
  {
    id: 'beginner', name: 'Beginner', description: 'Core data engineering fundamentals', color: '#68D391',
    items: [
      { module: '01', title: 'Data Ingestion', path: '/sources', required: true },
      { module: '02', title: 'Data Quality', path: '/quality', required: true },
      { module: '03', title: 'Pipeline Builder', path: '/pipeline', required: true },
      { module: '04', title: 'Data Modeling', path: '/modeling', required: true },
      { module: '05', title: 'Orchestration', path: '/orchestration', required: true },
      { module: '08', title: 'Data Catalog', path: '/catalog', required: false },
    ],
  },
  {
    id: 'intermediate', name: 'Intermediate', description: 'Platform engineering & advanced patterns', color: '#63B3ED',
    items: [
      { module: '06', title: 'Storage Layers', path: '/storage', required: true },
      { module: '07', title: 'Monitoring', path: '/monitoring', required: true },
      { module: '09', title: 'Governance', path: '/governance', required: true },
      { module: '10', title: 'Reporting', path: '/reporting', required: true },
      { module: '12', title: 'Big Data & Databricks', path: '/bigdata', required: true },
      { module: '15', title: 'Change Data Capture', path: '/cdc', required: false },
      { module: '19', title: 'Infrastructure as Code', path: '/iac', required: false },
    ],
  },
  {
    id: 'senior', name: 'Senior', description: 'Architecture, MLOps, leadership & scale', color: '#B794F4',
    items: [
      { module: '13', title: 'CI/CD Pipelines', path: '/cicd', required: true },
      { module: '14', title: 'Data Contracts', path: '/contracts', required: true },
      { module: '17', title: 'Feature Store & MLOps', path: '/featurestore', required: true },
      { module: '18', title: 'Cloud-Native Services', path: '/cloud', required: true },
      { module: '21', title: 'Data Mesh', path: '/datamesh', required: true },
      { module: '22', title: 'Real-Time OLAP', path: '/olap', required: true },
      { module: '25', title: 'Capacity Planning', path: '/capacity', required: false },
      { module: '26', title: 'Leadership & Soft Skills', path: '/leadership', required: true },
    ],
  },
]

const STORAGE_KEY = 'de_workbench_progress'

function loadProgress(): Set<string> {
  try {
    const raw = localStorage.getItem(STORAGE_KEY)
    return raw ? new Set(JSON.parse(raw)) : new Set()
  } catch { return new Set() }
}

export default function LearningPath() {
  const [activePath, setActivePath] = useState<string | null>(null)
  const [visited, setVisited] = useState<Set<string>>(() => loadProgress())
  const navigate = useNavigate()

  useEffect(() => {
    const sync = () => setVisited(loadProgress())
    sync()
    window.addEventListener('storage', sync)
    const id = setInterval(sync, 2000)
    return () => { window.removeEventListener('storage', sync); clearInterval(id) }
  }, [])

  const totalModules = 26
  const completedCount = visited.size
  const pct = Math.round((completedCount / totalModules) * 100)

  return (
    <div className="px-2 py-3 mt-2 border-t border-[--border-dim]">
      {/* Progress bar */}
      <div className="px-1 mb-3">
        <div className="flex justify-between mb-1.5">
          <span className="label-section">Progress</span>
          <span className="font-mono text-[10px] text-[--text-tertiary]">{completedCount}/{totalModules}</span>
        </div>
        <div className="h-[3px] bg-[--bg-floating] overflow-hidden">
          <motion.div
            className="h-full"
            style={{ background: 'linear-gradient(90deg, var(--accent-primary), var(--accent-secondary))' }}
            initial={{ width: 0 }}
            animate={{ width: `${pct}%` }}
            transition={{ duration: 0.6, ease: [0.2, 0, 0, 1] }}
          />
        </div>
      </div>

      {/* Paths */}
      <div className="label-section px-1 mb-1.5">Learning Paths</div>
      <div className="space-y-0.5">
        {PATHS.map(lp => {
          const done = lp.items.filter(i => visited.has(i.path)).length
          const isOpen = activePath === lp.id

          return (
            <div key={lp.id} className="rounded-[5px] overflow-hidden border border-[--border-dim]">
              <button
                onClick={() => setActivePath(prev => prev === lp.id ? null : lp.id)}
                className="w-full flex items-center gap-2 px-2.5 py-2 text-left hover:bg-white/[0.02] transition-colors"
              >
                <span className="w-1.5 h-1.5 rounded-full flex-shrink-0" style={{ backgroundColor: lp.color }} />
                <div className="flex-1 min-w-0">
                  <p className="font-body text-xs font-medium" style={{ color: lp.color }}>{lp.name}</p>
                  <p className="font-mono text-[10px] text-[--text-tertiary]">{done}/{lp.items.length}</p>
                </div>
                <motion.span
                  animate={{ rotate: isOpen ? 90 : 0 }}
                  transition={{ duration: 0.15 }}
                  className="text-[--text-tertiary]"
                >
                  <svg width="10" height="10" viewBox="0 0 10 10" fill="none">
                    <path d="M3 2l4 3-4 3" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/>
                  </svg>
                </motion.span>
              </button>

              <AnimatePresence initial={false}>
                {isOpen && (
                  <motion.div
                    initial={{ height: 0, opacity: 0 }}
                    animate={{ height: 'auto', opacity: 1 }}
                    exit={{ height: 0, opacity: 0 }}
                    transition={{ duration: 0.18, ease: [0.2, 0, 0, 1] }}
                    className="overflow-hidden"
                  >
                    <div className="px-2 pb-1.5 space-y-0.5 border-t border-[--border-dim]">
                      {lp.items.map(item => {
                        const isDone = visited.has(item.path)
                        return (
                          <button
                            key={item.path}
                            onClick={() => navigate(item.path)}
                            className="w-full flex items-center gap-2 px-2 py-1.5 rounded text-left hover:bg-white/[0.03] transition-colors group"
                          >
                            <div
                              className={cn(
                                'w-3.5 h-3.5 rounded-full border flex-shrink-0 flex items-center justify-center',
                                isDone ? 'border-transparent' : 'border-[--border-loud] bg-transparent'
                              )}
                              style={isDone ? { backgroundColor: lp.color } : undefined}
                            >
                              {isDone && (
                                <svg width="8" height="8" viewBox="0 0 8 8" fill="none">
                                  <path d="M1 4l2 2 4-4" stroke="white" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/>
                                </svg>
                              )}
                            </div>
                            <span className={cn(
                              'font-body text-[11px] flex-1',
                              isDone ? 'text-[--text-tertiary] line-through' : 'text-[--text-secondary] group-hover:text-[--text-primary]'
                            )}>
                              {item.module}. {item.title}
                            </span>
                            {!item.required && (
                              <span className="font-mono text-[9px] text-[--text-tertiary]">opt</span>
                            )}
                          </button>
                        )
                      })}
                    </div>
                  </motion.div>
                )}
              </AnimatePresence>
            </div>
          )
        })}
      </div>
    </div>
  )
}
