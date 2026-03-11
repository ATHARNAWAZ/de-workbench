import { useState, useEffect } from 'react'
import { useLocation } from 'react-router-dom'
import { motion, AnimatePresence } from 'framer-motion'
import { cn } from '../lib/utils'
import CommandPalette from '../design-system/components/CommandPalette'

const ROUTE_LABELS: Record<string, string[]> = {
  '/sources': ['Workbench', 'Data Ingestion'],
  '/quality': ['Workbench', 'Data Quality'],
  '/pipeline': ['Workbench', 'Pipeline Builder'],
  '/modeling': ['Workbench', 'Data Modeling'],
  '/orchestration': ['Workbench', 'Orchestration'],
  '/storage': ['Workbench', 'Storage Layers'],
  '/monitoring': ['Workbench', 'Monitoring'],
  '/catalog': ['Workbench', 'Data Catalog'],
  '/governance': ['Workbench', 'Governance'],
  '/reporting': ['Workbench', 'Reporting'],
  '/learn': ['Workbench', 'Knowledge Base'],
  '/bigdata': ['Workbench', 'Big Data & Databricks'],
  '/cicd': ['Workbench', 'CI/CD Pipelines'],
  '/contracts': ['Workbench', 'Data Contracts'],
  '/cdc': ['Workbench', 'Change Data Capture'],
  '/reversetl': ['Workbench', 'Reverse ETL'],
  '/featurestore': ['Workbench', 'Feature Store & MLOps'],
  '/cloud': ['Workbench', 'Cloud-Native Services'],
  '/iac': ['Workbench', 'Infrastructure as Code'],
  '/incidents': ['Workbench', 'Incident Management'],
  '/datamesh': ['Workbench', 'Data Mesh'],
  '/olap': ['Workbench', 'Real-Time OLAP'],
  '/nosql': ['Workbench', 'NoSQL & Polyglot'],
  '/versioning': ['Workbench', 'Data Versioning'],
  '/capacity': ['Workbench', 'Capacity Planning'],
  '/leadership': ['Workbench', 'Leadership & Soft Skills'],
}

const NOTIFICATIONS = [
  { id: 1, type: 'error' as const, title: 'SLA Breach', message: 'gold.daily_revenue freshness SLA breached (7h overdue)', time: '1h ago' },
  { id: 2, type: 'warning' as const, title: 'Low Volume', message: 'bronze.raw_orders: 423 rows vs expected 12,000+', time: '30m ago' },
  { id: 3, type: 'success' as const, title: 'Pipeline Complete', message: 'daily_orders_pipeline completed in 4m 12s', time: '2h ago' },
  { id: 4, type: 'info' as const, title: 'Report Ready', message: 'Data quality report generated — 3 issues found', time: '4h ago' },
]

const notifColors = {
  error:   { dot: 'bg-[--text-danger]', bar: 'bg-[--text-danger]' },
  warning: { dot: 'bg-[--text-warning]', bar: 'bg-[--text-warning]' },
  success: { dot: 'bg-[--text-success]', bar: 'bg-[--accent-jade]' },
  info:    { dot: 'bg-[--accent-primary]', bar: 'bg-[--accent-primary]' },
}

export default function TopBar() {
  const location = useLocation()
  const crumbs = ROUTE_LABELS[location.pathname] || ['Workbench']
  const [notifOpen, setNotifOpen] = useState(false)
  const [cmdOpen, setCmdOpen] = useState(false)

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
        e.preventDefault()
        setCmdOpen(o => !o)
      }
      if (e.key === 'Escape') {
        setNotifOpen(false)
        setCmdOpen(false)
      }
    }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [])

  return (
    <>
      <header
        className="h-[52px] flex-shrink-0 flex items-center px-6 gap-4 border-b border-[--border-dim]"
        style={{
          backgroundColor: '#E4E4E8',
        }}
      >
        {/* Breadcrumb */}
        <nav className="flex items-center gap-1 flex-shrink-0">
          {crumbs.map((crumb, i) => (
            <span key={i} className="flex items-center gap-1">
              {i > 0 && <span className="text-[--text-tertiary] font-mono text-xs mx-0.5">/</span>}
              <span className={cn(
                'font-body text-xs',
                i === crumbs.length - 1 ? 'text-[--text-primary] font-medium' : 'text-[--text-tertiary]'
              )}>
                {crumb}
              </span>
            </span>
          ))}
        </nav>

        {/* Command palette trigger */}
        <button
          onClick={() => setCmdOpen(true)}
          className={cn(
            'flex-1 max-w-[280px] flex items-center gap-2.5 px-3 h-8 rounded-[5px]',
            'bg-[--bg-floating] border border-[--border-soft]',
            'transition-all duration-150 hover:border-[--border-loud]',
            'text-left'
          )}
        >
          <svg width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" className="text-[--text-tertiary] flex-shrink-0">
            <circle cx="6.5" cy="6.5" r="4.5"/><path d="M10.5 10.5L13.5 13.5"/>
          </svg>
          <span className="flex-1 font-body text-xs text-[--text-tertiary] truncate">Search or run a command...</span>
          <kbd className="font-mono text-[10px] text-[--text-tertiary] bg-[--bg-overlay] border border-[--border-dim] px-1.5 py-0.5 rounded flex-shrink-0">&#x2318;K</kbd>
        </button>

        <div className="flex-1" />

        {/* Pipeline status pill */}
        <div
          className="flex items-center gap-2 px-3.5 py-1.5 rounded-[10px]"
          style={{ background: 'rgba(16,185,129,0.1)', border: '1px solid rgba(16,185,129,0.3)' }}
        >
          <div className="w-[7px] h-[7px] rounded-full bg-[--green] status-dot-running" />
          <span className="font-mono text-[11px] font-semibold" style={{ color: '#34D399' }}>3 pipelines running</span>
        </div>

        {/* Notification bell */}
        <div className="relative">
          <button
            onClick={() => setNotifOpen(o => !o)}
            className="relative w-8 h-8 rounded-[5px] flex items-center justify-center text-[--text-secondary] hover:text-[--text-primary] hover:bg-black/[0.04] transition-colors"
          >
            <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
              <path d="M8 1.5A4.5 4.5 0 003.5 6v3l-1.5 2h12l-1.5-2V6A4.5 4.5 0 008 1.5z"/>
              <path d="M6.5 11a1.5 1.5 0 003 0"/>
            </svg>
            <span className="absolute top-1 right-1 w-2 h-2 rounded-full border-2 border-[#E4E4E8]" style={{ background: "#EC4899" }} />
          </button>

          <AnimatePresence>
            {notifOpen && (
              <motion.div
                initial={{ opacity: 0, y: -4, scale: 0.97 }}
                animate={{ opacity: 1, y: 0, scale: 1 }}
                exit={{ opacity: 0, y: -4, scale: 0.97 }}
                transition={{ duration: 0.15, ease: [0.2, 0, 0, 1] }}
                className="absolute right-0 top-10 w-[340px] border border-[--border-loud] rounded-[8px] z-50 overflow-hidden"
                style={{ background: '#FFFFFF', boxShadow: '0 8px 32px rgba(0,0,0,0.12), 0 2px 8px rgba(0,0,0,0.06)', backdropFilter: 'blur(24px) saturate(180%)' }}
              >
                <div className="px-4 py-3 border-b border-[--border-dim] flex items-center justify-between">
                  <span className="font-body text-sm font-medium text-[--text-primary]">Notifications</span>
                  <button onClick={() => setNotifOpen(false)} className="text-[--text-tertiary] hover:text-[--text-secondary] transition-colors">
                    <svg width="14" height="14" viewBox="0 0 14 14" fill="none"><path d="M1 1l12 12M13 1L1 13" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round"/></svg>
                  </button>
                </div>
                <ul>
                  {NOTIFICATIONS.map(n => (
                    <li key={n.id} className="flex items-start gap-3 px-4 py-3 hover:bg-black/[0.02] transition-colors border-b border-[--border-dim] last:border-0 relative">
                      <div className={cn('absolute left-0 top-0 bottom-0 w-[3px]', notifColors[n.type].bar)} />
                      <div className={cn('w-1.5 h-1.5 rounded-full flex-shrink-0 mt-1.5', notifColors[n.type].dot)} />
                      <div className="flex-1 min-w-0">
                        <p className="font-body text-xs font-medium text-[--text-primary]">{n.title}</p>
                        <p className="font-body text-xs text-[--text-secondary] leading-snug mt-0.5">{n.message}</p>
                        <p className="font-mono text-[10px] text-[--text-tertiary] mt-1">{n.time}</p>
                      </div>
                    </li>
                  ))}
                </ul>
              </motion.div>
            )}
          </AnimatePresence>
        </div>

        {/* Avatar */}
        <div className="w-8 h-8 rounded-full flex items-center justify-center flex-shrink-0" style={{ background: "linear-gradient(135deg, #7C3AED, #EC4899)", border: "2px solid rgba(124,58,237,0.5)", boxShadow: "0 0 14px rgba(124,58,237,0.3)" }}>
          <span className="font-display font-bold text-[10px] text-white">SR</span>
        </div>
      </header>

      <CommandPalette open={cmdOpen} onClose={() => setCmdOpen(false)} />
    </>
  )
}
