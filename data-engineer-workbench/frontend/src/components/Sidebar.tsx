import { useState } from 'react'
import { NavLink, useLocation } from 'react-router-dom'
import { motion, AnimatePresence } from 'framer-motion'
import { cn } from '../lib/utils'
import LearningPath from './LearningPath'

interface NavItem {
  path: string
  label: string
  icon: string
  module: string
}

interface NavGroup {
  id: string
  label: string
  color: string
  items: NavItem[]
}

// SVG icon paths (16x16 viewBox)
const icons: Record<string, string> = {
  database:    'M3 4c0-1.1 2.24-2 5-2s5 .9 5 2v2c0 1.1-2.24 2-5 2S3 7.1 3 6V4zm0 4c0 1.1 2.24 2 5 2s5-.9 5-2m-10 4c0 1.1 2.24 2 5 2s5-.9 5-2M3 8v4m10-4v4',
  shield:      'M8 1.5L2 4v4c0 3.5 2.5 6.5 6 7.5 3.5-1 6-4 6-7.5V4L8 1.5z',
  git:         'M5 3a2 2 0 100 4 2 2 0 000-4zm6 0a2 2 0 100 4 2 2 0 000-4zm-8 8a2 2 0 100 4 2 2 0 000-4zM5 5v1a3 3 0 003 3h1m2-4v4a3 3 0 01-3 3H7',
  layout:      'M2 3h12v3H2zM2 8h6v5H2zM10 8h4v5h-4z',
  clock:       'M8 2a6 6 0 100 12A6 6 0 008 2zm0 2v4l2.5 2.5',
  server:      'M2 3h12v4H2zm0 6h12v4H2zm10-5v2m0 6v2',
  chart:       'M2 12l3-4 3 2 3-6 3 3M2 14h12',
  book:        'M4 2h9a1 1 0 011 1v11a1 1 0 01-1 1H4a2 2 0 01-2-2V4a2 2 0 012-2zm0 0v12m4-8h3m-3 3h3',
  lock:        'M5 7V5a3 3 0 016 0v2m-7 0h8a1 1 0 011 1v5a1 1 0 01-1 1H4a1 1 0 01-1-1V8a1 1 0 011-1z',
  pie:         'M8 2v6h6a6 6 0 11-6-6zm2 0a6 6 0 015 5h-5V2z',
  graduation:  'M8 2L2 5l6 3 6-3-6-3zm-6 5v4c0 1.1 2.69 2 6 2s6-.9 6-2V7',
  zap:         'M9 2L4 9h5l-2 5 7-8H9l2-4z',
  flame:       'M10 2c0 3-2 4-2 6a2 2 0 004 0c0-2-1-3-1-4 1 1 2 3 2 5a4 4 0 01-8 0c0-4 3-6 5-7z',
  merge:       'M5 3v6a3 3 0 003 3h5M5 3a2 2 0 100 4 2 2 0 000-4zm8 7a2 2 0 100 4 2 2 0 000-4zM5 7v6m0 0a2 2 0 100 4 2 2 0 000-4z',
  file:        'M4 2h6l4 4v9a1 1 0 01-1 1H4a1 1 0 01-1-1V3a1 1 0 011-1zm6 0v4h4',
  activity:    'M1 8h3l2-5 2 10 2-7 2 4h3',
  repeat:      'M3 8V5a2 2 0 012-2h6m4 5V11a2 2 0 01-2 2H5m10-9l2 2-2 2M3 6L1 8l2 2',
  cpu:         'M5 2h6v6H5zm3-2v2m0 6v2M2 5h2m8 0h2M2 11h2m8 0h2M3 3l2 2M11 3l-2 2M3 13l2-2M11 13l-2-2',
  cloud:       'M12 11a4 4 0 00-8 0H3a3 3 0 000 6h10a3 3 0 000-6h-1zm-4-7a5 5 0 014.9 4H4.1A5 5 0 018 4z',
  package:     'M8 1.5L14 5v6L8 14.5 2 11V5L8 1.5zm0 0v13M2 5l6 3m0 0l6-3',
  alert:       'M8 3L2 13h12L8 3zm0 4v3m0 2v1',
  network:     'M8 3a2 2 0 100 4 2 2 0 000-4zm-5 8a2 2 0 100 4 2 2 0 000-4zm10 0a2 2 0 100 4 2 2 0 000-4zM8 7v2m-3 2l-1 1m8-1l1 1',
  gauge:       'M3 10a5 5 0 1110 0M8 5v1m0 4l2-2',
  hard:        'M3 4a1 1 0 011-1h8a1 1 0 011 1v8a1 1 0 01-1 1H4a1 1 0 01-1-1V4zm0 5h10M12 7a1 1 0 11-2 0 1 1 0 012 0z',
  flask:       'M6 2v5L3 12a2 2 0 004 0H9a2 2 0 004 0l-3-5V2M6 2h4',
  users:       'M5.5 6a2.5 2.5 0 110-5 2.5 2.5 0 010 5zm7 0a2.5 2.5 0 110-5 2.5 2.5 0 010 5zM1 14a4.5 4.5 0 019 0M8 14a4.5 4.5 0 019 0',
}

function Icon({ name, size = 15, className, style }: { name: string; size?: number; className?: string; style?: React.CSSProperties }) {
  const d = icons[name] || icons.database
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 16 16"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.5"
      strokeLinecap="round"
      strokeLinejoin="round"
      className={className}
      style={style}
    >
      <path d={d} />
    </svg>
  )
}

const NAV_GROUPS: NavGroup[] = [
  {
    id: 'foundation',
    label: 'Data Foundation',
    color: '#68D391',
    items: [
      { path: '/sources', label: 'Data Ingestion', icon: 'database', module: '01' },
      { path: '/quality', label: 'Data Quality', icon: 'shield', module: '02' },
      { path: '/pipeline', label: 'Pipeline Builder', icon: 'git', module: '03' },
      { path: '/modeling', label: 'Data Modeling', icon: 'layout', module: '04' },
      { path: '/orchestration', label: 'Orchestration', icon: 'clock', module: '05' },
    ],
  },
  {
    id: 'platform',
    label: 'Platform & Storage',
    color: '#63B3ED',
    items: [
      { path: '/storage', label: 'Storage Layers', icon: 'server', module: '06' },
      { path: '/monitoring', label: 'Monitoring', icon: 'activity', module: '07' },
      { path: '/catalog', label: 'Data Catalog', icon: 'book', module: '08' },
      { path: '/governance', label: 'Governance', icon: 'lock', module: '09' },
      { path: '/reporting', label: 'Reporting', icon: 'chart', module: '10' },
      { path: '/learn', label: 'Knowledge Base', icon: 'graduation', module: '11' },
      { path: '/bigdata', label: 'Big Data & Databricks', icon: 'zap', module: '12' },
    ],
  },
  {
    id: 'pipeline',
    label: 'Pipeline Engineering',
    color: '#F6AD55',
    items: [
      { path: '/cicd', label: 'CI/CD Pipelines', icon: 'merge', module: '13' },
      { path: '/contracts', label: 'Data Contracts', icon: 'file', module: '14' },
      { path: '/cdc', label: 'Change Data Capture', icon: 'repeat', module: '15' },
      { path: '/reversetl', label: 'Reverse ETL', icon: 'repeat', module: '16' },
      { path: '/featurestore', label: 'Feature Store & MLOps', icon: 'flask', module: '17' },
    ],
  },
  {
    id: 'advanced',
    label: 'Advanced Architecture',
    color: '#B794F4',
    items: [
      { path: '/cloud', label: 'Cloud-Native Services', icon: 'cloud', module: '18' },
      { path: '/iac', label: 'Infrastructure as Code', icon: 'package', module: '19' },
      { path: '/incidents', label: 'Incident Management', icon: 'alert', module: '20' },
      { path: '/datamesh', label: 'Data Mesh', icon: 'network', module: '21' },
      { path: '/olap', label: 'Real-Time OLAP', icon: 'gauge', module: '22' },
      { path: '/nosql', label: 'NoSQL & Polyglot', icon: 'hard', module: '23' },
    ],
  },
  {
    id: 'career',
    label: 'Career & Leadership',
    color: '#76E4F7',
    items: [
      { path: '/versioning', label: 'Data Versioning', icon: 'cpu', module: '24' },
      { path: '/capacity', label: 'Capacity Planning', icon: 'chart', module: '25' },
      { path: '/leadership', label: 'Leadership & Soft Skills', icon: 'users', module: '26' },
    ],
  },
]

function NavGroupSection({ group, defaultOpen }: { group: NavGroup; defaultOpen: boolean }) {
  const [open, setOpen] = useState(defaultOpen)
  const location = useLocation()
  const isGroupActive = group.items.some(i => i.path === location.pathname)

  return (
    <div className="mb-1">
      <button
        onClick={() => setOpen(o => !o)}
        className="w-full flex items-center justify-between px-3 py-2 label-section hover:text-[--text-secondary] transition-colors group"
      >
        <span className="flex items-center gap-2">
          <span
            className="w-1.5 h-1.5 rounded-full flex-shrink-0"
            style={{ backgroundColor: isGroupActive ? group.color : 'var(--text-tertiary)' }}
          />
          {group.label}
        </span>
        <motion.span
          animate={{ rotate: open ? 90 : 0 }}
          transition={{ duration: 0.15 }}
          className="text-[--text-tertiary]"
        >
          <svg width="10" height="10" viewBox="0 0 10 10" fill="none">
            <path d="M3 2l4 3-4 3" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/>
          </svg>
        </motion.span>
      </button>

      <AnimatePresence initial={false}>
        {open && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: 'auto', opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={{ duration: 0.2, ease: [0.2, 0, 0, 1] }}
            className="overflow-hidden"
          >
            <div className="pb-1">
              {group.items.map(item => (
                <NavLink
                  key={item.path}
                  to={item.path}
                  className={({ isActive }) => cn(
                    'flex items-center gap-2.5 px-3 h-9 rounded-[8px] mx-1 transition-all duration-150 group/item relative',
                    isActive
                      ? 'text-white shadow-[0_2px_12px_rgba(124,58,237,0.4)]'
                      : 'text-[--text-muted] hover:text-[--text-primary] hover:bg-black/[0.04]'
                  )}
                  style={({ isActive }: { isActive: boolean }) =>
                    isActive ? { background: 'linear-gradient(135deg, #4C1D95, #3730A3)', border: '1px solid rgba(167,139,250,0.4)', boxShadow: '0 4px 18px rgba(124,58,237,0.35), inset 0 1px 0 rgba(255,255,255,0.1)' } : undefined
                  }
                >
                  {({ isActive }) => (
                    <>
                      <Icon
                        name={item.icon}
                        size={14}
                        className="flex-shrink-0 transition-colors duration-150"
                        style={isActive ? { color: 'rgba(255,255,255,0.9)' } : { color: group.color }}
                      />
                      <span className="text-xs font-body font-medium truncate">{item.label}</span>
                      <span className={isActive
                        ? 'ml-auto font-mono text-[10px] text-white opacity-50'
                        : 'ml-auto font-mono text-[10px] text-[--text-dim] opacity-0 group-hover/item:opacity-100 transition-opacity'
                      }>
                        {item.module}
                      </span>
                    </>
                  )}
                </NavLink>
              ))}
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  )
}

export default function Sidebar() {
  return (
    <aside className="w-[220px] flex-shrink-0 flex flex-col border-r border-[--border-dim] overflow-hidden relative" style={{ background: "#E4E4E8" }}>
      {/* Logo */}
      <div className="px-4 py-4 flex-shrink-0 border-b border-[--border-dim] relative z-10">
        <div className="flex items-center gap-2.5">
          <div className="w-9 h-9 rounded-[11px] flex items-center justify-center flex-shrink-0" style={{ background: "linear-gradient(135deg, #7C3AED 0%, #4F46E5 100%)", boxShadow: "0 0 16px rgba(124,58,237,0.5)" }}>
            <span className="font-display font-bold text-[11px] text-white tracking-tight">DE</span>
          </div>
          <div>
            <div className="font-display font-bold text-sm text-[--text-primary] leading-tight">
              Workbench
              <span
                className="text-transparent bg-clip-text ml-1"
                style={{ backgroundImage: 'var(--gradient-iridescent)' }}
              >
                v2
              </span>
            </div>
            <div className="font-mono text-[10px] text-[--text-tertiary]">Data Engineer</div>
          </div>
        </div>
      </div>

      {/* Nav */}
      <nav className="flex-1 overflow-y-auto py-3 px-1 relative z-10">
        {NAV_GROUPS.map((group, i) => (
          <NavGroupSection key={group.id} group={group} defaultOpen={i < 2} />
        ))}
        <LearningPath />
      </nav>

      {/* Footer */}
      <div className="px-3 py-3 border-t border-[--border-dim] flex-shrink-0 relative z-10">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <div className="w-1.5 h-1.5 rounded-full bg-[--green] status-dot-running" />
            <span className="font-mono text-[10px] text-[--text-dim]">Backend live</span>
          </div>
          <div className="flex items-center gap-2">
            <div className="w-6 h-6 rounded-full flex items-center justify-center" style={{ background: "linear-gradient(135deg, #7C3AED 0%, #4F46E5 100%)" }}>
              <span className="font-display font-bold text-[9px] text-white">SR</span>
            </div>
            <span className="font-mono text-[10px] text-[--text-dim]">v2.0.0</span>
          </div>
        </div>
      </div>
    </aside>
  )
}
