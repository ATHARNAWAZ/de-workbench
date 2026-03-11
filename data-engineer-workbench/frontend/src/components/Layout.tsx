import { useEffect } from 'react'
import { Outlet, useLocation } from 'react-router-dom'
import { motion, AnimatePresence } from 'framer-motion'
import Sidebar from './Sidebar'
import TopBar from './TopBar'

const PROGRESS_KEY = 'de_workbench_progress'

export default function Layout() {
  const location = useLocation()

  useEffect(() => {
    try {
      const raw = localStorage.getItem(PROGRESS_KEY)
      const visited: string[] = raw ? JSON.parse(raw) : []
      if (!visited.includes(location.pathname)) {
        visited.push(location.pathname)
        localStorage.setItem(PROGRESS_KEY, JSON.stringify(visited))
      }
    } catch { /* noop */ }
  }, [location.pathname])

  return (
    <div className="flex h-screen overflow-hidden" style={{ backgroundColor: 'var(--bg-void)' }}>
      <Sidebar />
      <div className="flex flex-col flex-1 min-w-0 overflow-hidden">
        <TopBar />
        <main className="flex-1 overflow-y-auto">
          <AnimatePresence mode="wait">
            <motion.div
              key={location.pathname}
              initial={{ opacity: 0, y: 8 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -4 }}
              transition={{ duration: 0.24, ease: [0.2, 0, 0, 1] }}
              className="h-full p-8"
            >
              <Outlet />
            </motion.div>
          </AnimatePresence>
        </main>
      </div>
    </div>
  )
}
