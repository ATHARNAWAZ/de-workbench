import { useState } from 'react'
import { BookOpen, ChevronDown, ChevronUp, Lightbulb, Target, Wrench } from 'lucide-react'
import clsx from 'clsx'

interface ExplanationPanelProps {
  title: string
  what: string
  why: string
  how: string
  tools?: string[]
  seniorTip?: string
}

export default function ExplanationPanel({ title, what, why, how, tools, seniorTip }: ExplanationPanelProps) {
  const [open, setOpen] = useState(false)

  return (
    <div className="card mb-6 overflow-hidden">
      <button
        onClick={() => setOpen(!open)}
        className="w-full flex items-center justify-between px-5 py-4 text-left hover:bg-gray-800/50 transition-colors"
      >
        <div className="flex items-center gap-3">
          <div className="p-1.5 bg-amber-500/10 rounded-lg">
            <BookOpen size={16} className="text-amber-400" />
          </div>
          <div>
            <span className="text-sm font-semibold text-gray-200">{title}</span>
            <span className="ml-2 text-xs text-amber-400/80">How This Works</span>
          </div>
        </div>
        {open ? (
          <ChevronUp size={16} className="text-gray-500 flex-shrink-0" />
        ) : (
          <ChevronDown size={16} className="text-gray-500 flex-shrink-0" />
        )}
      </button>

      {open && (
        <div className="border-t border-gray-800 px-5 py-5 space-y-5 bg-gray-900/50 animate-slide-in">
          <div className="grid grid-cols-1 md:grid-cols-3 gap-5">
            <div className="space-y-2">
              <div className="flex items-center gap-2 text-xs font-semibold text-blue-400 uppercase tracking-wider">
                <Target size={12} />
                What it is
              </div>
              <p className="text-sm text-gray-400 leading-relaxed">{what}</p>
            </div>
            <div className="space-y-2">
              <div className="flex items-center gap-2 text-xs font-semibold text-green-400 uppercase tracking-wider">
                <Lightbulb size={12} />
                Why it matters
              </div>
              <p className="text-sm text-gray-400 leading-relaxed">{why}</p>
            </div>
            <div className="space-y-2">
              <div className="flex items-center gap-2 text-xs font-semibold text-purple-400 uppercase tracking-wider">
                <Wrench size={12} />
                How senior engineers do it
              </div>
              <p className="text-sm text-gray-400 leading-relaxed">{how}</p>
            </div>
          </div>

          {(tools || seniorTip) && (
            <div className="border-t border-gray-800 pt-4 flex flex-wrap gap-4 items-start">
              {tools && (
                <div className="flex flex-wrap gap-2 items-center">
                  <span className="text-xs text-gray-500 font-medium">Industry tools:</span>
                  {tools.map((tool) => (
                    <span key={tool} className="text-xs px-2 py-0.5 bg-gray-800 border border-gray-700 rounded-full text-gray-300">{tool}</span>
                  ))}
                </div>
              )}
              {seniorTip && (
                <div className="flex-1 bg-amber-900/10 border border-amber-800/30 rounded-lg px-4 py-3">
                  <div className="text-xs font-semibold text-amber-400 mb-1">Senior Engineer Tip</div>
                  <p className="text-xs text-gray-400 leading-relaxed">{seniorTip}</p>
                </div>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  )
}
