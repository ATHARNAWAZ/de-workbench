import { cn } from '../../lib/utils'

type StatVariant = 'blue' | 'green' | 'amber' | 'purple' | 'cyan'

const accentGradients: Record<StatVariant, string> = {
  blue:   'linear-gradient(90deg, #3B82F6, #06B6D4)',
  green:  'linear-gradient(90deg, #10B981, #34D399)',
  amber:  'linear-gradient(90deg, #F59E0B, #FBBF24)',
  purple: 'linear-gradient(90deg, #7C3AED, #A78BFA)',
  cyan:   'linear-gradient(90deg, #06B6D4, #22D3EE)',
}

const deltaColors: Record<StatVariant, string> = {
  blue:   '#60A5FA',
  green:  '#34D399',
  amber:  '#FCD34D',
  purple: '#A78BFA',
  cyan:   '#22D3EE',
}

interface MiniBarProps {
  bars: number[]
  color: string
}

function MiniBars({ bars, color }: MiniBarProps) {
  const max = Math.max(...bars, 1)
  return (
    <div className="flex items-end gap-[2px]" style={{ height: 24 }}>
      {bars.map((v, i) => (
        <div
          key={i}
          className="w-[4px] rounded-[2px_2px_0_0] flex-shrink-0"
          style={{
            height: `${(v / max) * 24}px`,
            background: color,
            opacity: 0.45 + (i / bars.length) * 0.55,
          }}
        />
      ))}
    </div>
  )
}

interface StatCardProps {
  label: string
  value: string | number
  delta?: string
  variant?: StatVariant
  bars?: number[]
  icon?: React.ReactNode
  className?: string
}

export function StatCard({
  label, value, delta,
  variant = 'blue',
  bars = [],
  icon,
  className,
}: StatCardProps) {
  const color = deltaColors[variant]
  const accent = accentGradients[variant]

  return (
    <div
      className={cn('relative rounded-[14px] overflow-hidden transition-all duration-200 hover:-translate-y-0.5', className)}
      style={{
        background: '#FFFFFF',
        border: '1px solid rgba(0,0,0,0.10)',
        padding: '18px 20px',
      }}
      onMouseEnter={e => {
        (e.currentTarget as HTMLElement).style.borderColor = 'rgba(0,0,0,0.22)'
      }}
      onMouseLeave={e => {
        (e.currentTarget as HTMLElement).style.borderColor = 'rgba(0,0,0,0.10)'
      }}
    >
      {/* Top accent line */}
      <div
        className="absolute top-0 left-0 right-0 h-[2px] rounded-[14px_14px_0_0]"
        style={{ background: accent }}
      />

      <div className="flex items-start justify-between gap-2 mt-1">
        <div className="flex-1 min-w-0">
          {icon && <div className="mb-2 text-[--text-muted]">{icon}</div>}
          <p className="font-body text-[10px] font-bold tracking-[0.1em] uppercase mb-[11px]" style={{ color: '#999AAA' }}>
            {label}
          </p>
          <p className="font-display font-extrabold text-[26px] tracking-[-0.02em] leading-none mb-3 text-[#1A1A2E]">
            {value}
          </p>
          {delta && (
            <p className="font-mono text-[11px] font-medium" style={{ color }}>
              {delta}
            </p>
          )}
        </div>

        {bars.length > 0 && (
          <div className="flex-shrink-0 self-end">
            <MiniBars bars={bars} color={color} />
          </div>
        )}
      </div>
    </div>
  )
}
