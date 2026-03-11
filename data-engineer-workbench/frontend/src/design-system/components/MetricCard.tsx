import { cn } from '../../lib/utils'

type MetricVariant = 'purple' | 'cyan' | 'pink' | 'green'

// v3 hero card gradients (from de-workbench-v3 reference)
const gradients: Record<MetricVariant, string> = {
  purple: 'linear-gradient(135deg, #3B0764 0%, #4338CA 60%, #1E3A8A 100%)',
  cyan:   'linear-gradient(135deg, #134E4A 0%, #0E7490 50%, #1D4ED8 100%)',
  pink:   'linear-gradient(135deg, #831843 0%, #9333EA 60%, #4F46E5 100%)',
  green:  'linear-gradient(135deg, #064E3B 0%, #065F46 50%, #0E7490 100%)',
}

const boxShadows: Record<MetricVariant, string> = {
  purple: '0 6px 32px rgba(76,29,149,0.45)',
  cyan:   '0 6px 32px rgba(6,182,212,0.3)',
  pink:   '0 6px 32px rgba(236,72,153,0.3)',
  green:  '0 6px 32px rgba(16,185,129,0.3)',
}

const chartColors: Record<MetricVariant, string> = {
  purple: '#C4B5FD',
  cyan:   '#67E8F9',
  pink:   '#F9A8D4',
  green:  '#6EE7B7',
}

const chartFills: Record<MetricVariant, { from: string; to: string }> = {
  purple: { from: 'rgba(196,181,253,0.5)', to: 'rgba(196,181,253,0)' },
  cyan:   { from: 'rgba(103,232,249,0.5)', to: 'rgba(103,232,249,0)' },
  pink:   { from: 'rgba(249,168,212,0.5)', to: 'rgba(249,168,212,0)' },
  green:  { from: 'rgba(110,231,183,0.5)', to: 'rgba(110,231,183,0)' },
}

interface AreaChartProps {
  data: number[]
  variant: MetricVariant
  height?: number
  id: string
}

function AreaChart({ data, variant, height = 58, id }: AreaChartProps) {
  if (data.length < 2) return null
  const W = 300
  const h = height
  const min = Math.min(...data)
  const max = Math.max(...data)
  const range = max - min || 1
  const pts = data.map((v, i) => {
    const x = (i / (data.length - 1)) * W
    const y = h - ((v - min) / range) * h * 0.82 - 4
    return [x, y] as [number, number]
  })
  const linePath = pts.map(([x, y], i) => `${i === 0 ? 'M' : 'L'}${x},${y}`).join(' ')
  const areaPath = `${linePath} L${W},${h} L0,${h} Z`
  const { from, to } = chartFills[variant]
  const color = chartColors[variant]
  const gradId = `area-${id}`

  return (
    <svg viewBox={`0 0 ${W} ${h}`} width="100%" height={h} preserveAspectRatio="none">
      <defs>
        <linearGradient id={gradId} x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor={from} stopOpacity="1" />
          <stop offset="100%" stopColor={to} stopOpacity="1" />
        </linearGradient>
      </defs>
      <path d={areaPath} fill={`url(#${gradId})`} />
      <path d={linePath} fill="none" stroke={color} strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  )
}

interface MetricCardProps {
  label: string
  value: string | number
  sub?: string
  variant?: MetricVariant
  data?: number[]
  id?: string
  icon?: React.ReactNode
  months?: string[]
  className?: string
}

export function MetricCard({
  label, value, sub, variant = 'purple',
  data = [], id = 'mc', icon, months,
  className,
}: MetricCardProps) {
  return (
    <div
      className={cn('relative rounded-[16px] p-[22px] overflow-hidden cursor-default transition-transform duration-180', className)}
      style={{
        background: gradients[variant],
        boxShadow: boxShadows[variant],
        border: '1px solid rgba(255,255,255,0.12)',
      }}
      onMouseEnter={e => (e.currentTarget.style.transform = 'translateY(-3px)')}
      onMouseLeave={e => (e.currentTarget.style.transform = 'translateY(0)')}
    >
      {/* Glass sheen overlay */}
      <div
        className="absolute inset-0 pointer-events-none rounded-[16px]"
        style={{ background: 'linear-gradient(135deg, rgba(255,255,255,0.1) 0%, transparent 55%)' }}
      />

      <div className="relative z-10 flex items-start justify-between mb-[14px]">
        <div className="flex-1 min-w-0">
          <p className="font-body text-[11px] font-semibold text-white/55 uppercase tracking-[0.05em] mb-[7px]">{label}</p>
          <p className="font-display font-extrabold text-[30px] text-white leading-none tracking-[-0.03em]">{value}</p>
          {sub && <p className="font-mono text-[11px] text-white/40 mt-[6px]">{sub}</p>}
        </div>
        {icon && (
          <div className="opacity-90 flex-shrink-0">{icon}</div>
        )}
      </div>

      {data.length > 1 && (
        <div className="relative z-10">
          <AreaChart data={data} variant={variant} id={id} />
          {months && months.length > 0 && (
            <div className="flex justify-between mt-[5px]">
              {months.map(m => (
                <span key={m} className="font-mono text-[9px] text-white/30">{m}</span>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  )
}
