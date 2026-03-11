import { cn } from '../../lib/utils'

type BadgeVariant = 'running' | 'success' | 'failed' | 'pending' | 'warning' | 'info' | 'purple'

const badgeStyles: Record<BadgeVariant, string> = {
  running:  'bg-[rgba(124,58,237,0.12)] text-[--purple-light] border border-[rgba(124,58,237,0.25)]',
  success:  'bg-[rgba(16,185,129,0.1)] text-[--text-success] border border-[rgba(16,185,129,0.2)]',
  failed:   'bg-[rgba(239,68,68,0.1)] text-[--text-danger] border border-[rgba(239,68,68,0.2)]',
  pending:  'bg-black/[0.04] text-[--text-muted] border border-[--border-soft]',
  warning:  'bg-[rgba(245,158,11,0.1)] text-[--text-warning] border border-[rgba(245,158,11,0.2)]',
  info:     'bg-[rgba(6,182,212,0.1)] text-[--cyan] border border-[rgba(6,182,212,0.2)]',
  purple:   'bg-[rgba(124,58,237,0.15)] text-[--purple-light] border border-[rgba(124,58,237,0.3)]',
}

interface BadgeProps {
  variant?: BadgeVariant
  children: React.ReactNode
  className?: string
  pulse?: boolean
}

export function Badge({ variant = 'pending', children, className, pulse }: BadgeProps) {
  return (
    <span className={cn(
      'inline-flex items-center gap-1.5 px-2 py-0.5 rounded-[6px]',
      'font-mono text-[11px] uppercase tracking-wide',
      badgeStyles[variant],
      className
    )}>
      {pulse && variant === 'running' && (
        <span className="w-1 h-1 rounded-full bg-current status-dot-running flex-shrink-0" />
      )}
      {children}
    </span>
  )
}
