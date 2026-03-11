import { forwardRef } from 'react'
import { cn } from '../../lib/utils'

interface CardProps extends React.HTMLAttributes<HTMLDivElement> {
  accent?: 'none' | 'blue' | 'green' | 'amber' | 'danger' | 'iris' | 'purple' | 'cyan' | 'iridescent'
  hover?: boolean
  noise?: boolean
}

const accentColors = {
  none:        '',
  blue:        'before:bg-[--blue]',
  green:       'before:bg-[--green]',
  amber:       'before:bg-[--amber]',
  danger:      'before:bg-[--text-danger]',
  iris:        'before:bg-[--purple-light]',
  purple:      'before:bg-[--purple]',
  cyan:        'before:bg-[--cyan]',
  iridescent:  'before:bg-gradient-to-r before:from-[--purple] before:via-[--purple-light] before:to-[--cyan]',
}

export const Card = forwardRef<HTMLDivElement, CardProps>(
  ({ className, accent = 'none', hover = false, noise = false, children, ...props }, ref) => (
    <div
      ref={ref}
      className={cn(
        'relative rounded-[14px] overflow-hidden',
        accent !== 'none' && [
          'before:absolute before:top-0 before:left-0 before:right-0 before:h-[2px] before:z-10 before:rounded-[14px_14px_0_0]',
          accentColors[accent],
        ],
        hover && 'transition-all duration-200 hover:-translate-y-0.5 cursor-pointer',
        noise && 'noise-overlay',
        className
      )}
      style={{ background: '#FFFFFF', border: '1px solid rgba(0,0,0,0.10)', ...props.style }}
      onMouseEnter={hover ? e => {
        const el = e.currentTarget as HTMLElement
        el.style.borderColor = 'rgba(167,139,250,0.35)'
        el.style.boxShadow = '0 4px 24px rgba(0,0,0,0.4)'
      } : undefined}
      onMouseLeave={hover ? e => {
        const el = e.currentTarget as HTMLElement
        el.style.borderColor = 'rgba(0,0,0,0.10)'
        el.style.boxShadow = 'none'
      } : undefined}
      {...props}
    >
      {children}
    </div>
  )
)
Card.displayName = 'Card'

export function CardHeader({ className, ...props }: React.HTMLAttributes<HTMLDivElement>) {
  return <div className={cn('px-5 py-4 border-b border-[--border-dim]', className)} {...props} />
}

export function CardBody({ className, ...props }: React.HTMLAttributes<HTMLDivElement>) {
  return <div className={cn('p-5', className)} {...props} />
}
