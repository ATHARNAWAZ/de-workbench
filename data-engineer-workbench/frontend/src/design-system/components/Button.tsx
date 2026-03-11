import { forwardRef } from 'react'
import { cva, type VariantProps } from 'class-variance-authority'
import { cn } from '../../lib/utils'

const buttonVariants = cva(
  'inline-flex items-center justify-center gap-2 font-body text-sm font-medium transition-all duration-[120ms] focus-visible:outline-none disabled:opacity-40 disabled:pointer-events-none select-none',
  {
    variants: {
      variant: {
        primary: [
          'text-white rounded-[10px] px-4 py-[7px] border-0',
          'shadow-btn-purple',
          'hover:brightness-110 hover:-translate-y-px hover:shadow-glow-purple',
          'active:translate-y-0 active:brightness-95',
        ],
        ghost: [
          'bg-transparent text-[--text-secondary]',
          'rounded-[10px] px-4 py-[7px]',
          'border border-[--border-soft]',
          'hover:border-[--border-loud] hover:text-[--text-primary] hover:bg-black/[0.04]',
        ],
        danger: [
          'bg-transparent text-[--text-danger]',
          'rounded-[10px] px-4 py-[7px]',
          'border border-[rgba(239,68,68,0.25)]',
          'hover:bg-[rgba(239,68,68,0.08)]',
        ],
        subtle: [
          'bg-black/[0.04] text-[--text-secondary]',
          'rounded-[10px] px-4 py-[7px]',
          'border border-[--border-dim]',
          'hover:bg-black/[0.07] hover:text-[--text-primary]',
        ],
      },
      size: {
        sm: 'text-xs px-3 py-1.5 rounded-[8px]',
        md: 'text-sm px-4 py-[7px]',
        lg: 'text-base px-5 py-2.5',
      },
    },
    defaultVariants: {
      variant: 'ghost',
      size: 'md',
    },
  }
)

interface ButtonProps
  extends React.ButtonHTMLAttributes<HTMLButtonElement>,
    VariantProps<typeof buttonVariants> {
  loading?: boolean
}

export const Button = forwardRef<HTMLButtonElement, ButtonProps>(
  ({ className, variant, size, loading, children, disabled, style, ...props }, ref) => (
    <button
      ref={ref}
      disabled={disabled || loading}
      className={cn(buttonVariants({ variant, size }), className)}
      style={
        variant === 'primary'
          ? { background: 'linear-gradient(135deg, #7C3AED 0%, #4F46E5 100%)', ...style }
          : style
      }
      {...props}
    >
      {loading ? (
        <span className="flex items-center gap-1">
          {[0, 1, 2].map(i => (
            <span
              key={i}
              className="w-1 h-1 rounded-full bg-current animate-pulse"
              style={{ animationDelay: `${i * 150}ms` }}
            />
          ))}
        </span>
      ) : children}
    </button>
  )
)
Button.displayName = 'Button'
