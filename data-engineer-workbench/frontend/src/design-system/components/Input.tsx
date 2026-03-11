import { forwardRef } from 'react'
import { cn } from '../../lib/utils'

interface InputProps extends React.InputHTMLAttributes<HTMLInputElement> {
  label?: string
  error?: string
}

export const Input = forwardRef<HTMLInputElement, InputProps>(
  ({ className, label, error, id, ...props }, ref) => (
    <div className="flex flex-col gap-1.5">
      {label && (
        <label htmlFor={id} className="label-section">
          {label}
        </label>
      )}
      <input
        ref={ref}
        id={id}
        className={cn(
          'h-[34px] w-full rounded-[5px] px-3',
          'bg-[--bg-void] border border-[--border-soft]',
          'font-body text-sm text-[--text-primary]',
          'placeholder:text-[--text-tertiary]',
          'transition-all duration-150',
          'focus:outline-none focus:border-[--accent-primary] focus:shadow-[0_0_0_3px_rgba(99,179,237,0.12)]',
          error && 'border-[rgba(252,129,129,0.5)]',
          className
        )}
        {...props}
      />
      {error && <p className="text-xs text-[--text-danger]">{error}</p>}
    </div>
  )
)
Input.displayName = 'Input'
