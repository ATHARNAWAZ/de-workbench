/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      fontFamily: {
        display: ['Plus Jakarta Sans', 'sans-serif'],
        body: ['Plus Jakarta Sans', 'sans-serif'],
        mono: ['JetBrains Mono', 'Fira Code', 'Cascadia Code', 'monospace'],
      },
      colors: {
        gray: {
          50:  '#F0F0F3',
          100: '#E4E4E8',
          200: '#EAEAEE',
          300: '#F5F5F8',
          400: '#9999BB',
          500: '#666680',
          600: '#555570',
          700: '#2D2D44',
          800: '#1A1A2E',
          900: '#0F0F1A',
          950: '#06071A',
        },
        purple: {
          400: '#A78BFA',
          500: '#9D6EF8',
          600: '#7C3AED',
          700: '#6D28D9',
          800: '#5B21B6',
          900: '#4C1D95',
        },
        navy: {
          900: '#090B1A',
          800: '#0D1025',
          700: '#131629',
          600: '#161830',
          500: '#1C1F3A',
        },
      },
      backgroundImage: {
        'grad-purple':   'linear-gradient(135deg, #7C3AED 0%, #4F46E5 100%)',
        'grad-cyan':     'linear-gradient(135deg, #06B6D4 0%, #3B82F6 100%)',
        'grad-green':    'linear-gradient(135deg, #10B981 0%, #059669 100%)',
        'grad-pink':     'linear-gradient(135deg, #EC4899 0%, #8B5CF6 100%)',
        'grad-sidebar':  'linear-gradient(180deg, #1A1040 0%, #0D0E1C 100%)',
        'metric-purple': 'linear-gradient(135deg, #3B0764 0%, #4338CA 60%, #1E3A8A 100%)',
        'metric-cyan':   'linear-gradient(135deg, #134E4A 0%, #0E7490 50%, #1D4ED8 100%)',
        'metric-pink':   'linear-gradient(135deg, #831843 0%, #9333EA 60%, #4F46E5 100%)',
        'metric-green':  'linear-gradient(135deg, #065F46 0%, #0369A1 100%)',
      },
      boxShadow: {
        'btn-purple':  '0 4px 14px rgba(124,58,237,0.4)',
        'glow-purple': '0 0 20px rgba(124,58,237,0.35), 0 0 60px rgba(124,58,237,0.15)',
        'glow-cyan':   '0 0 20px rgba(6,182,212,0.25)',
        'glow-green':  '0 0 20px rgba(16,185,129,0.25)',
      },
      animation: {
        'pulse-slow': 'pulse 3s cubic-bezier(0.4, 0, 0.6, 1) infinite',
        'slide-in':   'slideIn 0.2s ease-out',
        'glow-pulse': 'glowPulse 3s ease-in-out infinite',
      },
      keyframes: {
        slideIn: {
          '0%':   { opacity: '0', transform: 'translateY(-8px)' },
          '100%': { opacity: '1', transform: 'translateY(0)' },
        },
        glowPulse: {
          '0%, 100%': { opacity: '0.6', transform: 'scale(1)' },
          '50%':      { opacity: '1',   transform: 'scale(1.1)' },
        },
      },
    },
  },
  plugins: [],
}
