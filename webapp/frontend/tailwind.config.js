/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{vue,js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        'bg': '#0a0a0a',
        'surface-1': '#111111',
        'surface-2': '#181818',
        'border-subtle': 'rgba(255, 255, 255, 0.05)',
        'text-primary': '#f1f1f1',
        'text-muted': '#888888',
        'accent': '#4f46e5',
      },
      borderRadius: {
        'lg': '12px',
      },
      spacing: {
        '3.5': '14px',
        '4.5': '18px',
        '5.5': '22px',
      }
    },
  },
  plugins: [],
}