/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        // VS Code Dark Theme Colors
        vscode: {
          bg: '#1e1e1e',
          'bg-light': '#252526',
          'bg-lighter': '#2d2d30',
          sidebar: '#252526',
          border: '#3c3c3c',
          accent: '#007acc',
          'accent-hover': '#1a8ad4',
          text: '#d4d4d4',
          'text-muted': '#808080',
          'text-bright': '#ffffff',
          success: '#4ec9b0',
          warning: '#dcdcaa',
          error: '#f14c4c',
          info: '#569cd6',
        }
      },
      fontFamily: {
        mono: ['Consolas', 'Monaco', 'Courier New', 'monospace'],
      }
    },
  },
  plugins: [],
}
