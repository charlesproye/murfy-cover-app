/** @type {import('tailwindcss').Config} */
const colors = require('tailwindcss/colors');

module.exports = {
  content: [
    './src/pages/**/*.{js,ts,jsx,tsx,mdx}',
    './src/components/**/*.{js,ts,jsx,tsx,mdx}',
    './src/app/**/*.{js,ts,jsx,tsx,mdx}',
  ],
  darkMode: ['class', 'class'],
  theme: {
  	extend: {
  		keyframes: {
  			vibrate: {
  				'0%, 100%': {
  					transform: 'translateX(0)'
  				},
  				'25%': {
  					transform: 'translateX(-4px)'
  				},
  				'50%': {
  					transform: 'translateX(4px)'
  				},
  				'75%': {
  					transform: 'translateX(-4px)'
  				}
  			},
  			loading: {
  				'0%': {
  					transform: 'translateX(-100%)'
  				},
  				'100%': {
  					transform: 'translateX(100%)'
  				}
  			}
  		},
  		animation: {
  			vibrate: 'vibrate 0.3s linear',
  			loading: 'loading 1s linear infinite'
  		},
  		colors: {
                ...colors,
  			primary: {
  				DEFAULT: '#007AFF',
  				foreground: '#ffffff'
  			},
  			'green-rapport': '#108149',
  			'green-rapport-light': '#43AB69',
  			'green-rapport-extra-light': '#5EDF8E',
  			'primary-gray': '#9197B3',
  			'gray-background': '#f1f1f4',
  			white: '#ffffff',
  			'white-ghost': '#f8f8ff',
  			black: '#000000',
  			gray: '#79798d',
  			'white-clair': '#e5f0ff',
  			'gray-light': '#bcbccb',
  			'gray-purple': '#615e83',
  			'gray-blue': '#9291a5',
  			'bleu-vif': '#4993ff',
  			blue: '#3498db',
  			'blue-almostFilled': '#2d67f6',
  			'blue-lessFilled': '#a1c3fa',
  			red: '#F87171',
  			'red-price': '#ef4444',
  			green: '#00ff00',
  			'green-500': '#22c55e',
  			'green-400': '#4ade80',
  			'yellow-500': '#eab308',
  			'orange-500': '#f97316',
  			'orange-600': '#ea580c',
  			'red-500': '#ef4444',
  			'green-price': '#22c55e',
  			success: '#008a2e',
  			'success-bg': '#ecfdf3',
  			warning: '#dc7609',
  			'warning-bg': '#fffcf0',
  			error: '#e60000',
  			'error-bg': '#fff0f0',
  			'dark-gray-background': '#18181b',
  			'dark-box-background': '#2e2e38',
  			'dark-dark': '#ffffff',
  			'dark-white': '#000000',
  			'dark-gray': '#c2c2d6',
  			'dark-white-clair': '#2a3b55',
  			'dark-gray-light': '#d3d3dc',
  			'dark-bleu-vif': '#376ad1',
  			'dark-black': '#ffffff',
  			'dark-success': '#59f3a6',
  			'dark-success-bg': '#001f0f',
  			'dark-warning': '#f3cf58',
  			'dark-warning-bg': '#1d1f00',
  			'dark-error': '#ff9ea1',
  			'dark-error-bg': '#2d0607',
  			background: 'hsl(var(--background))',
  			foreground: 'hsl(var(--foreground))',
  			card: {
  				DEFAULT: 'hsl(var(--card))',
  				foreground: 'hsl(var(--card-foreground))'
  			},
  			popover: {
  				DEFAULT: 'hsl(var(--popover))',
  				foreground: 'hsl(var(--popover-foreground))'
  			},
  			secondary: {
  				DEFAULT: 'hsl(var(--secondary))',
  				foreground: 'hsl(var(--secondary-foreground))'
  			},
  			muted: {
  				DEFAULT: 'hsl(var(--muted))',
  				foreground: 'hsl(var(--muted-foreground))'
  			},
  			accent: {
  				DEFAULT: 'hsl(var(--accent))',
  				foreground: 'hsl(var(--accent-foreground))'
  			},
  			destructive: {
  				DEFAULT: 'hsl(var(--destructive))',
  				foreground: 'hsl(var(--destructive-foreground))'
  			},
  			border: 'hsl(var(--border))',
  			input: 'hsl(var(--input))',
  			ring: 'hsl(var(--ring))',
  			chart: {
  				'1': 'hsl(var(--chart-1))',
  				'2': 'hsl(var(--chart-2))',
  				'3': 'hsl(var(--chart-3))',
  				'4': 'hsl(var(--chart-4))',
  				'5': 'hsl(var(--chart-5))'
  			}
  		},
  		blur: {
  			feature: '7px'
  		},
  		screens: {
  			mobile: '640px',
  			tablet: '768px',
  			laptop: '1024px',
  			mac: '1180px',
  			desktop: '1280px',
  			wide: '1536px'
  		},
  		borderRadius: {
  			lg: 'var(--radius)',
  			md: 'calc(var(--radius) - 2px)',
  			sm: 'calc(var(--radius) - 4px)'
  		}
  	},
  	fontFamily: {
  		poppins: [
  			'Poppins'
  		]
  	}
  },
  plugins: [],
};
