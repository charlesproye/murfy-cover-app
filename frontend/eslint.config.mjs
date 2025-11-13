import nextjs from '@next/eslint-plugin-next'
import typescript from '@typescript-eslint/eslint-plugin'
import typescriptParser from '@typescript-eslint/parser'
import js from '@eslint/js'
import globals from 'globals'

export default [
  {
    ignores: ['node_modules/**', '.next/**', 'dist/**'],
  },
  js.configs.recommended,
  {
    files: ['**/*.{js,jsx,ts,tsx}'],
    languageOptions: {
      ecmaVersion: 'latest',
      sourceType: 'module',
      parser: typescriptParser,
       globals: {
        ...globals.browser,
        NodeJS: true,
        process: true,
        "RequestInit": true
      },
      parserOptions: {
        ecmaFeatures: {
          jsx: true
        }
      }
    },
    plugins: {
      '@typescript-eslint': typescript,
      '@next/next': nextjs
    },
    rules: {
      'no-unused-vars': 'off',
      '@typescript-eslint/no-unused-vars': 'error',
      '@typescript-eslint/no-explicit-any': 'error',
      // '@typescript-eslint/explicit-function-return-type': ['error', {
      //   allowExpressions: true,
      //   allowTypedFunctionExpressions: true
      // }],
      'no-console': ['warn', { allow: ['warn', 'error'] }]
    }
  }
]
