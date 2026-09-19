import { fileURLToPath, URL } from 'node:url'

import vue from '@vitejs/plugin-vue'
import federation from '@originjs/vite-plugin-federation'
import { defineConfig } from 'vite'

export default defineConfig({
  plugins: [
    vue(),
    federation({
      name: 'SubscriptionManager',
      filename: 'remoteEntry.js',
      exposes: { './Config': './src/Config.vue' },
      shared: {
        vue: { requiredVersion: false, generate: false },
        vuetify: { requiredVersion: false, generate: false },
        'vuetify/styles': { requiredVersion: false, generate: false },
      },
    }),
  ],
  resolve: {
    alias: { '@': fileURLToPath(new URL('./src', import.meta.url)) },
  },
  build: {
    target: 'esnext',
    minify: false,
    cssCodeSplit: true,
    outDir: 'dist',
    emptyOutDir: true,
    rollupOptions: { input: 'src/main.js' },
  },
})
