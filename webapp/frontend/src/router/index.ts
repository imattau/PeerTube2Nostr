import { createRouter, createWebHistory } from 'vue-router'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', redirect: '/overview' },
    { path: '/overview', name: 'overview', component: () => import('@/views/Overview.vue') },
    { path: '/queue', name: 'queue', component: () => import('@/views/Queue.vue') },
    { path: '/sources', name: 'sources', component: () => import('@/views/Sources.vue') },
    { path: '/relays', name: 'relays', component: () => import('@/views/Relays.vue') },
    { path: '/activity', name: 'activity', component: () => import('@/views/Activity.vue') },
    { path: '/diagnostics', name: 'diagnostics', component: () => import('@/views/Diagnostics.vue') },
    { path: '/preferences', name: 'preferences', component: () => import('@/views/Preferences.vue') },
    { path: '/wizard', name: 'wizard', component: () => import('@/views/SetupWizard.vue') },
  ],
})

export default router
