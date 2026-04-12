import { createRouter, createWebHistory } from 'vue-router';

const router = createRouter({
  history: createWebHistory(),
  routes: [
    {
      path: '/',
      name: 'Dashboard',
      component: () => import('../views/Dashboard.vue')
    },
    {
      path: '/config',
      name: 'Config',
      component: () => import('../views/Config.vue')
    },
    {
      path: '/scan',
      name: 'Scan',
      component: () => import('../views/TaskManager.vue')
    },
    {
      path: '/duplicates',
      name: 'Duplicates',
      component: () => import('../views/Duplicates.vue')
    },
    {
      path: '/history',
      name: 'History',
      component: () => import('../views/History.vue')
    }
  ]
});

export default router;