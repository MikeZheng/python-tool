<template>
  <div class="min-h-screen bg-gray-100">
    <!-- Navigation -->
    <nav class="bg-white shadow-lg">
      <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div class="flex justify-between h-16">
          <div class="flex">
            <div class="flex-shrink-0 flex items-center">
              <span class="font-bold text-xl text-indigo-600">文件去重系统</span>
            </div>
            <div class="hidden sm:ml-6 sm:flex sm:space-x-8">
              <router-link 
                to="/" 
                class="nav-link border-b-2 border-transparent text-gray-500 hover:text-gray-700 inline-flex items-center px-1 pt-1 text-sm font-medium"
                active-class="border-indigo-500 text-gray-900"
              >
                首页
              </router-link>
              <router-link 
                to="/config" 
                class="nav-link border-b-2 border-transparent text-gray-500 hover:text-gray-700 inline-flex items-center px-1 pt-1 text-sm font-medium"
                active-class="border-indigo-500 text-gray-900"
              >
                配置
              </router-link>
              <router-link 
                to="/scan" 
                class="nav-link border-b-2 border-transparent text-gray-500 hover:text-gray-700 inline-flex items-center px-1 pt-1 text-sm font-medium"
                active-class="border-indigo-500 text-gray-900"
              >
                扫描目录
              </router-link>
              <router-link 
                to="/duplicates" 
                class="nav-link border-b-2 border-transparent text-gray-500 hover:text-gray-700 inline-flex items-center px-1 pt-1 text-sm font-medium"
                active-class="border-indigo-500 text-gray-900"
              >
                重复文件
              </router-link>
              <router-link 
                to="/history" 
                class="nav-link border-b-2 border-transparent text-gray-500 hover:text-gray-700 inline-flex items-center px-1 pt-1 text-sm font-medium"
                active-class="border-indigo-500 text-gray-900"
              >
                操作记录
              </router-link>
            </div>
          </div>
        </div>
      </div>
    </nav>

    <!-- Main Content -->
    <main class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
      <router-view v-slot="{ Component }">
        <transition name="fade" mode="out-in">
          <component :is="Component" />
        </transition>
      </router-view>
    </main>

    <!-- Toast Component -->
    <Toast />
  </div>
</template>

<script setup lang="ts">
import Toast from './components/common/Toast.vue';
</script>

<style>
/* Global styles */
body {
  margin: 0;
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Open Sans', 'Helvetica Neue', sans-serif;
}

/* Transition styles */
.fade-enter-active,
.fade-leave-active {
  transition: opacity 0.3s ease;
}

.fade-enter-from,
.fade-leave-to {
  opacity: 0;
}

/* Navigation link styles */
.nav-link.active {
  border-color: #4f46e5;
  color: #111827;
}
</style>