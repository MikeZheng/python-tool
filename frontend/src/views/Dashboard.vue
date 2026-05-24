<template>
  <div class="p-6">
    <h1 class="text-3xl font-bold text-gray-900 mb-6">仪表盘</h1>

    <!-- Stats Cards -->
    <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6 mb-6">
      <StatCard title="重复文件组" :value="stats.duplicate_groups" color="text-indigo-600" />
      <StatCard title="扫描目录数" :value="stats.scanned_directories" color="text-green-600" />
      <StatCard title="照片数量" :value="stats.photo_count" color="text-blue-600" />
      <StatCard title="视频数量" :value="stats.video_count" color="text-purple-600" />
    </div>

    <!-- Additional Stats -->
    <div class="grid grid-cols-1 sm:grid-cols-3 gap-6 mb-6">
      <StatCard title="总文件数" :value="stats.total_files" color="text-gray-900" size="text-2xl" />
      <StatCard title="重复文件总数" :value="stats.duplicate_files" color="text-orange-600" size="text-2xl" />
      <StatCard title="已释放空间" :value="formatFileSize(stats.space_saved)" color="text-emerald-600" size="text-2xl" />
    </div>

    <!-- Recent Activity -->
    <div class="bg-white rounded-lg shadow">
      <div class="px-6 py-4 border-b border-gray-200">
        <h2 class="text-lg font-semibold text-gray-900">最近操作</h2>
      </div>
      <div class="px-6 py-4">
        <RecentActivity />
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted } from 'vue';
import { useDashboardStore } from '../stores/dashboard';
import { formatFileSize } from '../utils/format';
import StatCard from '../components/dashboard/StatCard.vue';
import RecentActivity from '../components/dashboard/RecentActivity.vue';

const dashboardStore = useDashboardStore();

const stats = computed(() => dashboardStore.stats);


onMounted(() => {
  Promise.all([
    dashboardStore.fetchStats(),
    dashboardStore.fetchRecentActivity()
  ]);
});
</script>