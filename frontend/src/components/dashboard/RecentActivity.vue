<template>
  <div>
    <div v-if="operations.length === 0" class="text-center text-gray-500 py-4">
      暂无操作记录
    </div>
    <div v-else class="space-y-2">
      <div 
        v-for="operation in operations" 
        :key="operation.id"
        class="flex items-center justify-between py-2 border-b border-gray-100 last:border-0"
      >
        <div>
          <div class="text-sm font-medium text-gray-900">去重操作</div>
          <div class="text-xs text-gray-500">{{ formatDate(operation.created_at) }}</div>
        </div>
        <div class="text-sm text-green-600">+{{ formatFileSize(operation.space_saved) }}</div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import { useDashboardStore } from '../../stores/dashboard';
import { formatFileSize } from '../../utils/format';

const dashboardStore = useDashboardStore();

const operations = computed(() => dashboardStore.recentActivity);

const formatDate = (dateString: string): string => {
  return new Date(dateString).toLocaleString('zh-CN');
};
</script>