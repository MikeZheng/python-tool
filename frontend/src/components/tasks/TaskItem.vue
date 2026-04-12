<template>
  <div class="px-6 py-4 flex items-center justify-between hover:bg-gray-50 transition-all duration-200">
    <div class="flex-1">
      <div class="flex items-center gap-2 mb-1">
        <div class="w-3 h-3 rounded-full" :class="getStatusColor(task.status)"></div>
        <div class="text-sm font-medium text-gray-900 truncate">{{ task.directory_path }}</div>
      </div>
      <div class="text-xs text-gray-500 mt-1 flex flex-wrap gap-2">
        <span :class="getStatusColor(task.status)">状态: {{ getStatusText(task.status) }}</span>
        <span v-if="task.total_files > 0">文件: {{ task.total_files }}</span>
        <span v-if="task.processed_files > 0">已处理: {{ task.processed_files }}</span>
        <span v-if="task.error_message" class="text-red-500">错误: {{ task.error_message }}</span>
      </div>
      <div class="text-xs text-gray-400 flex flex-wrap gap-2 mt-1">
        <span v-if="task.scan_started_at">开始: {{ formatDate(task.scan_started_at) }}</span>
        <span v-if="task.scan_ended_at">结束: {{ formatDate(task.scan_ended_at) }}</span>
        <span v-if="task.cancelled_at">作废: {{ formatDate(task.cancelled_at) }}</span>
        <span v-if="task.scan_started_at && task.scan_ended_at">耗时: {{ calculateDuration(task.scan_started_at, task.scan_ended_at) }}</span>
      </div>
    </div>
    <div class="flex gap-2">
      <button 
        v-if="task.status === 'failed'"
        @click="$emit('retry', task.id)"
        class="px-3 py-1 bg-blue-100 hover:bg-blue-200 text-blue-700 text-sm rounded transition-colors"
      >
        重试
      </button>
      <button 
        v-if="task.status !== 'completed' && task.status !== 'failed' && task.status !== 'cancelled'"
        @click="$emit('cancel', task.id)"
        class="px-3 py-1 bg-orange-100 hover:bg-orange-200 text-orange-700 text-sm rounded transition-colors"
      >
        作废
      </button>
      <button 
        v-if="task.status !== 'running'"
        @click="$emit('delete', task.id)"
        class="px-3 py-1 bg-red-100 hover:bg-red-200 text-red-700 text-sm rounded transition-colors"
      >
        删除
      </button>
    </div>
  </div>
</template>

<script setup lang="ts">
import type { Task } from '../../types';

const props = defineProps<{
  task: Task;
}>();

defineEmits<{
  (e: 'retry', taskId: number): void;
  (e: 'delete', taskId: number): void;
  (e: 'cancel', taskId: number): void;
}>();

const getStatusText = (status: string): string => {
  const statusMap = {
    'queued': '排队中',
    'running': '运行中',
    'paused': '已暂停',
    'completed': '已完成',
    'failed': '失败',
    'cancelled': '已作废'
  };
  return statusMap[status as keyof typeof statusMap] || status;
};

const getStatusColor = (status: string): string => {
  const colorMap = {
    'queued': 'bg-yellow-400',
    'running': 'bg-blue-400',
    'paused': 'bg-purple-400',
    'completed': 'bg-green-400',
    'failed': 'bg-red-400',
    'cancelled': 'bg-orange-400'
  };
  return colorMap[status as keyof typeof colorMap] || 'bg-gray-400';
};

const formatDate = (dateString: string): string => {
  return new Date(dateString).toLocaleString('zh-CN');
};

const calculateDuration = (start: string, end: string): string => {
  const startDate = new Date(start);
  const endDate = new Date(end);
  const duration = endDate.getTime() - startDate.getTime();
  const seconds = Math.floor(duration / 1000);
  const minutes = Math.floor(seconds / 60);
  const remainingSeconds = seconds % 60;
  return `${minutes}分${remainingSeconds}秒`;
};
</script>