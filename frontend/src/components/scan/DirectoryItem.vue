<template>
  <div class="px-6 py-4 flex items-center justify-between hover:bg-gray-50">
    <div class="flex-1">
      <div class="text-sm font-medium text-gray-900 truncate">{{ directory.directory_path }}</div>
      <div class="text-xs text-gray-500 mt-1">
        文件: {{ directory.total_files }} | 照片: {{ directory.photo_count }} | 视频: {{ directory.video_count }} | 重复: {{ directory.duplicate_count }}
      </div>
      <div class="text-xs text-gray-400">扫描时间: {{ formatDate(directory.scanned_at) }}</div>
    </div>
    <div class="flex gap-2">
      <button 
        @click="$emit('rescan', directory.id)"
        class="px-3 py-1 bg-blue-100 hover:bg-blue-200 text-blue-700 text-sm rounded"
      >
        重新扫描
      </button>
      <button 
        @click="$emit('delete', directory.id)"
        class="px-3 py-1 bg-red-100 hover:bg-red-200 text-red-700 text-sm rounded"
      >
        删除
      </button>
    </div>
  </div>
</template>

<script setup lang="ts">
import type { Directory } from '../../types';

const props = defineProps<{
  directory: Directory;
}>();

defineEmits<{
  (e: 'rescan', directoryId: number): void;
  (e: 'delete', directoryId: number): void;
}>();

const formatDate = (dateString: string): string => {
  return new Date(dateString).toLocaleString('zh-CN');
};
</script>