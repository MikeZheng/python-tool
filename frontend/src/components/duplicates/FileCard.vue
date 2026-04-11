<template>
  <div :class="['border rounded-lg overflow-hidden', isEarliest ? 'border-green-500 bg-green-50' : 'border-gray-200']">
    <div v-if="isImage" class="h-32 bg-gray-100 relative">
      <img 
        :src="file.filepath" 
        :alt="file.filename"
        class="w-full h-full object-cover"
        @error="handleImageError"
      >
    </div>
    <div v-else-if="isVideo" class="h-32 bg-gray-800 flex items-center justify-center text-white">
      <svg class="w-12 h-12" fill="currentColor" viewBox="0 0 20 20">
        <path d="M2 6a2 2 0 012-2h6a2 2 0 012 2v8a2 2 0 01-2 2H4a2 2 0 01-2-2V6zM14.553 7.106A1 1 0 0014 8v4a1 1 0 001 1h3l4 4V8.618a1 1 0 01-.563-.894L14.553 7.106zM15 8v4H5V8h10z"/>
      </svg>
    </div>
    <div v-else class="h-32 bg-gray-100 flex items-center justify-center text-gray-400">
      <svg class="w-12 h-12" fill="currentColor" viewBox="0 0 20 20">
        <path fill-rule="evenodd" d="M4 4a2 2 0 012-2h4.586A2 2 0 0112 2.586L15.414 6A2 2 0 0116 7.414V16a2 2 0 01-2 2H6a2 2 0 01-2-2V4zm2 6a1 1 0 011 1v6a1 1 0 11-2 0V11a1 1 0 011-1zm6-6h-6v6h6V5z" clip-rule="evenodd"/>
      </svg>
    </div>
    <div class="p-3">
      <div class="flex items-center gap-2 mb-2">
        <span v-if="isEarliest" class="px-2 py-0.5 bg-green-100 text-green-700 text-xs rounded-full">
          最早
        </span>
        <span :class="[
          'px-2 py-0.5 text-xs rounded-full',
          file.file_type === 'photo' ? 'bg-blue-100 text-blue-700' :
          file.file_type === 'video' ? 'bg-purple-100 text-purple-700' :
          'bg-gray-100 text-gray-700'
        ]">
          {{ file.file_type === 'photo' ? '照片' : file.file_type === 'video' ? '视频' : '其他' }}
        </span>
      </div>
      <div class="text-sm font-medium text-gray-900 truncate" :title="file.filename">
        {{ file.filename }}
      </div>
      <div class="text-xs text-gray-500 truncate" :title="file.filepath">
        {{ file.filepath }}
      </div>
      <div class="text-xs text-gray-400 mt-1">
        {{ formatFileSize(file.file_size) }}
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import type { File } from '../../types';

const props = defineProps<{
  file: File;
  isEarliest: boolean;
}>();

const isImage = computed(() => {
  const ext = props.file.filename.toLowerCase().substring(props.file.filename.lastIndexOf('.'));
  return ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.tiff'].includes(ext);
});

const isVideo = computed(() => {
  const ext = props.file.filename.toLowerCase().substring(props.file.filename.lastIndexOf('.'));
  return ['.mp4', '.mov', '.avi', '.mkv', '.wmv'].includes(ext);
});

const handleImageError = (event: Event) => {
  const target = event.target as HTMLImageElement;
  target.parentElement!.innerHTML = '<div class="h-full flex items-center justify-center text-gray-400">无预览</div>';
};

const formatFileSize = (bytes: number): string => {
  if (bytes < 1024) return bytes + ' B';
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(2) + ' KB';
  if (bytes < 1024 * 1024 * 1024) return (bytes / (1024 * 1024)).toFixed(2) + ' MB';
  return (bytes / (1024 * 1024 * 1024)).toFixed(2) + ' GB';
};
</script>