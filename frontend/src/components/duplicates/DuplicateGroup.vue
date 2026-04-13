<template>
  <div class="bg-white rounded-lg shadow overflow-hidden" :data-sha256="group[0].sha256">
    <div class="px-6 py-4 bg-gray-50 border-b border-gray-200 flex items-center justify-between">
      <div class="flex items-center gap-2 relative">
        <span class="text-sm text-gray-500">SHA256:</span>
        <span 
          class="font-mono text-xs text-gray-600 cursor-pointer hover:text-indigo-600 relative group"
          @click="copySha256(group[0].sha256)"
          :title="group[0].sha256"
        >
          {{ group[0].sha256.substring(0, 32) }}...
        </span>
      </div>
      <div class="flex items-center gap-4">
        <span class="text-sm text-gray-500">{{ group.length }} 个文件</span>
        <span class="text-sm text-gray-500">{{ formatFileSize(group[0].file_size) }}</span>
        <span v-if="earliestFile.earliest_time" class="text-sm text-indigo-600">
          最早: {{ formatDate(earliestFile.earliest_time) }}
        </span>
      </div>
    </div>
    <div class="p-4 grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
      <FileCard 
        v-for="file in group" 
        :key="file.filepath"
        :file="file"
        :is-earliest="file.is_earliest"
      />
    </div>
    <div class="px-6 py-3 bg-gray-50 border-t border-gray-200 flex justify-end">
      <input 
        type="checkbox" 
        class="w-4 h-4 text-indigo-600 rounded"
        :checked="selected"
        @change="handleSelect"
      >
      <button 
        @click="emit('deduplicate', props.group[0].sha256)"
        class="ml-4 px-4 py-2 bg-orange-600 hover:bg-orange-700 text-white text-sm font-medium rounded-md"
      >
        去重
      </button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue';
import FileCard from './FileCard.vue';
import { useToast } from '../../composables/useToast';
import type { DuplicateGroup } from '../../types';

const { showToast } = useToast();

const props = defineProps<{
  group: DuplicateGroup;
  selected: boolean;
}>();

const earliestFile = computed(() => {
  return props.group.find(file => file.is_earliest) || props.group[0];
});

const formatFileSize = (bytes: number): string => {
  if (bytes < 1024) return bytes + ' B';
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(2) + ' KB';
  if (bytes < 1024 * 1024 * 1024) return (bytes / (1024 * 1024)).toFixed(2) + ' MB';
  return (bytes / (1024 * 1024 * 1024)).toFixed(2) + ' GB';
};

const formatDate = (dateString: string): string => {
  return new Date(dateString).toLocaleString('zh-CN');
};

const emit = defineEmits<{
  (e: 'select', sha256: string, selected: boolean): void;
  (e: 'deduplicate', sha256: string): void;
}>();

const handleSelect = (event: Event) => {
  const target = event.target as HTMLInputElement;
  emit('select', props.group[0].sha256, target.checked);
};

const copySha256 = async (sha256: string) => {
  try {
    await navigator.clipboard.writeText(sha256);
    showToast('SHA256 已复制到剪贴板');
  } catch (err) {
    console.error('无法复制 SHA256:', err);
    showToast('复制失败', 'error');
  }
};
</script>