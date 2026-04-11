<template>
  <div class="p-6">
    <h1 class="text-3xl font-bold text-gray-900 mb-6">扫描目录</h1>

    <!-- Add New Directory -->
    <div class="bg-white rounded-lg shadow mb-6">
      <div class="px-6 py-4 border-b border-gray-200">
        <h2 class="text-lg font-semibold text-gray-900">添加新目录</h2>
      </div>
      <div class="p-6">
        <div class="flex gap-4">
          <input 
            type="text" 
            v-model="newDirectory"
            class="flex-1 border border-gray-300 rounded-md shadow-sm focus:ring-indigo-500 focus:border-indigo-500 px-4 py-2"
            placeholder="输入目录路径"
          >
          <button 
            @click="addDirectory"
            class="px-6 py-2 bg-green-600 hover:bg-green-700 text-white font-medium rounded-md"
          >
            扫描
          </button>
        </div>
      </div>
    </div>

    <!-- Scan Progress -->
    <div v-if="scanProgress.is_scanning" class="bg-blue-50 rounded-lg shadow mb-6 p-6">
      <div class="flex items-center justify-between mb-4">
        <h2 class="text-lg font-semibold text-blue-900">扫描进度</h2>
        <span class="text-sm font-bold text-blue-700">{{ scanProgress.percent }}%</span>
      </div>
      <div class="w-full bg-blue-200 rounded-full h-4 mb-4">
        <div 
          class="bg-blue-600 h-4 rounded-full transition-all duration-300"
          :style="{ width: scanProgress.percent + '%' }"
        ></div>
      </div>
      <div class="text-sm text-blue-800">
        <div>当前文件: <span class="font-mono">{{ scanProgress.current_file || '-' }}</span></div>
        <div>已处理: {{ scanProgress.processed_files }} / {{ scanProgress.total_files }}</div>
      </div>
    </div>

    <!-- Scanned Directories List -->
    <div class="bg-white rounded-lg shadow">
      <div class="px-6 py-4 border-b border-gray-200">
        <h2 class="text-lg font-semibold text-gray-900">已扫描目录</h2>
      </div>
      <div class="divide-y divide-gray-200">
        <div v-if="directories.length === 0" class="text-center text-gray-500 py-8">
          暂无扫描目录
        </div>
        <DirectoryItem 
          v-for="directory in directories" 
          :key="directory.id"
          :directory="directory"
          @rescan="rescanDirectory"
          @delete="deleteDirectory"
        />
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted } from 'vue';
import { useDirectoryStore } from '../stores/scan';
import { useScanStore } from '../stores/scan';
import { useToast } from '../composables/useToast';
import DirectoryItem from '../components/scan/DirectoryItem.vue';

const directoryStore = useDirectoryStore();
const scanStore = useScanStore();
const { showToast } = useToast();

const newDirectory = ref('');
const directories = computed(() => directoryStore.directories);
const scanProgress = computed(() => scanStore.progress);

let progressInterval: number | null = null;

const addDirectory = async () => {
  if (!newDirectory.value) {
    showToast('请输入目录路径', 'error');
    return;
  }

  const result = await directoryStore.addDirectory(newDirectory.value);
  if (result.success) {
    showToast('开始扫描');
    newDirectory.value = '';
    await directoryStore.fetchDirectories();
  } else {
    showToast('添加失败: ' + (result.error || '未知错误'), 'error');
  }
};

const rescanDirectory = async (directoryId: number) => {
  const result = await directoryStore.rescanDirectory(directoryId);
  if (result.success) {
    showToast('重新扫描已开始');
  } else {
    showToast('失败: ' + (result.error || '未知错误'), 'error');
  }
};

const deleteDirectory = async (directoryId: number) => {
  if (!confirm('确定删除此目录及其文件记录吗？')) return;

  const result = await directoryStore.deleteDirectory(directoryId);
  if (result.success) {
    showToast('目录已删除');
    await directoryStore.fetchDirectories();
  } else {
    showToast('删除失败: ' + (result.error || '未知错误'), 'error');
  }
};

const updateScanProgress = async () => {
  await scanStore.fetchProgress();
};

onMounted(async () => {
  await directoryStore.fetchDirectories();
  await scanStore.fetchProgress();
  progressInterval = window.setInterval(updateScanProgress, 1000);
});

onUnmounted(() => {
  if (progressInterval) {
    clearInterval(progressInterval);
  }
});
</script>