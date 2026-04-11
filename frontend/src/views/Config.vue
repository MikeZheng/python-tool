<template>
  <div class="p-6">
    <h1 class="text-3xl font-bold text-gray-900 mb-6">配置</h1>

    <div class="bg-white rounded-lg shadow">
      <div class="px-6 py-4 border-b border-gray-200">
        <h2 class="text-lg font-semibold text-gray-900">存储配置</h2>
      </div>
      <div class="p-6 space-y-6">
        <!-- Storage Directory -->
        <div>
          <label class="block text-sm font-medium text-gray-700 mb-2">
            去重文件存储目录
          </label>
          <div class="flex gap-4">
            <input 
              type="text" 
              v-model="storageDirectory"
              class="flex-1 border border-gray-300 rounded-md shadow-sm focus:ring-indigo-500 focus:border-indigo-500 px-4 py-2"
              placeholder="例如: F:\整理后的文件"
            >
            <button 
              @click="selectStorageDirectory"
              class="px-4 py-2 bg-gray-200 hover:bg-gray-300 text-gray-700 rounded-md"
            >
              浏览
            </button>
          </div>
          <p class="mt-1 text-sm text-gray-500">去重后的文件将按年/月目录结构存储在此目录下</p>
        </div>

        <!-- Backup Directory -->
        <div>
          <label class="block text-sm font-medium text-gray-700 mb-2">
            备份目录
          </label>
          <div class="flex gap-4">
            <input 
              type="text" 
              v-model="backupDirectory"
              class="flex-1 border border-gray-300 rounded-md shadow-sm focus:ring-indigo-500 focus:border-indigo-500 px-4 py-2"
              placeholder="例如: F:\备份"
            >
            <button 
              @click="selectBackupDirectory"
              class="px-4 py-2 bg-gray-200 hover:bg-gray-300 text-gray-700 rounded-md"
            >
              浏览
            </button>
          </div>
          <p class="mt-1 text-sm text-gray-500">被替换的重复文件将移动到此目录作为备份</p>
        </div>

        <!-- Directory Structure Preview -->
        <div class="bg-gray-50 rounded-md p-4">
          <h3 class="text-sm font-medium text-gray-700 mb-2">目录结构预览</h3>
          <div class="font-mono text-sm text-gray-600">
            <div>存储目录/</div>
            <div class="ml-4">├── 2023/</div>
            <div class="ml-8">├── 01/</div>
            <div class="ml-8">├── 02/</div>
            <div class="ml-8">└── 03/</div>
            <div class="ml-4">└── 2024/</div>
            <div class="ml-8">├── 01/</div>
            <div class="ml-8">└── 02/</div>
          </div>
        </div>

        <!-- Save Button -->
        <div class="flex justify-end">
          <button 
            @click="saveConfig"
            class="px-6 py-2 bg-indigo-600 hover:bg-indigo-700 text-white font-medium rounded-md"
          >
            保存配置
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue';
import { useConfigStore } from '../stores/config';
import { useToast } from '../composables/useToast';

const configStore = useConfigStore();
const { showToast } = useToast();

const storageDirectory = ref('');
const backupDirectory = ref('');

const selectStorageDirectory = () => {
  const path = prompt('请输入存储目录路径:');
  if (path) storageDirectory.value = path;
};

const selectBackupDirectory = () => {
  const path = prompt('请输入备份目录路径:');
  if (path) backupDirectory.value = path;
};

const saveConfig = async () => {
  if (!storageDirectory.value) {
    showToast('请设置存储目录', 'error');
    return;
  }

  const result = await configStore.updateConfig(storageDirectory.value, backupDirectory.value);
  if (result.success) {
    showToast('配置保存成功');
  } else {
    showToast('保存失败: ' + (result.error || '未知错误'), 'error');
  }
};

onMounted(async () => {
  const config = await configStore.fetchConfig();
  if (config) {
    storageDirectory.value = config.storage_directory || '';
    backupDirectory.value = config.backup_directory || '';
  }
});
</script>