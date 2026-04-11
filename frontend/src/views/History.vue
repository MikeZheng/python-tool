<template>
  <div class="p-6">
    <h1 class="text-3xl font-bold text-gray-900 mb-6">操作记录</h1>

    <div class="bg-white rounded-lg shadow overflow-hidden">
      <table class="min-w-full divide-y divide-gray-200">
        <thead class="bg-gray-50">
          <tr>
            <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">操作时间</th>
            <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">类型</th>
            <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">SHA256</th>
            <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">保留文件</th>
            <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">备份文件数</th>
            <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase">释放空间</th>
          </tr>
        </thead>
        <tbody class="divide-y divide-gray-200">
          <tr v-if="operations.length === 0">
            <td colspan="6" class="px-6 py-8 text-center text-gray-500">
              暂无操作记录
            </td>
          </tr>
          <tr v-for="operation in operations" :key="operation.id" class="hover:bg-gray-50">
            <td class="px-6 py-4 text-sm text-gray-900">
              {{ formatDate(operation.created_at) }}
            </td>
            <td class="px-6 py-4">
              <span class="px-2 py-1 bg-green-100 text-green-700 text-xs rounded-full">
                去重
              </span>
            </td>
            <td class="px-6 py-4 text-xs font-mono text-gray-600">
              {{ operation.sha256.substring(0, 16) }}...
            </td>
            <td class="px-6 py-4 text-sm text-gray-900 truncate max-w-xs" :title="operation.kept_file_path">
              {{ operation.kept_file_new_path || operation.kept_file_path || '-' }}
            </td>
            <td class="px-6 py-4 text-sm text-gray-900">
              {{ operation.backup_files ? operation.backup_files.length : 0 }}
            </td>
            <td class="px-6 py-4 text-sm text-green-600 font-medium">
              +{{ formatSpace(operation.space_saved) }}
            </td>
          </tr>
        </tbody>
      </table>
    </div>

    <!-- Pagination -->
    <div class="flex justify-center items-center gap-4 mt-6">
      <button 
        @click="loadPage(currentPage - 1)"
        :disabled="currentPage <= 1"
        class="px-4 py-2 bg-white border border-gray-300 rounded-md hover:bg-gray-50 disabled:opacity-50"
      >
        上一页
      </button>
      <span class="text-gray-700">
        第 {{ currentPage }} 页 / 共 {{ totalPages }} 页
      </span>
      <button 
        @click="loadPage(currentPage + 1)"
        :disabled="currentPage >= totalPages"
        class="px-4 py-2 bg-white border border-gray-300 rounded-md hover:bg-gray-50 disabled:opacity-50"
      >
        下一页
      </button>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted } from 'vue';
import { useHistoryStore } from '../stores/history';


const historyStore = useHistoryStore();

const operations = computed(() => historyStore.operations);
const currentPage = computed(() => historyStore.currentPage);
const totalPages = computed(() => historyStore.totalPages);

const loadPage = async (page: number) => {
  if (page < 1 || page > totalPages.value) return;
  await historyStore.fetchHistory(page);
};

const formatDate = (dateString: string): string => {
  return new Date(dateString).toLocaleString('zh-CN');
};

const formatSpace = (bytes: number): string => {
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(2) + ' KB';
  if (bytes < 1024 * 1024 * 1024) return (bytes / (1024 * 1024)).toFixed(2) + ' MB';
  return (bytes / (1024 * 1024 * 1024)).toFixed(2) + ' GB';
};

onMounted(() => {
  historyStore.fetchHistory(1);
});
</script>