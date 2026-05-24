<template>
  <div class="p-6">
    <div class="flex justify-between items-center mb-6">
      <h1 class="text-3xl font-bold text-gray-900">重复文件</h1>
      <button 
        @click="batchDeduplicate"
        :disabled="selectedDuplicates.size === 0"
        class="px-4 py-2 bg-orange-600 hover:bg-orange-700 text-white font-medium rounded-md disabled:opacity-50"
      >
        批量去重
      </button>
    </div>

    <!-- Filter -->
    <div class="bg-white rounded-lg shadow mb-6 p-4">
      <div class="flex gap-4 items-center">
        <span class="text-sm font-medium text-gray-700">筛选:</span>
        <select
          v-model="filterType"
          class="border border-gray-300 rounded-md px-3 py-2 text-sm"
        >
          <option value="all">全部</option>
          <option value="photo">仅照片</option>
          <option value="video">仅视频</option>
        </select>
        <div class="flex items-center gap-2">
          <input 
            type="checkbox" 
            id="select-all"
            v-model="selectAll"
            @change="toggleSelectAll"
            class="w-4 h-4 text-indigo-600 rounded"
          >
          <label for="select-all" class="text-sm text-gray-700">全选</label>
        </div>
      </div>
    </div>

    <!-- Duplicate Groups -->
    <div class="space-y-6">
      <div v-if="filteredGroups.length === 0" class="text-center text-gray-500 py-8">
        没有找到重复文件
      </div>
      <DuplicateGroup 
        v-for="group in filteredGroups" 
        :key="group[0].sha256"
        :group="group"
        :selected="selectedDuplicates.has(group[0].sha256)"
        @select="updateSelected"
        @deduplicate="deduplicateGroup"
      />
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
import { ref, computed, onMounted } from 'vue';
import { useDuplicatesStore } from '../stores/duplicates';
import { useToast } from '../composables/useToast';
import { formatFileSize } from '../utils/format';
import DuplicateGroup from '../components/duplicates/DuplicateGroup.vue';


const duplicatesStore = useDuplicatesStore();
const { showToast } = useToast();

const filterType = ref('all');
const selectAll = ref(false);
const selectedDuplicates = ref<Set<string>>(new Set());

const currentPage = computed(() => duplicatesStore.currentPage);
const totalPages = computed(() => duplicatesStore.totalPages);
const allGroups = computed(() => duplicatesStore.duplicateGroups);

const filteredGroups = computed(() => {
  if (filterType.value === 'all') return allGroups.value;
  return allGroups.value.filter(group => 
    group.some(file => file.file_type === filterType.value)
  );
});

const loadPage = async (page: number) => {
  if (page < 1 || page > totalPages.value) return;
  await duplicatesStore.fetchDuplicates(page);
  selectedDuplicates.value.clear();
  selectAll.value = false;
};

const toggleSelectAll = () => {
  if (selectAll.value) {
    filteredGroups.value.forEach(group => {
      selectedDuplicates.value.add(group[0].sha256);
    });
  } else {
    selectedDuplicates.value.clear();
  }
};

const updateSelected = (sha256: string, selected: boolean) => {
  if (selected) {
    selectedDuplicates.value.add(sha256);
  } else {
    selectedDuplicates.value.delete(sha256);
    selectAll.value = false;
  }
};

const deduplicateGroup = async (sha256: string) => {
  const result = await duplicatesStore.deduplicateGroup(sha256);
  if (result.success && result.data) {
    showToast(`去重成功，释放 ${formatFileSize(result.data.space_saved)}`);
    await duplicatesStore.fetchDuplicates(currentPage.value);
  } else {
    showToast('去重失败: ' + (result.error || '未知错误'), 'error');
  }
};

const batchDeduplicate = async () => {
  if (selectedDuplicates.value.size === 0) {
    showToast('请先选择要去重的文件组', 'error');
    return;
  }

  if (!confirm(`确定要对选中的 ${selectedDuplicates.value.size} 个文件组执行去重操作吗？`)) return;

  const result = await duplicatesStore.batchDeduplicate(Array.from(selectedDuplicates.value));
  if (result.success && result.data) {
    showToast(`批量去重完成，成功 ${result.data.success_count} 个，失败 ${result.data.error_count} 个，释放 ${formatFileSize(result.data.total_space_saved)}`);
    await duplicatesStore.fetchDuplicates(currentPage.value);
    selectedDuplicates.value.clear();
    selectAll.value = false;
  } else {
    showToast('批量去重失败: ' + (result.error || '未知错误'), 'error');
  }
};

onMounted(() => {
  duplicatesStore.fetchDuplicates(1);
});
</script>