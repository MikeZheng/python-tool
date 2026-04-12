<template>
  <div class="p-6">
    <h1 class="text-3xl font-bold text-gray-900 mb-6">扫描任务管理</h1>

    <!-- Add New Task -->
    <div class="bg-white rounded-lg shadow mb-6">
      <div class="px-6 py-4 border-b border-gray-200">
        <h2 class="text-lg font-semibold text-gray-900">添加新任务</h2>
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
            @click="addTask"
            class="px-6 py-2 bg-green-600 hover:bg-green-700 text-white font-medium rounded-md"
          >
            添加任务
          </button>
        </div>
      </div>
    </div>

    <!-- Current Running Task -->
    <div v-if="taskStore.runningTask" class="bg-blue-50 rounded-lg shadow mb-6 p-6">
      <div class="flex items-center justify-between mb-4">
        <h2 class="text-lg font-semibold text-blue-900">当前运行任务</h2>
      </div>
      <div class="mb-4">
        <div class="text-sm font-medium text-gray-900 mb-2">{{ taskStore.runningTask.directory_path }}</div>
        <div class="w-full bg-blue-200 rounded-full h-4 mb-2">
          <div 
            class="bg-blue-600 h-4 rounded-full transition-all duration-300"
            :style="{ width: (taskStore.runningTask.processed_files / taskStore.runningTask.total_files) * 100 + '%' }"
          ></div>
        </div>
        <div class="text-sm text-blue-800 flex justify-between">
          <span>已处理: {{ taskStore.runningTask.processed_files }} / {{ taskStore.runningTask.total_files }}</span>
          <span>{{ Math.round((taskStore.runningTask.processed_files / taskStore.runningTask.total_files) * 100) }}%</span>
        </div>
      </div>
    </div>

    <!-- Task Queue -->
    <div v-if="taskStore.queuedTasks.length > 0" class="bg-yellow-50 rounded-lg shadow mb-6">
      <div class="px-6 py-4 border-b border-yellow-200">
        <h2 class="text-lg font-semibold text-yellow-900">待执行任务 ({{ taskStore.queuedTasks.length }})</h2>
      </div>
      <div class="divide-y divide-yellow-200">
        <TaskItem 
          v-for="task in taskStore.queuedTasks" 
          :key="task.id"
          :task="task"
          @delete="deleteTask"
        />
      </div>
    </div>

    <!-- Completed Tasks -->
    <div v-if="taskStore.completedTasks.length > 0" class="bg-white rounded-lg shadow mb-6">
      <div class="px-6 py-4 border-b border-gray-200">
        <h2 class="text-lg font-semibold text-gray-900">已完成任务</h2>
      </div>
      <div class="divide-y divide-gray-200">
        <TaskItem 
          v-for="task in taskStore.completedTasks" 
          :key="task.id"
          :task="task"
          @delete="deleteTask"
        />
      </div>
    </div>

    <!-- Failed Tasks -->
    <div v-if="taskStore.failedTasks.length > 0" class="bg-red-50 rounded-lg shadow mb-6">
      <div class="px-6 py-4 border-b border-red-200">
        <h2 class="text-lg font-semibold text-red-900">失败任务</h2>
      </div>
      <div class="divide-y divide-red-200">
        <TaskItem 
          v-for="task in taskStore.failedTasks" 
          :key="task.id"
          :task="task"
          @retry="retryTask"
          @delete="deleteTask"
        />
      </div>
    </div>

    <!-- No Tasks -->
    <div v-if="taskStore.tasks.length === 0" class="bg-white rounded-lg shadow p-8 text-center">
      <p class="text-gray-500">暂无扫描任务</p>
      <p class="text-gray-400 text-sm mt-2">点击上方的"添加任务"按钮创建新的扫描任务</p>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, onUnmounted } from 'vue';
import { useTaskStore } from '../stores/tasks';
import { useScanStore } from '../stores/scan';
import { useToast } from '../composables/useToast';
import TaskItem from '../components/tasks/TaskItem.vue';

const taskStore = useTaskStore();
const scanStore = useScanStore();
const { showToast } = useToast();

const newDirectory = ref('');

let refreshInterval: number | null = null;

const addTask = async () => {
  if (!newDirectory.value) {
    showToast('请输入目录路径', 'error');
    return;
  }

  const result = await taskStore.addTask(newDirectory.value);
  if (result.success) {
    showToast('任务已添加到队列');
    newDirectory.value = '';
  } else {
    showToast('添加失败: ' + (result.error || '未知错误'), 'error');
  }
};

const deleteTask = async (taskId: number) => {
  if (!confirm('确定删除此任务吗？')) return;

  const result = await taskStore.deleteTask(taskId);
  if (result.success) {
    showToast('任务已删除');
  } else {
    showToast('删除失败: ' + (result.error || '未知错误'), 'error');
  }
};

const retryTask = async (taskId: number) => {
  const result = await taskStore.retryTask(taskId);
  if (result.success) {
    showToast('任务已添加到队列');
  } else {
    showToast('重试失败: ' + (result.error || '未知错误'), 'error');
  }
};

const updateTasks = async () => {
  await taskStore.refreshTasks();
  await scanStore.fetchProgress();
};

onMounted(async () => {
  await updateTasks();
  refreshInterval = window.setInterval(updateTasks, 3000);
});

onUnmounted(() => {
  if (refreshInterval) {
    clearInterval(refreshInterval);
  }
});
</script>