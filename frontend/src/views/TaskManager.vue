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
            :disabled="taskStore.loading"
            class="px-6 py-2 bg-green-600 hover:bg-green-700 text-white font-medium rounded-md disabled:opacity-50"
          >
            添加任务
          </button>
        </div>
      </div>
    </div>

    <!-- Current Running Task -->
    <div v-if="taskStore.runningTask && taskStore.runningTask.status === 'running'" class="bg-blue-50 rounded-lg shadow mb-6 p-6">
      <div class="flex items-center justify-between mb-4">
        <h2 class="text-lg font-semibold text-blue-900">当前运行任务</h2>
        <div class="flex gap-2">
          <button
            @click="pauseTask"
            :disabled="taskStore.loading"
            class="px-4 py-2 bg-yellow-600 hover:bg-yellow-700 text-white font-medium rounded-md text-sm disabled:opacity-50"
          >
            暂停任务
          </button>
          <button
            @click="cancelTask(taskStore.runningTask.id)"
            :disabled="taskStore.loading"
            class="px-4 py-2 bg-orange-600 hover:bg-orange-700 text-white font-medium rounded-md text-sm disabled:opacity-50"
          >
            作废任务
          </button>
        </div>
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

    <!-- Paused Task -->
    <div v-if="taskStore.runningTask && taskStore.runningTask.status === 'paused'" class="bg-yellow-50 rounded-lg shadow mb-6 p-6">
      <div class="flex items-center justify-between mb-4">
        <h2 class="text-lg font-semibold text-yellow-900">已暂停任务</h2>
        <button 
          @click="resumeTask"
          class="px-4 py-2 bg-green-600 hover:bg-green-700 text-white font-medium rounded-md text-sm"
        >
          恢复任务
        </button>
      </div>
      <div class="mb-4">
        <div class="text-sm font-medium text-gray-900 mb-2">{{ taskStore.runningTask.directory_path }}</div>
        <div class="w-full bg-yellow-200 rounded-full h-4 mb-2">
          <div 
            class="bg-yellow-600 h-4 rounded-full transition-all duration-300"
            :style="{ width: (taskStore.runningTask.processed_files / taskStore.runningTask.total_files) * 100 + '%' }"
          ></div>
        </div>
        <div class="text-sm text-yellow-800 flex justify-between">
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
          :loading="taskStore.loading"
          @delete="deleteTask"
          @cancel="cancelTask"
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
          :loading="taskStore.loading"
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
          :loading="taskStore.loading"
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
import { ref, computed, onMounted, onUnmounted } from 'vue';
import { useTaskStore } from '../stores/tasks';
import { useScanStore } from '../stores/scan';
import { useToast } from '../composables/useToast';
import TaskItem from '../components/tasks/TaskItem.vue';

const taskStore = useTaskStore();
const scanStore = useScanStore();
const { showToast } = useToast();

const newDirectory = ref('');
let refreshInterval: number | null = null;

const hasActiveTasks = computed(() =>
  taskStore.tasks.some(t => ['running', 'paused', 'queued'].includes(t.status))
);

const startPolling = () => {
  if (refreshInterval) return;
  refreshInterval = window.setInterval(updateTasks, 3000);
};

const stopPolling = () => {
  if (refreshInterval) {
    clearInterval(refreshInterval);
    refreshInterval = null;
  }
};

const addTask = async () => {
  if (!newDirectory.value) {
    showToast('请输入目录路径', 'error');
    return;
  }

  const result = await taskStore.addTask(newDirectory.value);
  if (result.success) {
    showToast('任务已添加到队列');
    newDirectory.value = '';
    startPolling();
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
    startPolling();
  } else {
    showToast('重试失败: ' + (result.error || '未知错误'), 'error');
  }
};

const pauseTask = async () => {
  if (!taskStore.runningTask) return;
  
  const result = await taskStore.pauseTask(taskStore.runningTask.id);
  if (result.success) {
    showToast('任务已暂停');
  } else {
    showToast('暂停失败: ' + (result.error || '未知错误'), 'error');
  }
};

const resumeTask = async () => {
  if (!taskStore.runningTask) return;
  
  const result = await taskStore.resumeTask(taskStore.runningTask.id);
  if (result.success) {
    showToast('任务已恢复');
    startPolling();
  } else {
    showToast('恢复失败: ' + (result.error || '未知错误'), 'error');
  }
};

const cancelTask = async (taskId: number) => {
  if (!confirm('确定作废此任务吗？')) return;
  
  const result = await taskStore.cancelTask(taskId);
  if (result.success) {
    showToast('任务已作废');
  } else {
    showToast('作废失败: ' + (result.error || '未知错误'), 'error');
  }
};

const updateTasks = async () => {
  await taskStore.refreshTasks();
  await scanStore.fetchProgress();
  if (!hasActiveTasks.value) {
    stopPolling();
  }
};

onMounted(async () => {
  await updateTasks();
  if (hasActiveTasks.value) {
    startPolling();
  }
});

onUnmounted(() => {
  if (refreshInterval) {
    clearInterval(refreshInterval);
  }
});
</script>