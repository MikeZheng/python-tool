import { defineStore } from 'pinia';
import { taskApi } from '../services/api';
import type { Task } from '../types';

export const useTaskStore = defineStore('tasks', {
  state: () => ({
    tasks: [] as Task[],
    queuedTasks: [] as Task[],
    runningTask: null as Task | null,
    loading: false
  }),

  getters: {
    completedTasks: (state) => state.tasks.filter(task => task.status === 'completed'),
    failedTasks: (state) => state.tasks.filter(task => task.status === 'failed'),
    hasRunningTask: (state) => state.runningTask !== null
  },

  actions: {
    async fetchTasks() {
      this.loading = true;
      try {
        const response = await taskApi.getTasks();
        if (response.success && response.data) {
          this.tasks = response.data;
          // 同时检查running和paused状态
          this.runningTask = this.tasks.find(task => task.status === 'running' || task.status === 'paused') || null;
          // 从数据库中获取queued状态的任务
          this.queuedTasks = this.tasks.filter(task => task.status === 'queued');
        }
      } catch (error) {
        console.error('Error fetching tasks:', error);
      } finally {
        this.loading = false;
      }
    },

    async addTask(directory: string) {
      this.loading = true;
      try {
        const response = await taskApi.addTask(directory);
        if (response.success) {
          await this.fetchTasks();
        }
        return response;
      } catch (error) {
        console.error('Error adding task:', error);
        return { success: false, error: '添加任务失败' };
      } finally {
        this.loading = false;
      }
    },

    async deleteTask(taskId: number) {
      this.loading = true;
      try {
        const response = await taskApi.deleteTask(taskId);
        if (response.success) {
          await this.fetchTasks();
        }
        return response;
      } catch (error) {
        console.error('Error deleting task:', error);
        return { success: false, error: '删除任务失败' };
      } finally {
        this.loading = false;
      }
    },

    async retryTask(taskId: number) {
      this.loading = true;
      try {
        const response = await taskApi.retryTask(taskId);
        if (response.success) {
          await this.fetchTasks();
        }
        return response;
      } catch (error) {
        console.error('Error retrying task:', error);
        return { success: false, error: '重试任务失败' };
      } finally {
        this.loading = false;
      }
    },

    async pauseTask(taskId: number) {
      this.loading = true;
      try {
        const response = await taskApi.pauseTask(taskId);
        if (response.success) {
          await this.fetchTasks();
        }
        return response;
      } catch (error) {
        console.error('Error pausing task:', error);
        return { success: false, error: '暂停任务失败' };
      } finally {
        this.loading = false;
      }
    },

    async resumeTask(taskId: number) {
      this.loading = true;
      try {
        const response = await taskApi.resumeTask(taskId);
        if (response.success) {
          await this.fetchTasks();
        }
        return response;
      } catch (error) {
        console.error('Error resuming task:', error);
        return { success: false, error: '恢复任务失败' };
      } finally {
        this.loading = false;
      }
    },

    async cancelTask(taskId: number) {
      this.loading = true;
      try {
        const response = await taskApi.cancelTask(taskId);
        if (response.success) {
          await this.fetchTasks();
        }
        return response;
      } catch (error) {
        console.error('Error cancelling task:', error);
        return { success: false, error: '作废任务失败' };
      } finally {
        this.loading = false;
      }
    },

    async refreshTasks() {
      await this.fetchTasks();
    }
  }
});