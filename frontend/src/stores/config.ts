import { defineStore } from 'pinia';
import { configApi } from '../services/api';
import type { Config, ApiResponse } from '../types';

export const useConfigStore = defineStore('config', {
  state: () => ({
    config: {
      storage_directory: '',
      backup_directory: '',
      max_concurrent_tasks: 2
    } as Config,
    loading: false
  }),

  actions: {
    async fetchConfig() {
      this.loading = true;
      try {
        const response = await configApi.getConfig();
        if (response.success && response.data) {
          this.config = response.data;
          return response.data;
        }
      } catch (error) {
        console.error('Error fetching config:', error);
      } finally {
        this.loading = false;
      }
      return null;
    },

    async updateConfig(storageDirectory: string, backupDirectory: string, maxConcurrentTasks?: number): Promise<ApiResponse<Config>> {
      this.loading = true;
      try {
        const response = await configApi.updateConfig(storageDirectory, backupDirectory, maxConcurrentTasks);
        if (response.success && response.data) {
          this.config = response.data;
        }
        return response;
      } catch (error) {
        console.error('Error updating config:', error);
        return {
          success: false,
          error: '更新配置失败'
        };
      } finally {
        this.loading = false;
      }
    }
  }
});