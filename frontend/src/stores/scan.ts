import { defineStore } from 'pinia';
import { directoryApi, scanApi } from '../services/api';
import type { Directory, ScanProgress, ApiResponse } from '../types';

export const useDirectoryStore = defineStore('directory', {
  state: () => ({
    directories: [] as Directory[],
    loading: false
  }),

  actions: {
    async fetchDirectories() {
      this.loading = true;
      try {
        const response = await directoryApi.getDirectories();
        if (response.success && response.data) {
          this.directories = response.data;
        }
      } catch (error) {
        console.error('Error fetching directories:', error);
      } finally {
        this.loading = false;
      }
    },

    async addDirectory(directory: string): Promise<ApiResponse<{ directory_id: number; message: string }>> {
      this.loading = true;
      try {
        const response = await directoryApi.addDirectory(directory);
        return response;
      } catch (error) {
        console.error('Error adding directory:', error);
        return {
          success: false,
          error: '添加目录失败'
        };
      } finally {
        this.loading = false;
      }
    },

    async deleteDirectory(directoryId: number): Promise<ApiResponse<{ message: string }>> {
      this.loading = true;
      try {
        const response = await directoryApi.deleteDirectory(directoryId);
        return response;
      } catch (error) {
        console.error('Error deleting directory:', error);
        return {
          success: false,
          error: '删除目录失败'
        };
      } finally {
        this.loading = false;
      }
    },

    async rescanDirectory(directoryId: number): Promise<ApiResponse<{ message: string }>> {
      this.loading = true;
      try {
        const response = await directoryApi.rescanDirectory(directoryId);
        return response;
      } catch (error) {
        console.error('Error rescanning directory:', error);
        return {
          success: false,
          error: '重新扫描失败'
        };
      } finally {
        this.loading = false;
      }
    }
  }
});

export const useScanStore = defineStore('scan', {
  state: () => ({
    progress: {
      is_scanning: false,
      current_file: '',
      processed_files: 0,
      total_files: 0,
      percent: 0,
      started_at: null,
      updated_at: null
    } as ScanProgress
  }),

  actions: {
    async fetchProgress() {
      try {
        const response = await scanApi.getProgress();
        if (response.success && response.data) {
          this.progress = response.data;
        }
      } catch (error) {
        console.error('Error fetching scan progress:', error);
      }
    }
  }
});