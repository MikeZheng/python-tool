/**
 * 扫描进度管理 Store
 * 用于管理文件扫描的进度状态
 */
import { defineStore } from 'pinia';
import { scanApi } from '../services/api';
import type { ScanProgress } from '../types';

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