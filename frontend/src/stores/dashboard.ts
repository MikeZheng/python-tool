import { defineStore } from 'pinia';
import { dashboardApi, historyApi } from '../services/api';
import type { DashboardStats, HistoryOperation } from '../types';

export const useDashboardStore = defineStore('dashboard', {
  state: () => ({
    stats: {
      duplicate_groups: 0,
      scanned_directories: 0,
      photo_count: 0,
      video_count: 0,
      total_files: 0,
      duplicate_files: 0,
      space_saved: 0
    } as DashboardStats,
    recentActivity: [] as HistoryOperation[],
    loading: false
  }),

  actions: {
    async fetchStats() {
      this.loading = true;
      try {
        const response = await dashboardApi.getStats();
        if (response.success && response.data) {
          this.stats = response.data;
        }
      } catch (error) {
        console.error('Error fetching dashboard stats:', error);
      } finally {
        this.loading = false;
      }
    },

    async fetchRecentActivity() {
      try {
        const response = await historyApi.getHistory(1, 5);
        if (response.success && response.data) {
          this.recentActivity = response.data.operations;
        }
      } catch (error) {
        console.error('Error fetching recent activity:', error);
      }
    }
  }
});