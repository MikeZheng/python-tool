import { defineStore } from 'pinia';
import { historyApi } from '../services/api';
import type { HistoryOperation } from '../types';

export const useHistoryStore = defineStore('history', {
  state: () => ({
    operations: [] as HistoryOperation[],
    currentPage: 1,
    totalPages: 1,
    loading: false
  }),

  actions: {
    async fetchHistory(page: number = 1) {
      this.loading = true;
      try {
        const response = await historyApi.getHistory(page);
        if (response.success && response.data) {
          this.operations = response.data.operations;
          this.currentPage = response.data.pagination.page;
          this.totalPages = response.data.pagination.total_pages;
        }
      } catch (error) {
        console.error('Error fetching history:', error);
      } finally {
        this.loading = false;
      }
    }
  }
});