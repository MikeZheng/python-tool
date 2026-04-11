import { defineStore } from 'pinia';
import { duplicatesApi } from '../services/api';
import type { DuplicateGroup, ApiResponse, DeduplicateResult, BatchDeduplicateResult } from '../types';

export const useDuplicatesStore = defineStore('duplicates', {
  state: () => ({
    duplicateGroups: [] as DuplicateGroup[],
    currentPage: 1,
    totalPages: 1,
    loading: false
  }),

  actions: {
    async fetchDuplicates(page: number = 1) {
      this.loading = true;
      try {
        const response = await duplicatesApi.getDuplicates(page);
        if (response.success) {
          this.duplicateGroups = response.data;
          this.currentPage = response.pagination?.page || page;
          this.totalPages = response.pagination?.total_pages || 1;
        }
      } catch (error) {
        console.error('Error fetching duplicates:', error);
      } finally {
        this.loading = false;
      }
    },

    async deduplicateGroup(sha256: string): Promise<ApiResponse<DeduplicateResult>> {
      this.loading = true;
      try {
        const response = await duplicatesApi.deduplicateGroup(sha256);
        return response;
      } catch (error) {
        console.error('Error deduplicating group:', error);
        return {
          success: false,
          error: '去重失败'
        };
      } finally {
        this.loading = false;
      }
    },

    async batchDeduplicate(sha256List: string[]): Promise<ApiResponse<BatchDeduplicateResult>> {
      this.loading = true;
      try {
        const response = await duplicatesApi.batchDeduplicate(sha256List);
        return response;
      } catch (error) {
        console.error('Error batch deduplicating:', error);
        return {
          success: false,
          error: '批量去重失败'
        };
      } finally {
        this.loading = false;
      }
    }
  }
});