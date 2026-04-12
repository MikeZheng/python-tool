import axios from 'axios';
import type { ApiResponse, DashboardStats, Config, Directory, ScanProgress, HistoryResponse, DeduplicateResult, BatchDeduplicateResult, Task } from '../types';

// API基础URL
const API_BASE = 'http://localhost:5000/api';

// 创建axios实例
const apiClient = axios.create({
  baseURL: API_BASE,
  headers: {
    'Content-Type': 'application/json'
  }
});

// 仪表盘API
export const dashboardApi = {
  getStats: async (): Promise<ApiResponse<DashboardStats>> => {
    const response = await apiClient.get('/dashboard/stats');
    return response.data;
  }
};

// 配置API
export const configApi = {
  getConfig: async (): Promise<ApiResponse<Config>> => {
    const response = await apiClient.get('/config');
    return response.data;
  },
  updateConfig: async (storageDirectory: string, backupDirectory: string): Promise<ApiResponse<Config>> => {
    const response = await apiClient.put('/config', {
      storage_directory: storageDirectory,
      backup_directory: backupDirectory
    });
    return response.data;
  }
};

// 目录API
export const directoryApi = {
  getDirectories: async (): Promise<ApiResponse<Directory[]>> => {
    const response = await apiClient.get('/directories');
    return response.data;
  },
  addDirectory: async (directory: string): Promise<ApiResponse<{ directory_id: number; message: string }>> => {
    const response = await apiClient.post('/directories', { directory });
    return response.data;
  },
  deleteDirectory: async (directoryId: number): Promise<ApiResponse<{ message: string }>> => {
    const response = await apiClient.delete(`/directories/${directoryId}`);
    return response.data;
  },
  rescanDirectory: async (directoryId: number): Promise<ApiResponse<{ message: string }>> => {
    const response = await apiClient.post(`/directories/${directoryId}/rescan`);
    return response.data;
  }
};

// 扫描进度API
export const scanApi = {
  getProgress: async (): Promise<ApiResponse<ScanProgress>> => {
    const response = await apiClient.get('/scan/progress');
    return response.data;
  }
};

// 重复文件API
export const duplicatesApi = {
  getDuplicates: async (page: number = 1, limit: number = 20): Promise<any> => {
    const response = await apiClient.get(`/duplicates?page=${page}&limit=${limit}`);
    return response.data;
  },
  deduplicateGroup: async (sha256: string): Promise<ApiResponse<DeduplicateResult>> => {
    const response = await apiClient.post(`/duplicates/${sha256}/deduplicate`);
    return response.data;
  },
  batchDeduplicate: async (sha256List: string[]): Promise<ApiResponse<BatchDeduplicateResult>> => {
    const response = await apiClient.post('/duplicates/batch-deduplicate', {
      sha256_list: sha256List
    });
    return response.data;
  }
};

// 历史API
export const historyApi = {
  getHistory: async (page: number = 1, limit: number = 20): Promise<ApiResponse<HistoryResponse>> => {
    const response = await apiClient.get(`/history?page=${page}&limit=${limit}`);
    return response.data;
  }
};

// 任务管理API
export const taskApi = {
  getTasks: async (): Promise<ApiResponse<Task[]>> => {
    const response = await apiClient.get('/tasks');
    return response.data;
  },
  addTask: async (directory: string): Promise<ApiResponse<{ task_id: number; message: string }>> => {
    const response = await apiClient.post('/tasks', { directory });
    return response.data;
  },
  getTask: async (taskId: number): Promise<ApiResponse<Task>> => {
    const response = await apiClient.get(`/tasks/${taskId}`);
    return response.data;
  },
  deleteTask: async (taskId: number): Promise<ApiResponse<{ message: string }>> => {
    const response = await apiClient.delete(`/tasks/${taskId}`);
    return response.data;
  },
  retryTask: async (taskId: number): Promise<ApiResponse<{ message: string }>> => {
    const response = await apiClient.post(`/tasks/${taskId}/retry`);
    return response.data;
  },
  pauseTask: async (taskId: number): Promise<ApiResponse<{ message: string }>> => {
    const response = await apiClient.post(`/tasks/${taskId}/pause`);
    return response.data;
  },
  resumeTask: async (taskId: number): Promise<ApiResponse<{ message: string }>> => {
    const response = await apiClient.post(`/tasks/${taskId}/resume`);
    return response.data;
  },
  getTaskQueue: async (): Promise<ApiResponse<Task[]>> => {
    const response = await apiClient.get('/tasks/queue');
    return response.data;
  }
};