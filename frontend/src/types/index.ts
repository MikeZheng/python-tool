// API响应类型
export interface ApiResponse<T> {
  success: boolean;
  data?: T;
  error?: string;
  message?: string;
}

// 仪表盘统计数据类型
export interface DashboardStats {
  duplicate_groups: number;
  scanned_directories: number;
  photo_count: number;
  video_count: number;
  total_files: number;
  duplicate_files: number;
  space_saved: number;
}

// 配置类型
export interface Config {
  storage_directory: string;
  backup_directory: string;
  max_concurrent_tasks: number;
}

// 目录类型
export interface Directory {
  id: number;
  directory_path: string;
  total_files: number;
  photo_count: number;
  video_count: number;
  duplicate_count: number;
  scanned_at: string;
}

// 扫描进度类型
export interface ScanProgress {
  is_scanning: boolean;
  current_file: string;
  processed_files: number;
  total_files: number;
  percent: number;
  started_at: string | null;
  updated_at: string | null;
}

// 文件类型
export interface File {
  filename: string;
  filepath: string;
  creation_time: string;
  file_size: number;
  sha256: string;
  earliest_time: string | null;
  file_type: 'photo' | 'video' | 'other';
  is_earliest: boolean;
  is_kept: boolean;
}

// 重复文件组类型
export type DuplicateGroup = File[];

// 分页类型
export interface Pagination {
  page: number;
  limit: number;
  total_groups: number;
  total_pages: number;
  has_more: boolean;
}

// 重复文件响应类型
export interface DuplicatesResponse {
  groups: DuplicateGroup[];
  pagination: Pagination;
}

// 历史操作类型
export interface HistoryOperation {
  id: number;
  sha256: string;
  kept_file_path: string;
  kept_file_new_path?: string;
  backup_files: string[];
  space_saved: number;
  created_at: string;
}

// 历史响应类型
export interface HistoryResponse {
  operations: HistoryOperation[];
  pagination: Pagination;
}

// 去重结果类型
export interface DeduplicateResult {
  success: boolean;
  space_saved: number;
  error?: string;
}

// 批量去重结果类型
export interface BatchDeduplicateResult {
  success: boolean;
  success_count: number;
  error_count: number;
  total_space_saved: number;
  error?: string;
}

// 扫描任务类型
export interface Task {
  id: number;
  directory_path: string;
  status: string;
  scan_started_at: string | null;
  scan_ended_at: string | null;
  total_files: number;
  processed_files: number;
  error_message: string | null;
  created_at: string;
  cancelled_at: string | null;
  photo_count?: number;
  video_count?: number;
  other_count?: number;
  duplicate_count?: number;
}