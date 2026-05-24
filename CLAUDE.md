# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A duplicate file finder tool with a Flask REST API backend and Vue 3 frontend. Scans directories, calculates SHA256 hashes, identifies duplicate files, and supports deduplication (move earliest file to organized storage, backup the rest). Multi-task concurrent scanning with a configurable thread pool.

## Commands

```bash
# Start backend (Flask on port 5000)
cd backend && python app.py

# Start frontend dev server (Vite on port 5173, proxies /api to :5000)
cd frontend && npm run dev
```

## Architecture

### Backend (`backend/`)

**Entry Point** (`app.py`):
- Flask app with CORS, rate limiting (200/min + 10/s), and 6 route blueprints under `/api`
- On startup, calls `_schedule_queued_tasks()` to resume queued scan tasks
- Inline route: `GET /api/files/<path>` serves files for frontend preview

**Route Layer** (`routes/`):

- `config_routes.py` — GET/PUT `/api/config` (storage dir, backup dir, max concurrent tasks)
- `task_routes.py` — Full task lifecycle CRUD + configurable thread pool (`_running_tasks` dict, `_pool_lock`)
  - `_claim_and_start()` atomically transitions queued→running then spawns a daemon thread
  - `_schedule_queued_tasks()` fills pool slots up to `max_concurrent_tasks` (reads config live)
  - `_run_task_wrapper()` runs scanner, cleans up in `finally`, re-schedules
- `duplicate_routes.py` — GET duplicates (paginated, marks `is_earliest`), POST single/batch deduplicate
- `history_routes.py` — GET paginated operation history
- `dashboard_routes.py` — GET aggregate stats
- `progress_routes.py` — GET current scan progress with percent

**Service Layer** (`services/`):

- `config_service.py` — Config read/write, directory validation, storage path generation (`YYYY/MM/`)
- `time_extraction.py` — Extracts earliest timestamp from 4 sources (FS times, EXIF via Pillow, video metadata via ffprobe with 10s timeout, 9 filename regex patterns)
- `file_operations.py` — Dedup logic: `_find_earliest_file()`, `_backup_file()` (shutil.move to backup), `_move_to_storage()` (move to `storage/YYYY/MM/` with timestamped filename)
- `progress_service.py` — Manages `scan_progress` singleton row
- `history_service.py` — Paginated operation history queries

**Scanner** (`tasks/scanner.py`):

- `scan_directory_task(directory_path, task_id)` — Main scan pipeline:
  1. Collect files via `os.walk`
  2. Load existing file cache (size/mtime) for modification detection
  3. Seed SHA256 counts from DB for cross-scan duplicate detection
  4. Per-file loop: pause/cancel check → mod check → SHA256 (4KB chunks) → file type → earliest time → DB write → duplicate tracking
  5. Dual pause/cancel check: once at loop start, once after expensive ops (SHA256+EXIF) to narrow race window
  6. Pause serializes state to `scan_tasks.pause_data` JSON; resume deserializes and continues from `files[processed:]`
- Module-level utilities: `calculate_sha256()`, `iter_files()`, `is_file_modified()`, `_save_pause_state()`, `_load_pause_state()`

**Storage Layer**:

- `storage_base.py` — `StorageInterface` ABC (24 abstract methods)
- `sqlite_storage.py` — Only concrete implementation. 6 tables: `files`, `config`, `operation_history`, `scan_progress`, `scan_tasks`, `scan_file_mappings`. Auto-migration via `_migrate_database()`.

**Dependency Injection** (`dependencies.py`):

- 6 lazy-initialized singletons: `get_storage()`, `get_config_service()`, `get_time_service()`, `get_file_ops_service()`, `get_progress_service()`, `get_history_service()`

**Task State Machine:**

```text
queued → running → completed
                 → failed
                 → paused → queued (resume) → running
                 → cancelled
```

### Frontend (`frontend/`)

**Tech**: Vue 3 (Composition API, `<script setup>`), TypeScript, Vite, Tailwind CSS 4, Pinia, Vue Router

**Pages** (5 routes):

| Route | View | Purpose |
| --- | --- | --- |
| `/` | `Dashboard.vue` | 7 stat cards + recent activity, parallel data fetch on mount |
| `/config` | `Config.vue` | Storage dir, backup dir, max concurrent tasks (1-10) |
| `/scan` | `TaskManager.vue` | Task CRUD, progress bars, 3s polling (auto-stops when idle) |
| `/duplicates` | `Duplicates.vue` | Filterable duplicate groups, single/batch dedup, file preview |
| `/history` | `History.vue` | Paginated operation history table |

**Stores** (6 Pinia stores, Options Store syntax):

- `useTaskStore` — Tasks partitioned into `runningTasks[]`, `pausedTasks[]`, `queuedTasks[]`; all mutating actions re-fetch
- `useDuplicatesStore` — Duplicate groups with pagination, single/batch dedup
- `useDashboardStore` — Stats + recent activity (parallel fetch)
- `useConfigStore` — Config read/write
- `useHistoryStore` — Paginated history
- `useScanStore` — Scan progress (polled alongside tasks)

**Components** (8 total, 2 unused):

- `Toast.vue` — Global notifications (mounted in App.vue)
- `StatCard.vue`, `RecentActivity.vue` — Dashboard widgets
- `DuplicateGroup.vue`, `FileCard.vue` — Duplicate group display with image/video preview
- `TaskItem.vue` — Single task row with status-aware action buttons
- `ProgressBar.vue`, `DirectoryItem.vue` — Defined but not currently used

**Composables**: `useToast()` — Singleton toast system (module-level refs, 3s auto-dismiss)

**Key patterns**:

- API base URL uses `import.meta.env.VITE_API_BASE_URL || '/api'` with Vite proxy in dev
- Dynamic polling: 3s interval when active tasks exist, stops when idle
- `formatFileSize()` in `utils/format.ts` shared across all views
- `ApiResponse<T>` generic wrapper on all API responses (`{ success, data?, error? }`)

## Configuration

Configuration is stored in the SQLite `config` table (singleton row, id=1), managed through the `/api/config` API. The `max_concurrent_tasks` value is read live by the thread pool scheduler — changes take effect immediately without restart.

## Output Files

- `backend/file_database.db` — SQLite database (all persistent state)
- `backend/file_processing.log` — Rotating log (10MB × 3 backups)
