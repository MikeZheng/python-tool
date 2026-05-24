# 重复文件查找工具

一个用于扫描、识别和管理重复文件的工具，基于 SHA256 哈希检测，提供 Web 界面使用。

## 项目结构

```text
├── backend/              # 后端代码 (Flask + SQLite)
│   ├── app.py            # Flask 应用入口，蓝图注册，限流配置
│   ├── dependencies.py   # 依赖注入（懒加载单例）
│   ├── utils.py          # 工具函数（路径去重、日期解析）
│   ├── routes/           # API 路由层
│   │   ├── config_routes.py      # 配置读写
│   │   ├── task_routes.py        # 扫描任务管理 + 线程池
│   │   ├── duplicate_routes.py   # 重复文件查询与去重
│   │   ├── history_routes.py     # 操作历史
│   │   ├── dashboard_routes.py   # 仪表盘统计
│   │   └── progress_routes.py    # 扫描进度
│   ├── services/         # 业务逻辑层
│   │   ├── config_service.py     # 配置管理
│   │   ├── time_extraction.py    # 时间提取（EXIF/ffprobe/文件名）
│   │   ├── file_operations.py    # 去重文件移动操作
│   │   ├── progress_service.py   # 扫描进度管理
│   │   └── history_service.py    # 操作历史查询
│   ├── tasks/            # 后台任务
│   │   └── scanner.py           # 目录扫描引擎
│   ├── storage_base.py   # 存储抽象接口
│   └── sqlite_storage.py # SQLite 存储实现
├── frontend/             # 前端代码 (Vue 3 + Vite + Tailwind CSS)
│   ├── src/
│   │   ├── views/        # 5 个页面组件
│   │   ├── components/   # 8 个可复用组件
│   │   ├── router/       # 路由配置
│   │   ├── stores/       # 6 个 Pinia 状态管理
│   │   ├── services/     # API 接口封装
│   │   ├── composables/  # 组合式函数（useToast）
│   │   ├── types/        # TypeScript 类型定义
│   │   └── utils/        # 工具函数
│   ├── vite.config.ts
│   └── package.json
```

## 快速开始

### 1. 启动后端服务

```bash
cd backend
pip install -r requirements.txt
python app.py
```

后端服务运行在 <http://localhost:5000>。

### 2. 启动前端服务

```bash
cd frontend
npm install
npm run dev
```

前端开发服务器运行在 <http://localhost:5173>，API 请求通过 Vite 代理转发至后端。

## 使用方式

访问 <http://localhost:5173>，使用顶部导航栏切换功能页面：

- **仪表盘** — 系统概览：文件数量统计、重复文件组、节省空间
- **配置** — 设置存储目录、备份目录、最大并行扫描任务数
- **扫描任务** — 创建、暂停、恢复、作废扫描任务，实时进度监控
- **重复文件** — 浏览重复文件组，预览照片/视频，单个或批量去重
- **操作历史** — 查看所有去重操作记录

## API 端点

| 端点 | 方法 | 说明 |
| --- | --- | --- |
| `/api/config` | GET/PUT | 读写系统配置 |
| `/api/tasks` | GET/POST | 列举/创建扫描任务 |
| `/api/tasks/<id>` | GET/DELETE | 查看/删除任务 |
| `/api/tasks/<id>/retry` | POST | 重试失败任务 |
| `/api/tasks/<id>/pause` | POST | 暂停运行中任务 |
| `/api/tasks/<id>/resume` | POST | 恢复暂停任务 |
| `/api/tasks/<id>/cancel` | POST | 作废任务 |
| `/api/tasks/queue` | GET | 查看排队任务 |
| `/api/duplicates` | GET | 分页查询重复文件组 |
| `/api/duplicates/<sha256>/deduplicate` | POST | 对指定组去重 |
| `/api/duplicates/batch-deduplicate` | POST | 批量去重 |
| `/api/history` | GET | 分页查询操作历史 |
| `/api/dashboard/stats` | GET | 仪表盘聚合统计 |
| `/api/scan/progress` | GET | 当前扫描进度 |
| `/api/files/<path>` | GET | 通过文件路径返回文件（预览用） |

## 技术栈

### 后端

- Python 3.x
- Flask + flask-cors + flask-limiter
- SQLite（通过自定义 StorageInterface 抽象层访问）
- 多线程扫描（可配置并发数，实时生效）

### 前端

- Vue 3（Composition API + `<script setup>`）
- TypeScript
- Vite
- Tailwind CSS 4
- Vue Router 4
- Pinia
- Axios

## 配置

系统配置存储在 SQLite 数据库的 `config` 表中（单行记录），通过 Web API 读写，修改后实时生效：

- `storage_directory` — 去重后最早文件存储目录（按年/月组织）
- `backup_directory` — 被替换重复文件备份目录
- `max_concurrent_tasks` — 最大并行扫描任务数（1-10，默认 2）

## 许可

MIT License
