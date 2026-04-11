# 重复文件查找工具

一个用于扫描、识别和管理重复文件的工具，提供Web界面使用方式。

## 项目结构

```
├── backend/          # 后端代码 (Flask + SQLite)
│   ├── app.py        # Flask应用主文件
│   ├── models.py     # 数据模型
│   ├── services/     # 业务逻辑服务
│   ├── tasks/        # 后台任务
│   ├── routes/       # API路由
│   ├── sqlite_storage.py
│   └── storage_base.py
├── frontend/         # 前端代码 (Vue3 + Vite + Element Plus)
│   ├── src/
│   │   ├── views/    # 页面组件
│   │   ├── router/   # 路由配置
│   │   ├── stores/   # 状态管理
│   │   └── services/ # API接口封装
│   └── package.json
```

## 快速开始

### 1. 启动后端服务

```bash
# 进入后端目录
cd backend

# 安装依赖
pip install -r requirements.txt

# 启动Flask服务器
python app.py
```

后端服务将在 http://localhost:5000 启动。

### 2. 启动前端服务

```bash
# 进入前端目录
cd frontend

# 安装依赖
npm install

# 启动开发服务器
npm run dev
```

前端服务将在 http://localhost:3000 启动。

## 使用方式

### Web界面方式

1. 访问 http://localhost:3000
2. 使用导航栏切换不同功能页面：
   - **首页**：查看系统统计信息
   - **重复文件**：查看和管理重复文件
   - **扫描目录**：添加新目录进行扫描

## 主要功能

### Web界面功能

1. **仪表盘** - 系统概览和统计信息
   - 显示重复文件组数、扫描目录数、文件数量统计
   - 实时扫描进度显示
   - 快速操作入口

2. **配置管理** - 系统配置中心
   - 存储目录和备份目录配置
   - 扫描目录管理（添加、删除、重新扫描）
   - 数据管理（刷新、清空、导出）

3. **扫描目录** - 任务管理
   - 提交新的扫描任务
   - 查看扫描历史
   - 扫描状态监控

4. **重复文件** - 文件管理
   - 分页查看重复文件组
   - 选择性删除重复文件
   - 文件预览和信息查看
   - 批量操作支持

5. **操作记录** - 历史追踪
   - 查看所有操作历史
   - 按操作类型筛选
   - 删除和清空记录

## 技术栈

### 后端
- Python 3.x
- Flask
- SQLite
- 并行处理 (ProcessPoolExecutor)

### 前端
- Vue 3
- TypeScript
- Vite
- Element Plus
- Vue Router 4
- Pinia
- Axios

## 配置

存储类型通过 `config.json` 配置：
```json
{"storage_type": "sqlite", "last_updated": "..."}
```

## 开发

### 后端开发

后端API端点：
- `GET /duplicates?page=1` - 分页获取重复文件组
- `POST /scan-directory` - 扫描指定目录
- `POST /delete-file` - 删除文件并更新存储

### 前端开发

前端采用组件化开发，主要组件：
- `HomeView` - 首页
- `DuplicatesView` - 重复文件管理
- `ScanView` - 扫描目录

## 许可证

MIT License