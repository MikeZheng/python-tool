# 后端服务

基于 Flask 的重复文件查找工具后端 API。

## 启动服务

```bash
# 安装依赖
pip install -r requirements.txt

# 启动开发服务器
python app.py

# 或使用 Gunicorn
gunicorn -w 4 -b 0.0.0.0:5000 app:app
```

## API 端点

- `GET /duplicates?page=1` - 分页获取重复文件组
- `POST /scan-directory` - 扫描指定目录
- `POST /delete-file` - 删除文件并更新存储

## 配置

存储类型通过 `config.json` 配置，默认使用 SQLite。