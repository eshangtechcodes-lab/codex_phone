# 数据审核工具集 (Data Audit)

独立的数据同步与审计工具，从远程 DataAPI 拉取业务数据到本地 SQLite 镜像库，并提供多维度健康校验。

## 目录结构

```
data_audit/
├── config.py            # 统一配置（API 地址、DB 路径）
├── data/
│   └── dameng_mirror.db # 本地镜像数据库 (~310MB)
├── sync/                # 同步引擎
│   ├── sync_mirror.py   # 核心同步（A-F 六类表，增量/全量）
│   ├── sync_daily.py    # 日营收/日车流增量同步
│   ├── auto_sync.py     # 事件驱动自动同步
│   └── verify_data.py   # 同步后数据完整性比对
├── audit/               # 审计工具
│   └── data_health_check.py  # 5 层交叉校验
└── docs/                # 运维文档
    ├── 数据同步.md
    └── 自动运维.md
```

## 快速上手

### 1. 诊断数据库状态
```bash
python data_audit/sync/sync_mirror.py
```
不带参数运行，查看哪些月份缺失。

### 2. 增量同步指定月份
```bash
python data_audit/sync/sync_mirror.py --month 202603
```

### 3. 日粒度数据同步
```bash
python data_audit/sync/sync_daily.py              # 拉昨天
python data_audit/sync/sync_daily.py --backfill 7  # 回填7天
```

### 4. 数据健康检查
```bash
python data_audit/audit/data_health_check.py
python data_audit/audit/data_health_check.py --output report.md
```

### 5. 数据完整性比对
```bash
python data_audit/sync/verify_data.py              # 全部37张表
python data_audit/sync/verify_data.py --type A      # 只跑A类
```

## 依赖

- Python 3.8+
- `requests`
- `urllib3`（sync_daily 用到）

## 数据源

- 远程 DataAPI: `http://111.229.213.193:18071/api/dynamic`
- 云平台直连: `https://api.eshangtech.com`（日营收/日车流）
