# 分布式网页采集调度系统

面向多站点网页抽取、代理池调度、反爬对抗、增量采集、失败重试与采集结果校验的分布式采集调度系统。

这是一个前后端分离的管理平台：前端 React + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 React + Vite + TypeScript 前端
│   ├── src/views/            每个业务模块一个页面
│   ├── src/api/              统一请求封装
│   ├── src/stores/           会话与筛选状态
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/          每个业务模块一组接口
│   ├── app/services/         业务规则与状态流转
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm install
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 站点管理 | `site` | 采集站点 | 站点编号、域名地址、站点类型 |
| 抽取模板 | `template` | 网页模板 | 模板编号、入口地址、列表选择器 |
| 代理池管理 | `proxy` | 代理节点 | 节点编号、代理地址、代理协议 |
| 采集任务 | `spider` | 采集作业 | 作业编号、目标站点、抽取模板 |
| 调度策略 | `schedule` | 调度规则 | 规则编号、关联站点、调度表达式 |
| 反爬对抗 | `antibot` | 对抗策略 | 策略编号、验证码类型、识别服务 |
| 限速控制 | `ratelimit` | 限速配置 | 配置编号、目标站点、每秒请求上限 |
| 失败重试 | `retry` | 重试策略 | 策略编号、关联任务、最大重试次数 |
| 字段映射 | `extractor` | 映射规则 | 规则编号、源字段名称、目标字段名称 |
| 采集流水线 | `pipeline` | 流水线步骤 | 步骤编号、步骤名称、输入数据源 |
| 结果校验 | `validator` | 校验规则 | 规则编号、校验对象、校验类型 |
| 数据落盘 | `storage` | 存储配置 | 配置编号、存储类型、输出路径 |
| 采集监控 | `monitor` | 监控指标 | 指标编号、关联任务、指标名称 |
| 数据清洗 | `cleanse` | 清洗规则 | 规则编号、目标字段、空值替换 |
| 增量采集 | `delta` | 增量游标 | 游标编号、目标站点、增量字段 |
| 数据分发 | `export` | 交付信封 / 分发通道 | 信封编号、封存摘要、编码格式、目标系统、通道身份、回执阶段 |
| RPA流程 | `rpaflow` | 自动化流程 | 流程编号、目标页面、操作序列 |
| 登录凭证 | `auth` | 凭证配置 | 凭证编号、目标站点、认证方式 |

## 数据分发：交付信封对账

数据分发的主交互是「交付信封对账」页（`/export`），不是通道配置表：

1. 从**数据落盘的确认批次**里选一份内容，封存摘要、编码格式、目标系统和通道身份，
   生成一份不可变的交付信封快照。封存后落盘批次继续追加的内容不会混入该快照。
2. 向**本地模拟接收端**投递。发送端返回的 200 只说明传输层接收了载荷，交付结论以
   接收端回执为准。
3. 右侧按时间（或按证明阶段）展开三类回执：**送达证明 → 完整性验证 → 接收确认**，
   支持按通道筛选、按证明阶段排序，并直接定位尚缺哪种证明。
4. 接收端确认遗失后，操作员凭原信封要求对方补证或原样重送；摘要相同的重送沿用同一
   交付身份，传输次数不等于数据份数。
5. 拒收带明确阶段，是最终结论；之后迟到的低阶段证明不参与对账，也不能推翻拒收。
   通道之间的接收确认不能互相抵扣。
6. 内置演练覆盖：正常签收、接收端遗失、完整性拒收 + 迟到低阶段证明、已落盘但确认未送达、
   重复确认共用交付身份。完成对账后只保留唯一有效接收结果，而不是凭发送端 200 认定交付完成。

后端接口：`GET /api/export/envelopes`、`GET /api/export/envelopes/{id}`、
`POST /api/export/envelopes`（封存）、`.../send`（投递/原样重送）、
`.../supplement`（要求补证）、`.../receipts`（补录回执）、
`POST /api/export/batches/{id}/append`（演练：封存后追加落盘内容）。
规则测试：`cd backend && .venv/bin/python -m unittest discover -s tests`。

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.tsx`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数据采集、爬虫与RPA
