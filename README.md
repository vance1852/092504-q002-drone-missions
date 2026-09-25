# 无人机系统赛训协作服务

本项目面向无人机系统赛项训练的每日排程与执行协作，只处理**训练计划**与**结构化遥测摘要**，不接收原始飞控数据流。它在统一登记机构、场所、操作者与领域资料的基础能力之上，维护任务模板、人员资质、队伍、训练区域、设备状态与限制窗口，先产出**可解释的候选排程**，再由值班教员按**计划版本放行**；放行后归并可能乱序或重复的开始、暂停、结束与异常事件为唯一过程，并保留每条原始来源。

## 关键规则

- **候选只解释，不占资源**：`approved`（首选可行）、`rescheduled`（顺延并说明首选为何不可行）、`unschedulable`（逐条列出资源/资质/限制原因），每个计划项都带 `reasons`。
- **放行才原子预占**：值班教员（reviewer/admin）放行候选版本时，在单个 `BEGIN IMMEDIATE` 事务内二次校验区域/设备/教员状态、限制窗口与既有预占，然后一次性写入场地、区域、设备、教员、队伍的预占并创建任务实例；任何冲突整体回滚。并发放行由事务串行保证不会重复占用同一资源。
- **版本替代**：同一 `plan_key` 的新版本放行时，旧版本中尚未开始的任务被撤销并释放预占；已经开始的不受影响。
- **限制升级分流**：升级窗口只撤销**尚未开始**的放行（原子释放预占）；进行中（运行/暂停/异常）的任务进入**人工处置**，预占继续保留，直到值班教员人工结案才释放。
- **事件归并**：执行事件按 `event_id` 去重（同一事件经主/备链路重投仍识别为同一条），原始事件全部保留；唯一过程通过按事件发生时刻**重放**全部原始事件与外部干预锚点（限制升级、人工结案、版本撤销）重建，因此天然兼容乱序到达。重复或终态后的事件不产生迁移，但原样留存并给出 `reject_reason`。
- **查询可解释**：任务详情包含排程决策与理由、状态迁移链、原始事件及来源、资源预占与释放状态、撤销/人工处置记录、复盘结果，回答任务为何获批、改期、撤销或中止。
- **重启续办**：结构化遥测摘要先入待复盘队列，进程重启时自动（或调用 `resume_pending_work`）继续完成未处理记录。

## 目录

- `src/skills_workspace/`
  - `service.py`：组织、操作者、场所、领域资料的登记、权限与幂等（基础服务）。
  - `planning.py`：主数据、候选排程、放行、限制升级、事件归并、人工处置、遥测与复盘。
  - `storage.py`：SQLite 连接、建表、轻量列迁移与串行化事务。
  - `audit.py`：哈希串联审计链；`clock.py`：可替换 UTC 时钟。
  - `api.py`：仅依赖标准库的 HTTP/JSON 边界。
  - `acceptance.py` / `training_acceptance.py`：两套离线端到端验收。
- `tests/`：存储、服务、计划排程与执行、HTTP 路由、离线验收测试（共 31 个）。

## 环境

- Linux，Python 3.11 或更高版本，运行时仅使用 Python 标准库与 SQLite。

## 测试

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

## 构建检查

```bash
python3 -m compileall -q src tests
```

## 离线验收

```bash
PYTHONPATH=src python3 -m skills_workspace.acceptance
PYTHONPATH=src python3 -m skills_workspace.training_acceptance
```

两条命令分别核对基础登记幂等/审计链，以及训练计划的候选→放行→乱序事件归并→重启续办复盘；成功时输出一行 `status` 为 `ok` 的 JSON 并以退出码 `0` 结束。

## HTTP 服务

```bash
PYTHONPATH=src python3 -m skills_workspace.api --database skills_workspace.sqlite3 --host 127.0.0.1 --port 8080
```

健康检查 `GET /health`；所有写接口通过 `X-Actor-Id` 标识操作者，并用 `request_id` 保证幂等。

主数据与计划接口：

- `POST /teams`、`POST /persons`、`POST /team-members`
- `POST /task-templates`、`POST /training-areas`、`POST /equipment`、`POST /equipment-status`
- `POST /restrictions`、`POST /restrictions/escalate`
- `POST /plans/generate`、`POST /plans/release`、`GET /plans?plan_key=`、`GET /plans/{id}`
- `POST /task-events`（started/paused/resumed/ended/anomaly）、`POST /tasks/manual-resolve`
- `POST /telemetry-summaries`、`POST /reviews/run-pending`
- `GET /tasks?site_id=&state=`、`GET /tasks/{id}`

所有时间字段必须携带时区（如 `2026-09-26T08:00:00+08:00`）。服务重启后 SQLite 中的业务状态、审计链与待复盘队列继续保留，启动时自动续办待复盘记录。
