# 无人机系统训练协作基础服务

本项目提供无人机训练机构、训练区域、装调设备与任务资料的统一后台基础能力，负责机构、场所、操作者和领域资料的登记，支持请求幂等、角色权限、SQLite 事务与哈希串联审计。各项资料通过稳定业务键保存，相同请求会返回原回执，不同内容复用编号时返回明确冲突。

在此之上，项目内置**训练排程子系统**，只处理训练计划和结构化遥测摘要，覆盖无人机系统赛项训练的日常排程与放行：

- **基础数据**：维护任务模板（装调 / 航线规划 / 故障处置）、人员资质、训练区域、设备状态与限制窗口；
- **可解释候选排程**：按训练窗口生成候选时段，每条候选附逐项检查说明（区域、设备、教员资质、限制窗口），不可用时段同样给出原因；
- **按版本放行**：值班教员（`reviewer` 角色）按计划版本放行，版本不一致即拒绝；放行在同一事务内复查资源并原子建立区域、设备、教员预占，杜绝两支队伍同时拿到同一资源；
- **事件归并**：放行后的任务开始、暂停、恢复、结束、异常事件允许乱序或重复到达，系统按确定顺序重放状态机归并为唯一过程，原始事件及其来源全部保留，被忽略的事件标注原因；
- **限制升级**：限制窗口升级为阻断（或设备 / 区域转为维护）时，只撤销尚未开始的放行并释放预占，进行中的任务转入人工处置单，由值班教员决定继续、中止或改期；
- **可查询的决策**：决策日志记录任务为何获批、改期或中止，计划与任务查询一并返回；
- **复盘与恢复**：任务完成或中止后生成待复盘记录（含结构化遥测摘要），全部状态持久化在 SQLite，进程重启后可直接继续完成待复盘记录。

## 目录

- `src/skills_workspace/`：领域模型、SQLite 存储、权限服务、审计链、训练排程服务、HTTP 路由和离线验收；
- `tests/`：核心规则、事务边界、排程规则、接口路由和端到端验收测试。

## 环境

- Linux
- Python 3.11 或更高版本
- 运行时仅使用 Python 标准库和 SQLite

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
PYTHONPATH=src python3 -m skills_workspace.scheduling_acceptance
```

第一条命令验收基础登记链路；第二条命令在临时 SQLite 数据库中执行完整排程故事：登记基础数据、生成候选、放行、资源互斥、乱序与重复事件归并、限制升级撤销、人工处置、进程重启后完成待复盘，并校验审计链。成功时输出一行 `status` 为 `ok` 的 JSON 并以退出码 `0` 结束。

## HTTP 服务

```bash
PYTHONPATH=src python3 -m skills_workspace.api --database skills_workspace.sqlite3 --host 127.0.0.1 --port 8080
```

健康检查使用 `GET /health`。业务写入接口通过 `X-Actor-Id` 标识操作者，支持机构、操作者、场所和领域资料登记，以及审计事件查询。服务重启后，SQLite 中的业务状态和审计链继续保留。

### 训练排程接口

所有写接口需要 `request_id` 保证幂等，重放返回 `200` 与原回执，新请求返回 `201`：

| 方法与路径 | 说明 | 角色 |
| --- | --- | --- |
| `POST /scheduling/templates` | 登记任务模板（技能类型、时长、资质与设备要求、区域类型） | admin/operator |
| `POST /scheduling/qualifications` | 授予人员资质（等级、有效期） | admin/operator |
| `POST /scheduling/areas`、`POST /scheduling/areas/status` | 登记训练区域、变更区域状态 | admin/operator |
| `POST /scheduling/equipment`、`POST /scheduling/equipment/status` | 登记设备、变更设备状态（触发冲突清扫） | admin/operator |
| `POST /scheduling/restrictions` | 登记限制窗口（site/airspace/area/equipment 范围，advisory/blocking 级别） | admin/operator/reviewer |
| `POST /scheduling/restrictions/escalate`、`/scheduling/restrictions/lift` | 升级限制为阻断（撤销未开始放行、进行中转人工处置）、解除限制 | admin/operator/reviewer |
| `POST /scheduling/plans` | 创建训练计划并生成带说明的候选时段 | admin/operator |
| `POST /scheduling/plans/regenerate` | 调整窗口重新生成候选（版本 +1，旧候选作废） | admin/operator |
| `POST /scheduling/releases` | 值班教员按计划版本放行，原子建立资源预占 | admin/reviewer |
| `POST /scheduling/task-events` | 上报任务事件（可附结构化遥测摘要），乱序/重复自动归并 | admin/operator |
| `POST /scheduling/dispositions/resolve` | 人工处置：continue / abort / reschedule | admin/reviewer |
| `POST /scheduling/reviews/complete` | 完成复盘记录 | admin/reviewer |
| `GET /scheduling/plan?plan_id=` | 计划详情：候选、决策日志、活动预占、处置单 | 任意 |
| `GET /scheduling/task?slot_id=` | 任务详情：归并时间线、原始事件、复盘记录、决策日志 | 任意 |
| `GET /scheduling/reviews?site_id=&status=` | 复盘记录列表（重启后待复盘仍在） | 任意 |
| `GET /scheduling/dispositions?site_id=&status=` | 人工处置单列表 | 任意 |
| `GET /scheduling/resources?site_id=` | 模板、区域、设备、限制、资质总览 | 任意 |
