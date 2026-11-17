# 日切放行

> 遵守 `ops/底座生成前必读_效率与技术栈.md`。FastAPI + Vue 3 + SQLite，交付须 docker compose 一键起；库用 SQLite，不挂外置 Postgres。  
> **禁止同构换皮**：本仓主链路、表结构、接口与页面均独立，不得与其它仓共用同一套 `/api/items` 壳。

## 骨架一句话

业务日分区切片登记后裁决完整度，完整才放行下游，并与消费水位对账

## 基本信息

| 项 | 值 |
|----|-----|
| 产品名（首页） | 日切放行 |
| 目录 | `04-slicegate` |
| 领域建议 | 数仓与数据湖 |
| 接口 | http://127.0.0.1:10618 |
| 库 | `data/app.db` |
| 账号 | admin / 123456 |

## 为何能出多题

迟到切片、重切、跨分区依赖、部分完整冻结、消费水位回拨

## 避开

别做成作业拓扑编排换皮；别做成纯水位大盘

## 互斥切口候选

- 迟到切片整批拒收 vs 进待补齐冻结区
- 重切失败整笔回滚 vs 保留已完整分区
- 放行与水位同步闸 vs 异步放行后纠偏

## 多消费放行（本次主链路）

一个切片一次放行给多个消费方，先选定口径，选定即写入多消费提交簿，当日禁止改口。

| 口径 | 提交边界 | 失败时 |
|------|----------|--------|
| 甲 | 同一事务内为清单全部消费方写成功放行并各自推进水位 | 任一方失败整笔回滚，成功放行/水位一条不写 |
| 乙 | 逐消费方独立事务 | 成功方保留，失败方单列，整单标“部分成功”，禁止标成全部成功 |

- 口径按业务日锁定：`caliber_choices` 一行一个业务日，偏离即拒绝并记 `多消费口径偏了`。
- 同一切片同时落笔互斥：进程内锁 + `slice_locks` 表，后到直接判 `多消费争用`，不排队默成。
- 每个成功消费方各写一条自己的水位，禁止共享一条水位冒充多消费方。
- 消费方目录、放行侧栏、水位画面、策略旁注统一取自 `consumers` / `release_batches` / `releases` / `watermarks`，不各写一套。
- 文件库本体（`data/app.db` 既有 `slices/releases/watermarks` 表）不重建、不改列：多消费附加信息放新旁表 `release_details` / `watermark_details`，按 `release_id` / `watermark_id` 一一对应。

### 多消费接口

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/slices/{id}/multi-release` | 多消费放行，body: `{consumers:[...], caliber:"甲"\|"乙", note}` |
| GET | `/api/release-batches` | 多消费提交簿（可按 `slice_id` / `biz_date` 过滤） |
| GET | `/api/release-batches/{batch_no}` | 放行详情：逐消费方结果与水位 |
| GET | `/api/consumers` | 消费方目录（`POST` 登记、`PATCH /{code}` 启停） |
| GET | `/api/watermarks/summary` | 各消费方最新水位（与目录逐行对应） |
| GET | `/api/release-policy?biz_date=` | 策略旁注：当日口径、可选消费方与规则 |
| GET | `/api/release-incidents` | `多消费争用` / `多消费口径偏了` 留痕 |
| POST | `/api/slices/{id}/release` | 原单方放行，行为保持不变 |

## 启动（一键）

`powershell
cd d:\document\cccodex\workspace\slicegate\slicegate-08
docker compose up -d --build
`

浏览器：http://127.0.0.1:10618/  
健康检查：GET http://127.0.0.1:10618/api/health

本机开发（可选）：

`powershell
python -m pip install -r requirements.txt
python -m uvicorn app:app --app-dir backend --host 127.0.0.1 --port 10618
python -m pytest -q
`
