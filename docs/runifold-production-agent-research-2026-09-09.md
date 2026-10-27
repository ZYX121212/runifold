# Runifold 生产级 Agent 演进研究

## 1. 决策摘要

Runifold 下一阶段应将重点放在**可恢复的交互式执行**：让 Rust 应用开发者能用一条清楚的路径，组合会话、流式输出、工具执行、人工介入和故障恢复，并验证业务结果是否正确。

最有价值的产品承诺是：**请求被可靠接收，执行状态可解释，已完成工作可复用，不确定的外部结果不会被伪装成成功；用户断线、服务重启和版本升级时，系统仍能给出明确处理方式。** 这是一项需要逐条验证的目标，并非当前版本已经达到的保证。

当前基础足够支持这条路线。Runifold 已有模型路由、类型化工具、预算与权限、会话和语义记忆、自动摘要、Effect 协调、检查点、工作流、分布式租约、人工中断和评估工具。新增价值主要来自这些能力之间的连接与完整交付，而不是重新实现它们。[本地架构与实现见附录 A。]

建议按依赖关系推进：

1. **先确定语义**：Session、一次用户请求、执行尝试、事件订阅和外部操作分别如何标识；何时提交；如何取消；如何处理版本变化。
2. **打通最小生产路径**：一个 SQLite 支撑的会话应用，同时支持流式、自动摘要、重启恢复和请求去重。
3. **补齐真实操作闭环**：执行前准入、可恢复的工具审批、外部操作结果核对、上下文容量管理。
4. **建设业务可靠性评估**：在现有 testkit/eval 上增加故障条件下的终态验证与重复成功率，作为功能准入条件。
5. **再提升吞吐和协作**：受控工具并发、按需工具发现、小规模任务委派；用相同预算下的任务表现判断是否需要多 Agent。

这里对“Session 优先”的判断有一个重要限定：只增加流畅的 Builder 不足以解决生产需求。它必须连接既有执行状态机，并处理并发准入、提交边界、可恢复输出和版本兼容；否则复杂度只是从函数参数转移到了更隐蔽的地方。

## 2. 范围、证据与适用边界

本报告面向以 Rust 构建生产 Agent 应用的团队。分析截止到 2026-09-09；本地代码基线为 0.9.1、提交 `119f0be40cfb1434159db5672e0defe932221624`。外部框架文档是访问时的在线内容，不自动等同于某个已发布软件包；涉及具体功能状态时，尽量采用版本化论文、规范或已合并 PR。

证据分为四类，不能互相替代：

| 类型 | 可以支持的判断 | 不能据此断言 |
| --- | --- | --- |
| 从业者调查、真实部署案例 | 需求方向、实践中的权衡 | Rust 用户群的精确占比或付费意愿 |
| 官方文档、维护者 issue 与 PR | 已提供的接口、真实集成问题及解决方向 | 所有部署都可靠，或问题在所有版本都存在 |
| 学术实验和基准 | 受控条件下的失败模式、评估方法 | 最新模型或所有业务的统一成功率 |
| 本地源码和定向测试 | 当前实现路径、已覆盖的行为 | 完整生产认证、第三方复现结论 |

报告中的“事实”来自可追溯来源；“推演”是根据状态顺序和故障条件作出的分析；“建议”是产品与架构选择。对代码缺口的判断是当前接口与实现范围内的判断，不能直接等同于已经发生的线上事故。

公开资料中缺少具有代表性的“Rust 生产 Agent 开发者需求调查”。因此，市场定位是有证据支持的待验证假设，后续仍需设计伙伴和实际应用反馈。本文不使用 GitHub 星数、Provider 数量或未经核实的市场预测代替需求证据。

## 3. 业界和学术界的真实信号

### 3.1 生产团队最需要可控的正确性

《Measuring Agents in Production》v4 基于 20 个案例访谈和 306 份有效问卷，主体分析筛选其中 86 个生产或试点系统。研究在 2025 年收集数据：68% 的相关受访系统在人工介入前执行至多 10 步，74% 主要依靠人工评估，可靠性是首要挑战。不同问题的回答人数不同，不能将百分比一律乘以 86。样本偏向专业网络，案例团队主要位于美洲，结论适合作为方向性证据。[1](https://arxiv.org/html/2512.04123v4)[^1]

**判断：** 默认可控、遇到问题能交接、结果可以核对，比默认无限自治更符合当前目标用户。Runifold 的预算、权限和恢复设计具有契合度，但必须转化为应用可以直接使用的完整流程。

### 3.2 多 Agent 的收益取决于任务结构

《Towards a Science of Scaling Agent Systems》v3 扩展到 260 个配置和六个基准；观察到多 Agent 相对单 Agent 的表现从可分解任务上的提升到顺序规划上的下降。其跨验证解释力有限，研究不支持把某个性能阈值当成通用架构规则。这里使用 v3，不能混用旧版的 180 配置与相关系数。[2](https://arxiv.org/html/2512.08296v3)[^2]

Anthropic 的研究系统案例则说明，多方向独立搜索确实能从并行子 Agent 获益，但需要清楚的委派边界并付出更高 token 成本。该案例属于厂商内部评估，不能转化为 Runifold 的预期增益。[3](https://www.anthropic.com/engineering/multi-agent-research-system)[^3]

**判断：** 同一 Agent 的独立工具并发、小规模专家委派值得支持；通用群体协作不应成为下一阶段核心目标。必须保留单 Agent 基线，并同时比较质量、成本与时延。

### 3.3 协作失败往往发生在接口与验收处

MAST 研究 v3 发布了来自七个框架的 1600 多条标注轨迹，归纳 14 种失败模式，覆盖系统设计、Agent 间不一致和任务验证。失败分类来自具体模型和任务集合，不代表今日所有系统的发生率。[4](https://arxiv.org/html/2503.13657v3)[^4]

**判断：** 委派接口应表达任务范围、输出契约、证据、资源和终止条件。当前 Runifold 的 reviewer 与 completion requirement 是可复用基础；先改善“何谓完成”，比增加更多角色名称更有价值。

### 3.4 持久化、流式和取消正在成为独立契约

LangChain 的 join/rejoin 文档明确支持客户端离开后继续执行及重新订阅，但该功能依赖 Agent Server；不能把它全部归因于嵌入式 LangGraph 库。[5](https://docs.langchain.com/oss/python/langchain/frontend/join-rejoin)[^5]

Pydantic AI 的 #6460 描述了取消请求被步骤吸收后，运行仍正常完成的问题，并讨论 run 级取消、响应取消和退出订阅的区别。访问时该 issue 仍为 Open，应视为明确的需求和设计讨论，而不是已发布保证。[6](https://github.com/pydantic/pydantic-ai/issues/6460)[^6]

**判断：** “断开订阅”“取消当前生成”“取消整个任务”必须有不同语义。Runifold 已有层级取消，但应用入口仍需要将这些动作清楚地暴露出来。

### 3.5 存储、Agent 循环和执行环境需要独立演进

Anthropic 2026 年的 Managed Agents 案例将 session、harness、sandbox 分开，针对故障隔离与运行环境变化；还指出旧模型需要的上下文补丁可能在新模型上失去价值。[7](https://www.anthropic.com/engineering/managed-agents)[^7]

**判断：** Runifold 的稳定核心应表达身份、状态、权限、提交和恢复；摘要策略、计划方式和工具选择应可替换。不能为了 Session 把 Provider、数据库、HTTP 服务和沙箱收进同一个对象。

### 3.6 长期记忆需求包括更新、时间和“不知道”

LongMemEval 定义了信息提取、跨会话推理、时间推理、知识更新和拒绝无依据回答五种能力。它衡量的是记忆使用质量，不是数据库持久化正确性。[8](https://arxiv.org/html/2410.10813v2)[^8]

Anthropic 的上下文工程实践将压缩、结构化笔记和按需检索结合；也强调过度摘要会丢失后来才显得关键的信息。[9](https://www.anthropic.com/engineering/effective-context-engineering-for-ai-agents)[^9]

**判断：** “保存了历史”不能等同于“能正确使用历史”。Runifold 需要可评估的上下文装配策略；事实更正、任务约束和授权状态不能只靠自由文本摘要维护。

### 3.7 业务结果评估需要超越最终回答文本

τ-bench 用对话结束时的数据库状态验证任务，并用 pass^k 衡量重复运行的一致成功。其旧模型分数不用于判断现在的模型能力，但终态验证方法仍有价值。[10](https://arxiv.org/abs/2406.12045)[^10]

ReliabilityBench 提议把重复运行、语义扰动和工具故障组合起来评估。它是规模有限的预印本，本报告借鉴测试维度，不采用其结果作为生产认证门槛。[11](https://arxiv.org/abs/2601.06112)[^11]

**判断：** Runifold 现有基础设施故障测试与质量评估应建立连接：同一个业务任务在断线、超时、冲突和恢复后，最终状态仍应符合要求。

### 3.8 能调用工具不等于可以信任工具内容

AgentDojo 研究工具返回的不可信内容如何影响后续行为；这表明提示注入需要在具体操作路径中评估。[12](https://arxiv.org/abs/2406.13352v3)[^12] MCP 2025-11-25 规范也要求，在服务器不可信时不能信任工具 annotations。[13](https://modelcontextprotocol.io/specification/2025-11-25/server/tools)[^13]

**判断：** 工具可见性、权限、审批、外发数据范围和执行环境隔离需要分别处理。Runifold 的 RemoteToolPolicy 已由宿主显式指定 effect/risk，应保留这一设计；不要为了自动发现方便，把远端声明直接升级为授权依据。

## 4. 竞争参照与 Runifold 的位置

| 参照对象 | 已核实的相关能力或实践 | 对 Runifold 的启示 |
| --- | --- | --- |
| Rig | #1957 于 2026-06-25 合并，将显式并发设置用于流式工具执行，保留默认串行 | Rust 用户需要流式与普通调用语义一致；不能再把该历史 issue 描述为当前未修复缺陷 |
| LangGraph / Agent Server | 检查点、人工中断；Server 层提供 join/rejoin | 学习执行与订阅分离，并保留嵌入式部署的轻量性 |
| Pydantic AI | durable backend、流式与 MCP 组合；deferred tools 对接人工或外部执行 | 值得补齐工具暂停/完成的通用边界，不必复制 Python API |
| OpenAI Agents | 官方文档区分本地历史、Session、服务端会话及 response continuation | 文档必须区分“开始下一轮”和“恢复同一轮”，避免混用两套历史 |
| Anthropic Managed Agents | session、执行循环和 sandbox 分离 | 保持稳定语义接口，让策略和执行环境在边缘演进 |

上述比较只覆盖本报告核实的维度，不是完整功能排名，也没有对不同框架进行同任务实测。[14](https://github.com/0xPlaygrounds/rig/pull/1957)[15](https://pydantic.dev/docs/ai/capabilities/durable_execution/overview/)[16](https://pydantic.dev/docs/ai/tools-toolsets/deferred-tools/)[17](https://developers.openai.com/api/docs/guides/agents/running-agents#choose-one-conversation-strategy)[^14][^15][^16][^17]

**建议定位：** 为已有 Rust 服务增加具有明确状态、权限、预算和恢复行为的 Agent 执行能力。长期优势应是“能解释和验证复杂操作”，而非宣称比其他语言天然更可靠。内存安全不能保证业务幂等、模型判断或外部系统事务正确。

首先验证两个目标场景：交互式业务助手、可人工介入的后台操作。研究助手作为并发和上下文压力测试。完整编码 Agent、语音实时系统、通用跨组织 Agent 网络暂时不作为默认产品设计中心。

## 5. 当前代码的能力与缺口

### 5.1 应保留和复用的基础

Runifold 已具备会话持久化与摘要、语义记忆、富工具输出与 Artifact、外部 Effect 写前记录及结果核对、显式委派权限、工作流租约与等待、reviewer 和离线评估。工作流与 Agent 的恢复边界并不完全相同，不能因为工作流已有某能力就假设所有 Agent 入口自动具备它。

最重要的既有设计是：普通调用和流式调用驱动同一 Agent 状态机；会话、Journal、语义记忆分别建模；外部操作结果不确定时默认拒绝盲目重试。这些应成为新增高层 API 的约束，而非被便捷封装绕开。

### 5.2 六项直接影响生产应用的缺口

| 缺口 | 当前证据 | 对应用的影响 | 性质 |
| --- | --- | --- | --- |
| 流式、会话、摘要、恢复入口未统一 | execution.rs 分别提供 stream / run_conversation / with_summary / durable / resume；RFC 0018 明确延后 checkpointed streaming 和游标 | 开发者要自行完成最常见能力组合 | 已确认的接口缺口 |
| 流的消费驱动执行 | AgentEventStream 持有并 poll 执行 future；drop 会丢弃它 | 直接接入 HTTP 时，不能自然获得断线后继续执行 | 已确认语义，不是实现 bug |
| 会话 CAS 在执行之后 | 先读取 version、执行 Agent，再 append 或 commit | 同一会话两个不同请求可能在提交冲突前都已操作外部系统 | 基于顺序的风险推演，需并发用例验证 |
| Agent 普通工具计划未纳入完整恢复清单 | checkpoint 验证 Agent、Model、reviewer 等；未统一核对普通工具 schema/version 和生成策略 | 部署升级后恢复旧任务的行为边界不清楚 | 已确认覆盖范围；事故尚未复现 |
| Agent 单轮 callable 串行 | execute_calls 内逐个 await 工具或子 Agent | 独立慢工具的等待时间累加 | 已确认执行行为 |
| 上下文按条目配置，缺少统一装配计划 | ConversationContextPolicy 的 window / summary_batch 按条目计；工具规格整体加入请求 | 少量巨大输出或大量工具可消耗上下文 | 已确认配置范围；真实负载待测 |

具体定位见附录 A。这些缺口不支持“Runifold 缺少持久化”“没有记忆”“没有评估”的说法；它们支持的结论是：已有基础尚未形成默认完整的应用路径。

### 5.3 应用层还需补齐的两项连接

**工具人工审批。** 工作流 interrupt 已有实现，但在已检查的 Tool/Agent 入口中，没有看到统一的可持久化 pending-tool 结果与审批恢复入口。可以由应用自行包装，不过会增加每个应用重复处理关联 ID、参数版本与超时的成本。建议复用已有治理和等待机制，补连接而非重建审批系统。

**评估到排障。** testkit 已有 scorer、重复样本、波动分析、基线比较、输出脱敏、缓存与分片。下一步应增加“某次真实业务失败→脱敏案例→固定环境状态→回归实验”的路径。Journal 不默认存提示正文是优点，不能为了重放便利默默扩大数据采集。

## 6. 需求推演：目标用户真正要完成什么

以下不是市场占比预测，而是从证据和当前实现推导出的设计场景。

| 使用者 | 典型工作 | 最低可接受行为 | 首要能力 |
| --- | --- | --- | --- |
| Rust 后端开发者 | 将助手加入现有 API 服务 | 重复请求不重复扣费或写入；会话不串租户；能流式返回 | Session、请求准入、身份范围、事件适配 |
| 业务自动化团队 | 查资料、生成变更、审批后执行 | 审批对应确切参数；超时后能查明是否执行；可继续 | Pending action、Effect reconciliation、恢复 |
| 平台/SRE 团队 | 扩缩容、重启、升级、限流 | 活跃任务可接管；取消可解释；旧任务不被新定义误执行 | 租约、版本清单、状态查询、预算 |
| 研究/分析助手团队 | 多源搜索、汇总、报告 | 子任务可并行；来源可追溯；成本受控 | 受控并发、Artifact、证据与上下文 |
| 应用质量负责人 | 比较模型或 Prompt 版本 | 固定任务环境、重复运行、验证业务终态 | 实验清单、状态 scorer、失败分类 |

共同需求是：**把不确定的模型决策接入可以验证的软件过程。** 运行时应保证它能保证的状态和权限；业务适配器验证订单、文件、工单等真实结果；模型负责需要推理的部分。三者的责任要在 API 和文档中分开。

## 7. 架构推演：Session 应该包含什么

### 7.1 五种身份不能混成一个 ID

| 概念 | 含义 | 生命周期 |
| --- | --- | --- |
| Session / Conversation | 一段业务会话及其拥有者 | 跨多次用户请求 |
| Request / Turn | 用户提交的一次逻辑工作 | 跨网络重试和执行尝试 |
| Run / Attempt | 一次具体执行尝试 | 可能因接管产生新尝试，保留因果关系 |
| Action / Effect | 一个已确定参数的外部操作 | 跨安全重试或结果核对 |
| Subscription | 某客户端读取执行事件的位置 | 可断开和重新建立 |

这些是建议语义，不要求立即增加五个新 crate 或五套存储。先与现有 ConversationId、CheckpointId、RunId、InvocationId、EffectId 对齐，避免创造重复概念。逻辑请求键必须限定在租户与会话范围；同键不同输入应报冲突，不应悄悄返回旧结果。

### 7.2 拆开执行与观察，保留轻量路径

嵌入式调用继续允许由调用者 poll 流驱动执行，保持现有背压和 WASM 使用方式。生产服务可选择由宿主监督的执行模式：执行有明确的所有者，订阅只读取事件。不能把后台 task 偷偷脱离父 Run，也不能把每个普通 prompt 都强迫交给分布式 worker。

建议的依赖关系为：

```text
HTTP/SSE、终端等应用入口
          │
          ▼
Session / Turn 应用门面 ───► 事件订阅与状态查询
          │                        ▲
          ▼                        │
现有 Agent 状态机 ───────► 提交协调与事件投影
    │             │                  │
    ▼             ▼                  ▼
模型路由      Tool / Effect      现有 Store / Journal
                  │
                  ▼
          业务 API 或执行环境
```

这是逻辑职责图，不是目录迁移方案。初期在已有边界内验证，只有出现独立发布或依赖隔离需求时才考虑新 crate。HTTP、数据库实现、Tokio 专属机制和沙箱都不能进入平台无关核心。

### 7.3 三种状态需要分别可观察

**模型生成状态**：已收到一些内容，可能尚未完成结构校验或语义审查。

**业务执行状态**：工具是否准备、等待审批、执行中、完成或结果不确定。

**会话提交状态**：本轮是否已经和恢复检查点一起持久化。

因此，看到文本增量不等于任务完成，Agent 产生最终候选也不等于会话提交成功。建议对外提供明确的 committed / pending / failed / ambiguous 语义；命名可以在 API 设计时调整。若允许审查前文本出现在 UI，应标记为暂定内容；对于结构化业务客户端，应优先只发布通过验证并提交的结果。

### 7.4 事件恢复需要选择承诺的粒度

一个可用的订阅游标至少需要逻辑执行身份、单调序号和事件 schema 版本。客户端去重应基于事件身份，而不是文本内容。游标失效时返回快照和明确缺口，不重新执行模型来伪造旧流。

不必从第一版持久化每个 token。可以先承诺“重连得到已提交消息、工具状态和当前进度”，短期增量保留在有界缓冲中。若要承诺断线后逐字无损续传，就必须在发布前持久化对应增量并承担成本。两个级别必须在文档中区分。

不能直接把含敏感正文的 UI 流写入原本脱敏的 Journal。更合理的是从持久化会话和必要事件形成受权限控制的投影；需要完整捕获时由应用显式选择存储与保留策略。

### 7.5 Session 并发控制必须发生在外部操作之前

对于同一会话，默认每次只允许一个可变更状态的活跃请求。后续请求可以拒绝、排队，或由应用明确创建分支。进程内互斥不能覆盖多个 worker；需要数据库约束或租约来保证准入，并在接管时检查 fencing 信息。

租约过期也不证明旧 worker 的外部 HTTP 请求已经停止。对不能识别 fencing token 的外部系统，仍需幂等键、结果核对或禁止自动重发。若 Agent 是工作流的一部分，要定义唯一调度拥有者，避免 Session 和 Workflow 两边重复续租、重试或收取预算。

### 7.6 恢复要绑定执行定义，但不能假装模型可完全重现

建议为逻辑任务记录执行清单：Agent 定义版本、模型及路由配置、普通工具名称与版本/schema 摘要、上下文策略、reviewer 版本、预算与授权策略标识。敏感配置只保存安全标识或摘要。

恢复时先校验清单，采取兼容恢复、明确迁移、沿用旧定义或拒绝四类决策。新增无关工具不一定要求拒绝恢复，应区分“已确定计划依赖”与“未来可用工具集合”；不能简单哈希整个运行环境导致任何小改动都无法恢复。

模型别名可能指向变化后的服务，供应商也未必允许锁定权重。清单用于解释和限制变更，不能保证重新生成完全相同的输出。对于已持久化的工具计划，应优先重用原计划和操作身份；未获得完整模型结果的恢复仍须服从歧义策略。

## 8. 十二个生产故障场景推演

下表是建议设计必须通过的反例，不是声称当前版本均存在对应 bug。它们刻意覆盖单个组件测试之外的组合边界。

| 场景 | 容易出现的错误处理 | 建议行为与验收 |
| --- | --- | --- |
| 1. 请求提交成功，但 HTTP 确认丢失 | 客户端重试被当作新任务 | 相同请求键和输入返回原任务；同键不同输入返回冲突；模型与业务调用计数不增加 |
| 2. UI 已收到内容，最终会话提交失败 | 前端显示完成，后端没有完整记录 | 内容标为暂定；提交失败不能发送最终提交事件；查询能返回确定状态 |
| 3. 订阅断开，工具仍在执行 | 无声终止任务，或无所有者地继续执行 | 按显式运行模式处理；托管执行继续受原预算与取消约束，重连读取状态 |
| 4. 工具已写入外部系统，响应丢失 | 自动重试造成重复业务操作 | 用同一逻辑操作身份查询结果；无法核实时进入 ambiguous，不伪造成功或失败 |
| 5. 同一会话两个不同请求同时到达 | 两者都执行外部写入，最后只有一个会话提交成功 | 执行前准入；拒绝/排队策略可测试；明确分支才允许独立执行 |
| 6. 旧 worker 租约过期，新 worker 接管 | 两个 worker 同时对外操作 | 内部写入 fencing；外部操作另需幂等或结果核对；不能把失去租约当作请求未发送 |
| 7. 审批等待期间参数或权限变化 | 一次批准覆盖变化后的动作 | 审批绑定规范化参数、工具版本、业务资源版本和有效期；执行前重验授权 |
| 8. 用户取消时工具恰好完成 | 一律标失败导致重做，或忽略取消继续下一步 | 保留已发生结果，停止后续工作；取消状态与已完成外部操作同时可查询 |
| 9. 摘要模型完成，摘要提交冲突 | 重复收取摘要预算或覆盖新历史 | 摘要带基准版本与范围；CAS 失败后重读；不把未提交摘要作为权威上下文 |
| 10. 部署更新后恢复旧任务 | 同名工具的新语义应用于旧计划 | 兼容性清单检查；不兼容时明确迁移、旧版本处理或拒绝 |
| 11. 并行调用乱序完成，部分失败 | transcript 顺序随机；成功分支被重做 | 操作身份提前固定；事件与历史顺序规则分开；已完成结果可复用；分支独立记账 |
| 12. 读取文档后模型请求外发私有数据 | 仅因工具已注册就执行 | 宿主对本次参数、目的地和权限作决策；不可信上下文不能改变授权来源 |

场景 4 的关键是区分两种现实：外部系统根本未执行，以及已经执行但确认丢失。只观察本地超时，两者可能完全相同。没有下游幂等或可核对证据，运行时无法同时保证自动重试和绝不重复。

LangGraph 官方文档也明确说明 interrupt 前的副作用可能在节点恢复时重执行，因此需要幂等处理。[18](https://docs.langchain.com/oss/python/langgraph/interrupts#side-effects-called-before-interrupt-must-be-idempotent)[^18] 对 Runifold 来说，既有 Effect 歧义处理应继续作为底层约束；“一次且仅一次”必须限定到具体提交范围，不能推广到任意外部 API。

## 9. 上下文、工具与协作的下一层优化

### 9.1 上下文装配：先保证可解释，再尝试智能优化

建议增加可替换的 ContextPolicy / ContextPlan，复用现有 ConversationStore、Summarizer、Retriever 和 ArtifactStore。它应说明本轮加入了哪些来源、保留或省略的原因、预算估算、模型容量和估算可信度。

容量规划可用以下约束表达：

```text
系统与任务约束 + 工具定义 + 选中历史 + 检索内容 + 当前输入
    ≤ 模型上下文容量 − 输出预留 − 安全余量
```

这是规划约束，不是可靠的跨 Provider 精确计费公式。tokenizer、隐藏模板、多模态编码和推理 token 规则可能不同；需要允许未知容量、估算误差和模型能力降级。输入上下文容量与整棵 Run 的累计 token/费用预算也是两回事。

先采用确定性策略：保留必要消息与工具调用/结果配对，缩减重复检索结果，将巨大正文转为 Artifact 引用，再按明确触发条件摘要。引用不等于模型能访问内容，必须提供受权限控制的读取工具；否则只是节省了 token，却切断了证据。

不要将授权、业务状态或精确金额只写在有损摘要中。它们应来自独立的结构化状态或业务系统。重要约束更正必须带来源和版本，避免旧记忆在检索后覆盖新指令。记忆的“过期”“被取代”“用户要求遗忘”也应成为应用策略，而非无限增长的向量集合。

### 9.2 工具选择：注册、可见、可执行是三件事

当前 Agent 会把工具 registry 的规格加入模型请求。对工具较少的应用，这简单可靠；工具增长后，可以加入每轮工具集合选择与按需发现机制，但不必先构建复杂搜索服务。

建议顺序为：静态白名单 → 按任务选择少量工具 → 在规模压力证据出现后增加动态发现。每次真正调用仍需权限验证。模型能看到一个工具不代表获准执行；看不到也不代表无法通过其他入口请求它。

对于 MCP，持久化“哪些工具定义曾被选中”的版本信息，并测试重连后远端 schema 变化。RemoteToolPolicy 的宿主选择不能被工具 annotations 覆盖。MCP 官方 2026 年讨论也强调 annotations 是风险词汇，价值取决于宿主据此采取何种具体行动。[19](https://blog.modelcontextprotocol.io/posts/2026-03-16-tool-annotations/)[^19]

### 9.3 受控并发：不能只替换成 join_all

并发策略至少要处理：总并发数、每个服务并发数、业务资源互斥、预算预留、调用顺序、失败策略和取消。默认保持串行兼容，允许宿主为确实独立的调用启用并发。

“只读”也不自动意味着适合并发：它可能收费、触发限流、读取变化中的状态，或要求先取得某个资源版本。对于写操作，“可幂等重试”和“可与另一个操作并发”也不是同一属性。资源冲突声明应由宿主或业务适配器提供，不能让模型自行承诺安全。

UI 事件可按完成顺序显示进度，但 transcript 应保持协议要求的稳定关联。持久化计划时先确定各调用的身份，再启动任务；恢复时用已有结果填补计划。父 Run 的预算、取消和最终状态应覆盖所有分支，不能在首个成功结果出现后遗留不受管理的操作。

### 9.4 多 Agent：先约束任务接口，再决定是否需要 Team

最小委派契约应包括：目标、输入快照、允许使用的工具、资源预算、输出 schema、证据引用、完成条件和无法完成时的状态。共享上下文必须显式，不应默认复制全部历史或允许子 Agent 获得额外权限。

先提供几种可比较的参考模式：单 Agent；单 Agent 并发工具；负责人委派两到四个独立子任务；固定流程加独立验收。子任务范围和数量是应用配置，不应成为普遍默认。

只有多条实际应用都需要持久化消息、控制权移交或动态队列时，再提炼 Team/Handoff API。Agent-to-Agent 协议可以作为边缘适配器，不能代替本地任务身份、结果验证和预算管理。

## 10. 评估方案：把研究问题变成可验证工程

### 10.1 三层指标

| 层级 | 要验证的问题 | 典型指标 |
| --- | --- | --- |
| 运行时正确性 | 是否违反身份、权限、预算、提交和取消契约 | 重复逻辑操作数、冲突覆盖、越权、预算超支、不可解释状态 |
| 业务任务可靠性 | 任务最终是否正确完成 | 业务终态通过率、pass^k、人工升级率、证据完整性 |
| 使用与经济性 | 团队能否方便地上线并维护 | 首个可用应用时间、业务外样板代码、定位失败时间、每成功任务成本、p95 延迟 |

成功不能只由 AgentOutcome 返回 Ok 判断。退款任务应验证退款账本；工单任务应验证状态和通知记录；研究任务应验证证据和结论。中断或拒绝也不一定是错误，应预先说明哪些情形应交接人工，而不是把所有安全停止都算任务失败。

每成功任务成本应包括失败尝试、摘要、reviewer、委派和安全重试；基础设施及人工成本能量化时另列。不要只统计最终成功路径的模型 token。延迟同时记录首个可用内容时间与最终提交时间，防止通过过早显示未经确认的结果来美化指标。

### 10.2 重复成功率不能和“多试几次总能成功”混淆

pass@k 衡量多次尝试至少一次成功；pass^k 关注多次均成功，后者更接近稳定业务交付。若某案例执行 n 次、有 c 次成功，在 n ≥ k 时，可按 τ-bench 的思路估计 k 次均成功概率为组合数 C(c,k) / C(n,k)，在 c < k 时为零。再跨案例汇总，并报告样本数和不确定性。[公式原文](https://arxiv.org/html/2406.12045v1)。

不能用所有任务的平均成功率直接取 k 次方，任务难度和失败相关性会破坏这种简化。测试环境应为每次试验重置；供应商波动、限流和工具状态要记录。固定随机种子也不保证远端模型完全确定性。

终态相同也可能通过错误过程获得，例如未经批准就完成操作。τ-bench 原文也承认其终态奖励并非充分条件。因此还需验证“先审批后执行”“未向未授权目的地外发”等轨迹约束；业务终态、过程约束和回答质量应分别报告。

### 10.3 三个参考应用与一套故障矩阵

**应用 A：业务会话助手。** SQLite、流式、多轮、一项查询工具、一项模拟变更工具、自动摘要、取消与恢复。作为最短开发路径，首先使用离线模型保证可复现，再做少量真实 Provider 验证。

**应用 B：审批操作助手。** 一个模拟订单或工单服务支持幂等键、查询结果与资源版本。设置审批等待、确认丢失、服务重启和权限变化。按真实业务终态评分，不能只比较回答。

**应用 C：研究助手。** 相同资料集、相同工具与总预算，比较串行工具、受控并发和小规模委派。评估来源覆盖、结论支撑、成本和最终提交延迟。不要把“搜索更广”自动等同于“结论更好”。

每个应用都跑正常、确认丢失、执行超时、取消竞争、提交冲突、上下文溢出和恢复版本不匹配。涉及多个进程或网络时，使用同步屏障和故障注入位置，避免靠随机 sleep 判断正确性。

### 10.4 五个值得验证的研究假设

| 假设 | 对照实验 | 推翻或缩小投入的条件 |
| --- | --- | --- |
| H1：统一 Session 显著减少集成成本 | 相同需求，现有 API 与新门面；记录步骤、编译错误、样板代码和耗时 | 只是缩短命名，复杂度与出错率没有下降 |
| H2：执行前准入能减少并发业务异常 | 相同会话同时提交不同请求，注入慢工具和确认丢失 | 只减少会话冲突，外部操作重复仍无法解释 |
| H3：容量感知上下文比固定条目更稳 | 固定条目、容量裁剪、裁剪加摘要；改变结果大小与关键信息位置 | 省 token 却明显降低约束与事实保留 |
| H4：工具并发比增加 Agent 更经济 | 固定总预算，单 Agent 串行/并发与小团队比较 | 主要收益来自增加预算，或业务冲突明显上升 |
| H5：独立终态检查提高交付可靠性 | 同一候选策略，有/无确定性 verifier；另测 LLM reviewer | 检查器接受错误终态，或高误拒导致总体收益下降 |

这些是建议实验，不是已经得到的实验结论。学术基准提供方法和案例类别，实际门槛由业务损失、延迟容忍与人工处理能力决定。

### 10.5 重放分成三个模式

**证据重放**只展示历史，不执行模型或工具；**离线模拟**用记录或 fixture 重建环境；**真实重新执行**会产生新的模型费用和潜在副作用，必须有新的尝试身份和明确操作策略。

当前 CLI 的 replay 以无副作用证据输出为目标，应保留这个含义。不能在一次“调试重放”中偷偷重新执行外部操作。捕获数据不足时，报告“无法重现”的具体原因；不把缺少正文的脱敏轨迹伪装成完整可执行录制。

## 11. 分阶段演进路线

### 阶段 0：执行契约和基线，优先级 P0

产出 Session/Turn 语义说明、十二场景的状态转换表、现有 API 完整示例及失败注入基线。先固定请求去重、提交、取消和兼容性规则，再设计 Builder。修正当前 quickstart 只引用异步函数、实际不执行的上手问题。

验收：两名未参与实现的 Rust 开发者能根据文档完成离线多轮调用，解释 resume 与下一轮调用的区别，并运行测试。人数是初始试用目标，不足以代表市场样本。

### 阶段 1：可恢复会话入口，优先级 P0

实现薄 Session 门面与会话准入，复用同一 Agent 状态机和 SQLite 提交。绑定稳定请求身份，提供会话版本与任务状态；支持流式与持久化组合。定义执行清单的最小兼容范围。

验收：重复请求、同键冲突、两个并发请求、提交成功后确认丢失、重启恢复、定义变化等场景均有明确结果。已确认完成的操作不重复；结果不明确的操作显式升级处理。不能用自动重试掩盖测试失败。

### 阶段 2：执行与订阅分离、工具暂停，优先级 P0/P1

在阶段 1 的稳定身份上增加宿主监督模式、订阅游标、状态快照及审批/外部结果等待。先保证消息与任务状态可重连，再评估逐 token 持久化的必要性。审批待办复用现有工作流治理能力。

验收：退出订阅不等于取消任务；取消后无新操作；重复或伪造审批不生效；审批对象变化会被拒绝或重新审批；租户只能看到自己的状态和内容。HTTP/SSE 示例属于适配层，核心不承担网站或认证系统。

### 阶段 3：上下文装配与业务回归，优先级 P1

把已有摘要和记忆接入持久化会话完整路径；增加容量估算、巨大结果策略、ContextPlan 和约束保留测试。扩展现有 eval 支持业务终态 scorer 与故障情景标签。

验收：长历史、巨大结果、事实更正、摘要提交冲突与缺失证据案例均可解释；评估输出质量和资源成本。预算不足时能够明确停止或等待，不绕开预算偷偷调用摘要模型。

### 阶段 4：受控并发与小规模委派，优先级 P1/P2

根据研究助手的负载启用 opt-in 并发，固定事件与历史顺序规则、调用身份和预算预留。先保证并行工具，再完善子 Agent 返回契约。

验收：并发上下文与非流式结果一致；乱序、取消、部分失败、恢复和服务限流均通过测试。采用性能控制实验，报告真实时延和成本变化，不承诺理论并发数等于加速倍数。

### 阶段 5：设计伙伴验证与公共 API 收敛

邀请三到五个真实 Rust 应用团队，至少覆盖会话助手与可审批业务操作两类。记录他们重复手写的胶水代码、失败处理、部署限制和放弃原因。将重复出现的需求提炼成公共能力，单个业务独有的逻辑保持在应用中。

各阶段应按验收结果推进，不按预设发布节奏强行合并。这里不提供未经团队规模、维护负担和接口试验校准的工期承诺。

## 12. 暂缓投入与反证条件

| 方向 | 暂缓理由 | 何时重新评估 |
| --- | --- | --- |
| 大量新增 Provider | 当前更明显的缺口在组合与恢复 | 设计伙伴因缺少具体 Provider 无法采用 |
| 通用 Team/Swarm 平台 | 收益依赖任务结构，容易增加上下文和协调成本 | 两类以上应用反复出现同一种动态协作需求 |
| 核心内置沙箱、浏览器、宿主服务 | 会扩大信任、平台与维护边界 | 可选适配器已有明确接口与用户需求 |
| 自研通用记忆算法 | 当前先需要装配策略、版本和评估 | 应用评估证明既有检索/摘要无法满足要求 |
| 默认模型自我优化 | 容易把评估偏差和策略变化带入生产 | 离线比较、版本管理与人工发布流程已稳定 |
| 新一轮深层治理扩展 | 当前已有大量治理能力，应用使用路径更迫切 | 实际部署出现无法用既有机制解决的治理问题 |
| 立刻重构整个 workspace | 风险高，收益尚未经完整应用验证 | 依赖隔离、编译或发布数据证明有必要 |

如果设计伙伴主要将 Runifold 嵌入已有调度系统，Session 应提供接入接口而不是强迫使用第二个 worker。若其任务大多是一次性结构化提取，轻量 prompt 路径仍应优先。若多 Agent 实验没有稳定收益，就停止扩展 Team 层，把投入转向工具契约和评估。

## 13. 本地验证与结论边界

在上述提交上执行了两组已有测试，未修改运行时代码：

```console
cargo test -p runifold-agent --lib streaming_
cargo test -p runifold-store-sqlite --test durable_conversation
```

第一组 2 项通过，覆盖规范 Agent 工具循环的流式顺序与逐事件背压。第二组 4 项通过，覆盖持久化会话重开恢复、检查点冲突回滚、进程退出后确认丢失恢复，以及会话状态保存。子进程测试的运行输出包含一次嵌套测试启动，不能将其另算为第五项。

未执行全 workspace、真实付费 Provider、PostgreSQL 故障、长时 soak 或第三方框架性能对照。因此，这些结果只证实被运行的六项行为。仓库 Reliability 文档列出的 CI 能力是项目已有证据说明，本报告没有重新认证其中全部声明。

产品决策的依据是公开原始研究、维护者资料与本地实现的交叉核对；没有直接访谈 Runifold 用户，也没有生产使用遥测。路线图需要在参考应用和设计伙伴反馈中继续校准。

## 附录 A：本地实现依据

以下文件定位均对应本报告的代码基线；源码后续变化可能影响行号。

| 编号 | 依据 | 用途 |
| --- | --- | --- |
| R1 | [CHARTER](/Users/nova/projects/runifold/docs/CHARTER.md:14) | 核心职责与非目标，会话/日志/记忆分离 |
| R2 | [Agent 执行入口](/Users/nova/projects/runifold/crates/runifold-agent/src/agent/execution.rs:77) | 会话、摘要、流式与恢复入口；执行后提交 |
| R3 | [流式设计 RFC](/Users/nova/projects/runifold/docs/rfcs/0018-agent-streaming.md:1) | 单状态机、背压、drop 行为与延后项 |
| R4 | [流式驱动](/Users/nova/projects/runifold/crates/runifold-agent/src/stream.rs:239) | poll 驱动执行，事件与 future 生命周期 |
| R5 | [工具与子 Agent 调用](/Users/nova/projects/runifold/crates/runifold-agent/src/agent/callable.rs:20) | 单轮调用串行与稳定 effect key |
| R6 | [会话上下文策略](/Users/nova/projects/runifold/crates/runifold-agent/src/conversation/store.rs:258) | 按历史条目限制、语义记忆可选 |
| R7 | [自动摘要](/Users/nova/projects/runifold/crates/runifold-agent/src/conversation/summarizer.rs:1) | 显式摘要边界、预算与最大轮次 |
| R8 | [恢复身份验证](/Users/nova/projects/runifold/crates/runifold-agent/src/agent/execution.rs:941) | Agent、Model、reviewer 等校验范围 |
| R9 | [AgentCheckpointState](/Users/nova/projects/runifold/crates/runifold-agent/src/checkpoint.rs:153) | 持久化身份、阶段、usage 与 reviewer 配置 |
| R10 | [EffectExecutor](/Users/nova/projects/runifold/crates/runifold-effect/src/executor.rs:18) | 歧义恢复、结果核对、默认脱敏 |
| R11 | [ToolDescriptor](/Users/nova/projects/runifold/crates/runifold-tool/src/descriptor.rs:9) | 工具版本、schema、effect 与 risk |
| R12 | [MCP RemoteToolPolicy](/Users/nova/projects/runifold/crates/runifold-mcp/src/remote_tool.rs:13) | 宿主显式选择远端工具权限语义 |
| R13 | [工具规格加入请求](/Users/nova/projects/runifold/crates/runifold-agent/src/agent/execution.rs:1233) | 当前工具定义装配路径 |
| R14 | [工作流构造](/Users/nova/projects/runifold/crates/runifold-workflow/src/workflow.rs:160) | 分支、并行、timer、signal、interrupt |
| R15 | [质量评估 RFC](/Users/nova/projects/runifold/docs/rfcs/0035-agent-quality-evaluation.md:1) | 已有 scorer、报告、隐私和回归策略 |
| R16 | [重复实验 RFC](/Users/nova/projects/runifold/docs/rfcs/0037-reproducible-evaluation-experiments.md:1) | 已有采样、置信区间、缓存和分片 |
| R17 | [持久化会话测试](/Users/nova/projects/runifold/crates/runifold-store-sqlite/tests/durable_conversation.rs:35) | 本次通过的四项测试 |
| R18 | [流式测试](/Users/nova/projects/runifold/crates/runifold-agent/src/agent/tests.rs:2720) | 本次通过的两项测试 |
| R19 | [Quickstart](/Users/nova/projects/runifold/crates/runifold/examples/quickstart.rs:17) | main 只引用函数，未真正执行示例 |
| R20 | [运维 CLI](/Users/nova/projects/runifold/crates/runifold-cli/src/main.rs:1) | 无副作用证据 replay 与运维定位 |
| R21 | [Reliability 矩阵](/Users/nova/projects/runifold/docs/RELIABILITY.md:1) | 项目声明的验证边界与尚需外部证据的范围 |

## 附录 B：来源

所有在线文档和 issue 的访问日期为 2026-09-09。滚动文档未标注发布日期时，不推断其首次发布或软件包版本。下面的编号与正文引用一致。

[^1]: Melissa Z. Pan 等，**Measuring Agents in Production**，arXiv v4，2026-06-04；实证数据采集于 2025 年，论文标记 ICML 2026 Oral。[原文](https://arxiv.org/html/2512.04123v4)。用于需求方向、调查口径和局限。
[^2]: Yubin Kim 等，**Towards a Science of Scaling Agent Systems**，arXiv v3，2026-04-08。[原文](https://arxiv.org/html/2512.08296v3)。用于多 Agent 的任务依赖与实验局限，不将结果推广为通用阈值。
[^3]: Anthropic，**How we built our multi-agent research system**，2025-06-13。[原文](https://www.anthropic.com/engineering/multi-agent-research-system)。厂商工程案例，用于并行研究与协调成本。
[^4]: Mert Cemri 等，**Why Do Multi-Agent LLM Systems Fail?**，arXiv v3，2025-10-26。[原文](https://arxiv.org/html/2503.13657v3)。用于失败分类、委派和验证需求。
[^5]: LangChain，**Join & rejoin streams**，在线文档。[原文](https://docs.langchain.com/oss/python/langchain/frontend/join-rejoin)。用于断线重连与 Agent Server 边界。
[^6]: Pydantic AI，**Cancellation semantics: a level-triggered contract for agent runs, streams, tools, and durable execution**，issue #6460，2026-07-14 创建，访问时 Open。[原文](https://github.com/pydantic/pydantic-ai/issues/6460)。用于真实取消需求，不视为已发布实现。
[^7]: Anthropic，**Scaling Managed Agents: Decoupling the brain from the hands**，2026-04-08。[原文](https://www.anthropic.com/engineering/managed-agents)。用于存储、执行循环和环境的职责分离。
[^8]: Di Wu 等，**LongMemEval: Benchmarking Chat Assistants on Long-Term Interactive Memory**，arXiv v2，2025-03-04，ICLR 2025。[原文](https://arxiv.org/html/2410.10813v2)。用于记忆更新、时间与无依据回答的评估维度。
[^9]: Anthropic，**Effective context engineering for AI agents**，2025-09-29。[原文](https://www.anthropic.com/engineering/effective-context-engineering-for-ai-agents)。用于摘要损失与按需上下文实践。
[^10]: Shunyu Yao 等，**τ-bench: A Benchmark for Tool-Agent-User Interaction in Real-World Domains**，2024-06-17；ICLR 2025 版本另见会议原文。[arXiv](https://arxiv.org/abs/2406.12045)，[会议论文](https://openreview.net/pdf?id=roNSXZpUDN)。用于终态验证与 pass^k，不引用旧模型分数评价当前模型。
[^11]: Aayush Gupta，**ReliabilityBench: Evaluating LLM Agent Reliability Under Production-Like Stress Conditions**，arXiv v1，2026-01-03。[原文](https://arxiv.org/abs/2601.06112v1)。预印本，仅借鉴重复、扰动、故障三个测试维度。
[^12]: Edoardo Debenedetti 等，**AgentDojo: A Dynamic Environment to Evaluate Prompt Injection Attacks and Defenses for LLM Agents**，arXiv v3，2024-11-24。[原文](https://arxiv.org/abs/2406.13352v3)。用于不可信工具内容的评估需求。
[^13]: Model Context Protocol，**Tools**，规范版本 2025-11-25。[原文](https://modelcontextprotocol.io/specification/2025-11-25/server/tools)。用于 annotations 信任边界。
[^14]: Rig / 0xPlaygrounds，**Concurrent tool execution in the streaming driver**，issue #1955 与 PR #1957，2026-06-25；PR 已合并。[需求](https://github.com/0xPlaygrounds/rig/issues/1955)，[实现合并记录](https://github.com/0xPlaygrounds/rig/pull/1957)。不据此推断任意已发布版本的功能。
[^15]: Pydantic，**Durable Execution**，在线文档。[原文](https://pydantic.dev/docs/ai/capabilities/durable_execution/overview/)。用于 durable、streaming 和 MCP 的组合方向。
[^16]: Pydantic，**Deferred Tools**，在线文档。[原文](https://pydantic.dev/docs/ai/tools-toolsets/deferred-tools/)。用于审批与异步外部结果入口；客户端确认不能代替服务端授权。
[^17]: OpenAI，**Running agents — Choose one conversation strategy**，官方在线文档。[原文](https://developers.openai.com/api/docs/guides/agents/running-agents#choose-one-conversation-strategy)。用于会话状态策略的区分。
[^18]: LangChain，**Interrupts — Side effects called before interrupt must be idempotent**，官方在线文档。[原文](https://docs.langchain.com/oss/python/langgraph/interrupts#side-effects-called-before-interrupt-must-be-idempotent)。用于恢复可能重执行副作用的边界。
[^19]: Model Context Protocol Blog，**Tool Annotations as Risk Vocabulary: What Hints Can and Can't Do**，2026-03-16。[原文](https://blog.modelcontextprotocol.io/posts/2026-03-16-tool-annotations/)。用于区分风险提示与宿主实际执行的控制。
