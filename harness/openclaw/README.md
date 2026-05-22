# openclaw-opik-tracer

把 `openclaw` 会话追踪到 Opik 的本地插件。它不是直接依赖 hook payload 里的事件内容，而是把 hook 只当成触发器，然后去读取 `openclaw` 的 session JSONL transcript，再增量解析并上报到 Opik。

这样做的目的，是规避 hook payload 在复杂场景下可能出现的 `sessionKey` 缺失、并发覆盖、结束时机过早等问题。

## 目录说明

- `index.ts`: OpenClaw 插件入口，注册 hook。
- `src/bridge.ts`: Node 侧桥接层，负责拉起 Python tracer。
- `tracer/openclaw_opik_tracer.py`: 真正的增量解析与 Opik 上报逻辑。
- `openclaw.plugin.json`: 插件清单和配置 schema。
- `tests/test_openclaw_opik_tracer.py`: Python tracer 单测。

## 工作原理

插件在以下 hook 上触发：

- `llm_input`
- `llm_output`
- `before_agent_reply`
- `after_tool_call`
- `agent_end`
- `session_end`
- `subagent_spawned`
- `subagent_ended`

每次触发时，TypeScript 层会把当前会话上下文和插件配置打包，通过 stdin 传给 `tracer/openclaw_opik_tracer.py`。Python tracer 再读取对应的 session transcript 文件，按 offset 增量解析，维护本地状态，并把 trace/span 发到 Opik。

本地状态和日志默认写在：

- `~/.openclaw/state/opik_tracer_state.json`
- `~/.openclaw/state/opik_tracer.log`

## 环境要求

- Node.js 22 或以上
- `npm`
- Python 3
- 一个可用的 `openclaw` 环境
- 一个可用的 Opik 项目/API Key

## 1. 编译插件

项目根目录：

```bash
cd /Users/sgwhat/trace-agent/opik-script/useful_opik/clean-workspace/openclaw-opik-tracer
```

安装 Node 依赖：

```bash
npm install
```

编译 TypeScript：

```bash
npm run build
```

编译产物会输出到：

```text
dist/index.js
dist/src/bridge.js
```

如果你在开发中需要持续编译：

```bash
npm run dev
```

## 2. 安装 Python tracer 依赖

建议用独立虚拟环境，避免污染系统 Python：

```bash
cd /Users/sgwhat/trace-agent/opik-script/useful_opik/clean-workspace/openclaw-opik-tracer
python3 -m venv .venv
source .venv/bin/activate
pip install -r tracer/requirements.txt
```

当前 `tracer/requirements.txt` 至少包含：

```text
opik>=1.0.0
```

如果你希望 tracer 使用这个虚拟环境里的 Python，后面在 `openclaw` 插件配置里把 `pythonPath` 指到：

```text
/Users/sgwhat/trace-agent/opik-script/useful_opik/clean-workspace/openclaw-opik-tracer/.venv/bin/python
```

## 3. 本地验证编译和测试

在当前仓库里，下面两条命令可以通过：

```bash
npm run build
python3 -m unittest tests/test_openclaw_opik_tracer.py
```

如果测试时看到 `urllib3`、`LiteLLM` 或 `sentry_sdk` 的 warning，但最终结果是 `OK`，通常不影响本插件逻辑。

## 4. 安装到 openclaw

`openclaw` 支持直接从本地路径安装插件。

如果你已经有 `openclaw` CLI：

```bash
openclaw plugins install /Users/sgwhat/trace-agent/opik-script/useful_opik/clean-workspace/openclaw-opik-tracer
```

安装后可以确认插件是否存在：

```bash
openclaw plugins list --verbose
openclaw plugins inspect openclaw-opik-tracer
```

如果你希望以“链接源码目录”的方式接入，便于反复修改本地代码，也可以用：

```bash
openclaw plugins install --link /Users/sgwhat/trace-agent/opik-script/useful_opik/clean-workspace/openclaw-opik-tracer
```

这种方式更适合开发调试；改完代码重新 `npm run build` 即可让 `openclaw` 加载新产物。

## 5. 配置 openclaw 插件参数

这个插件的配置路径是：

```text
plugins.entries.openclaw-opik-tracer.config
```

至少建议配置：

- `opikApiKey`
- `opikWorkspace`
- `opikProjectName`
- `pythonPath`

可以直接用 CLI 设置：

```bash
openclaw config set plugins.entries.openclaw-opik-tracer.enabled true
openclaw config set plugins.entries.openclaw-opik-tracer.config.opikUrl "https://www.comet.com/opik/api"
openclaw config set plugins.entries.openclaw-opik-tracer.config.opikApiKey "<YOUR_OPIK_API_KEY>"
openclaw config set plugins.entries.openclaw-opik-tracer.config.opikWorkspace "default"
openclaw config set plugins.entries.openclaw-opik-tracer.config.opikProjectName "openclaw"
openclaw config set plugins.entries.openclaw-opik-tracer.config.pythonPath "/Users/sgwhat/trace-agent/opik-script/useful_opik/clean-workspace/openclaw-opik-tracer/.venv/bin/python"
```

可选项：

```bash
openclaw config set plugins.entries.openclaw-opik-tracer.config.includeHistory true
openclaw config set plugins.entries.openclaw-opik-tracer.config.dryRun false
```

也可以直接编辑 `~/.openclaw/openclaw.json`，示例：

```json
{
  "plugins": {
    "entries": {
      "openclaw-opik-tracer": {
        "enabled": true,
        "config": {
          "opikUrl": "https://www.comet.com/opik/api",
          "opikApiKey": "<YOUR_OPIK_API_KEY>",
          "opikWorkspace": "default",
          "opikProjectName": "openclaw",
          "pythonPath": "/Users/sgwhat/trace-agent/opik-script/useful_opik/clean-workspace/openclaw-opik-tracer/.venv/bin/python",
          "tags": ["openclaw", "local"],
          "includeHistory": false,
          "dryRun": false
        }
      }
    }
  }
}
```

## 6. 重启 openclaw

配置完成后，重启 `openclaw` gateway，让插件重新加载：

```bash
openclaw gateway restart
```

如果你当前是前台直接运行的 gateway，也可以停掉后重新启动：

```bash
openclaw gateway
```

建议顺手跑一次：

```bash
openclaw doctor
```

## 7. 验证集成是否生效

最简单的验证方法：

1. 启动 `openclaw`。
2. 发起一轮真实对话，最好包含一次 LLM 回复和一次工具调用。
3. 查看 Opik 项目里是否出现新的 trace。
4. 如果没有 trace，先看本地 tracer 日志。

日志路径：

```bash
tail -f ~/.openclaw/state/opik_tracer.log
```

常见成功信号：

- 会话开始后出现新 trace
- 一轮对话会产生 turn 级 span
- 工具调用后出现 tool span
- 子 agent 流程会出现子 span 或子会话追踪

## 8. 常见问题

### 1. `openclaw` 能装上插件，但没有任何 Opik 数据

优先检查：

- `pythonPath` 是否正确
- 该 Python 里是否安装了 `opik`
- `opikApiKey` / `opikWorkspace` / `opikUrl` 是否正确
- `~/.openclaw/state/opik_tracer.log` 是否有报错

### 2. TypeScript 编译成功，但运行时报找不到 Python 脚本

默认脚本路径由 `src/bridge.ts` 推断：

- 编译后优先找 `../../tracer/openclaw_opik_tracer.py`
- 源码模式下再找 `../tracer/openclaw_opik_tracer.py`

所以不要把 `tracer/` 目录和编译产物拆开部署。

### 3. 想先验证解析逻辑，不真的发到 Opik

可以打开 dry-run：

```bash
openclaw config set plugins.entries.openclaw-opik-tracer.config.dryRun true
openclaw gateway restart
```

### 4. 想把完整历史一并发给 Opik

可以打开：

```bash
openclaw config set plugins.entries.openclaw-opik-tracer.config.includeHistory true
```

但这会增大 payload，建议先按默认 `false` 跑通。

## 9. 推荐集成流程

如果你的目标是稳定接到本地 `openclaw`，建议按这个顺序：

1. `npm install`
2. `npm run build`
3. 创建 Python venv 并安装 `tracer/requirements.txt`
4. `openclaw plugins install <本项目路径>`
5. 配置 `plugins.entries.openclaw-opik-tracer.*`
6. `openclaw gateway restart`
7. 跑一次真实会话
8. 看 Opik 页面和 `~/.openclaw/state/opik_tracer.log`

## 10. 当前版本信息

当前插件元数据：

- 插件 ID: `openclaw-opik-tracer`
- 入口文件: `dist/index.js`
- `pluginApi`: `>=2026.4.8`
- `minGatewayVersion`: `2026.4.8`

如果你的 `openclaw` 版本明显低于这个范围，优先升级 `openclaw`，否则可能出现插件不加载或 hook 不兼容。
