# hooks/

AI hook payload を `kb-mcp hook dispatch` へ流し込むための互換 shim / adapter 群。

## 目的

AIツール（Claude Code, Copilot CLI, Codex CLI など）の Stop 相当タイミングで、
turn checkpoint や session / tool / error 系イベントを durable event として取り込み、
worker が checkpoint / candidate / promotion / session log へ反映する。

## アーキテクチャ

```
[Claude/Copilot/Codex hook]
  → adapter / on-session-end.sh
  → kb-mcp hook dispatch
  → SQLite event store / outbox
  → kb-mcp worker run-once
  → checkpoint_writer / candidate_writer / promotion_planner / promotion_applier / session_finalizer
```

- **on-session-end.sh**: 旧 entry point を残す互換 shim。内部で `dispatch` を呼ぶ
- **adapters/**: 各ツール固有 payload を shim の入力へ寄せる
- **lib/**: project resolver などの最小補助

## ディレクトリ構造

```
hooks/
  lib/
    kb-utils.sh            # 共通シェル関数（ULID生成、タイムスタンプ、ファイル書き込み）
    kb-resolver.sh          # project resolver（Python resolver の thin wrapper）
  adapters/
    copilot-adapter.sh     # Copilot CLI → on-session-end.sh
    codex-adapter.sh       # Codex CLI → on-session-end.sh
  on-session-end.sh        # 汎用セッション終了フック（共通コア）
  claude-code/
    install.sh             # Claude Code 向けインストール手順表示
  copilot-cli/
    install.sh             # Copilot CLI 向けインストール手順表示
  codex-cli/
    install.sh             # Codex CLI 向けインストール手順表示（experimental）
```

## インストール

```bash
kb-mcp install hooks --all
kb-mcp install hooks --claude --execute
```

`install/hooks.sh` は後方互換 wrapper として残しており、内部では `kb-mcp install hooks` を呼ぶ。

## 現在の保存方針

- Claude / Copilot / Codex の Stop 相当 hook は全部 checkpoint として扱う
- hook 同期パスで直接 `session-log` を作らない
- `gap` / `knowledge` / `adr` 保存や `final_hint` 付き checkpoint をきっかけに、後段 worker が `session-log` を昇格する

### ツール別

#### Claude Code

`~/.claude/settings.json` の Stop hooks に wrapper command を追加する。

```bash
bash hooks/claude-code/install.sh
```

#### Copilot CLI

Copilot config の `hooks.session-end` に wrapper command を追加する。

```bash
bash hooks/copilot-cli/install.sh
```

#### Codex CLI (experimental)

hooks 設定で Stop イベントに wrapper command を登録する。JSON stdin の schema は変わりうるため、現状は snippet 出力を標準とする。

```bash
bash hooks/codex-cli/install.sh
```

> Codex CLI の hooks は experimental。安定するまでは skills + MCP を先に使うことを推奨。

## 使い方（手動実行 / 互換 shim）

```bash
# 旧 entry point をそのまま呼んでも内部では dispatch に転送される
KB_CWD=/path/to/my-repo ./hooks/on-session-end.sh "Fixed auth bug" claude "session body"

# 直接 event pipeline に流す場合
printf '{"session_id":"abc","summary":"Fixed auth bug","content":"session body"}' \
  | kb-mcp hook dispatch --tool claude --client claude-code --layer client_hook --event turn_checkpointed --run-worker
```

### 引数の順序

```
on-session-end.sh <summary> <ai_tool> <content> [project] [ai_client] [repo]
```

| 引数 | 必須 | 説明 |
|---|---|---|
| summary | yes | セッションの一行要約 |
| ai_tool | yes | AI ベンダー: `claude`, `copilot`, `codex` |
| content | yes | セッションログ本文（Markdown） |
| project | — | プロジェクト名（省略時は KB_CWD/KB_REPO から自動解決） |
| ai_client | — | 具体的なクライアント: `claude-code`, `copilot-cli`, `codex-cli` 等 |
| repo | — | リポジトリ識別子（省略時は KB_CWD から自動取得） |

## 依存関係

- bash 4+
- Python 3（ULID生成と project resolver に使用）
- uv（kb-resolver.sh が Python resolver を呼ぶため）

## 設計方針

- **durable enqueue first**: hook 同期パスでは note 保存まで行わず、まず event store に入れる
- **adapter パターン**: 共通 shim + ツール固有 adapter で payload 差分を吸収する
- **project resolver**: Python 版が source of truth。shell は thin wrapper に留める
- **ai_tool / ai_client 分離**: `ai_tool` はベンダー名、`ai_client` はクライアント名
- **後方互換**: 既存 `on-session-end.sh` は残しつつ、内部実装だけ Python pipeline に移す
