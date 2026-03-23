# hooks/

AIセッション終了時にセッションログを自動保存するためのフック群。

## 目的

AIツール（Claude Code, Copilot CLI, Codex CLI など）のセッション終了時に、
セッションログを `notes/projects/{project}/session-log/` へ自動的に書き出す。

MCP サーバーを経由せず、シェルスクリプトで直接ファイルを書き込む設計。
これにより、MCP接続の有無やAIツールの種類に依存せず安定して動作する。

## アーキテクチャ

```
[Claude Code Stop]  → on-session-end.sh → kb_write_session_log()
[Copilot sessionEnd] → copilot-adapter.sh → on-session-end.sh → kb_write_session_log()
[Codex Stop]        → codex-adapter.sh  → on-session-end.sh → kb_write_session_log()
```

- **on-session-end.sh**: 共通コア。環境変数/引数を受けてセッションログを書く
- **adapters/**: 各ツール固有の hook context を共通コアの形式に変換
- **lib/**: 共通関数（ULID生成、ファイル書き込み、project resolver）

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

### Quick Install（手順表示）

```bash
bash install/hooks.sh          # 全ツールの手順を表示
bash install/hooks.sh claude   # Claude Code のみ
bash install/hooks.sh copilot  # Copilot CLI のみ
bash install/hooks.sh codex    # Codex CLI のみ
```

### ツール別

#### Claude Code

`~/.claude/settings.json` の Stop hooks にコマンドを追加。

```bash
bash hooks/claude-code/install.sh
```

#### Copilot CLI

`.github/hooks/*.json` にセッション終了フックを設定。adapter が stdin JSON を解析。

```bash
bash hooks/copilot-cli/install.sh
```

#### Codex CLI (experimental)

hooks.json で Stop イベントに adapter を登録。JSON stdin で cwd/transcript_path を受け取る。

```bash
bash hooks/codex-cli/install.sh
```

> Codex CLI の hooks は experimental。安定するまでは skills + MCP を先に使うことを推奨。

## 使い方（手動実行）

```bash
# project は自動解決
KB_CWD=/path/to/my-repo ./hooks/on-session-end.sh "Fixed auth bug" claude "session body"

# project を明示指定
./hooks/on-session-end.sh "Fixed auth bug" claude "session body" my-project claude-code github.com/owner/repo

# 環境変数で指定
SUMMARY="Fixed auth bug" AI_TOOL=claude AI_CLIENT=claude-code CONTENT="session body" KB_CWD=/path/to/repo ./hooks/on-session-end.sh
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

- **直接ファイル書き込み**: MCP サーバーを経由しない。信頼性とAIツール非依存性を優先
- **adapter パターン**: 共通コア + ツール固有adapter で hook context の差異を吸収
- **project resolver**: Python 版が single source of truth。Shell 版は thin wrapper
- **ai_tool / ai_client 分離**: `ai_tool` はベンダー名、`ai_client` はクライアント名
- **安全なインストール**: 設定ファイルの自動変更は行わず、手順を表示するのみ
