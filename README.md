# kb

複数プロジェクト × 複数AIの共通コンテキスト基盤。ローカルMCPサーバーとして動作する。

## 技術スタック

- Python + uv
- MCP SDK（公式Python版）
- Obsidian CLI 1.12+
- git

## Installation

### Prerequisites

- [uv](https://docs.astral.sh/uv/)
- [Obsidian](https://obsidian.md/) 1.12+ (CLI 有効化: Settings > General > Advanced > Command Line Interface)

### 1. Install

```bash
uv tool install kb-mcp
```

### 2. Setup

```bash
kb-mcp setup
```

対話形式で以下を設定:
- Obsidian Vault のパス（新規作成 or 既存 Vault に統合）
- タイムゾーン

設定は `~/.config/kb/config.yml` に保存される。

### 3. MCP サーバー登録

使用する AI ツールに合わせて登録する。複数ツールから同じ `kb-mcp` を共有できる。

#### Claude Code

```bash
claude mcp add kb --scope user -- kb-mcp serve
```

#### Codex CLI

`~/.codex/config.toml` に追加:

```toml
[mcp_servers.kb]
command = "kb-mcp"
args = ["serve"]
```

#### GitHub Copilot CLI

`~/.copilot/mcp-config.json` を作成（または追記）:

```json
{
  "mcpServers": {
    "kb": {
      "command": "kb-mcp",
      "args": ["serve"]
    }
  }
}
```

#### Visual Studio Code (Copilot Chat)

コマンドパレット（`Cmd+Shift+P`）から:

1. `MCP: Add Server` を選択
2. `Command (stdio)` を選択
3. コマンド → `kb-mcp serve` を入力してエンター
4. サーバー ID → `kb` を入力してエンター
5. スコープ → `Global` を選択（全ワークスペースで有効にする場合）

または手動で `~/.vscode/mcp.json`（グローバル）に追加:

```json
{
  "servers": {
    "kb": {
      "command": "kb-mcp",
      "args": ["serve"]
    }
  }
}
```

プロジェクト単位で設定する場合は `.vscode/mcp.json` に同じ内容を配置する。

### 4. Skills / Hooks インストール（オプション）

```bash
kb-mcp install skills --claude --execute   # Claude Code 用スキル配置
kb-mcp install hooks --claude              # Claude Code 用フック配置
kb-mcp install hooks --codex               # Codex CLI 用フック配置
```

### 5. 動作確認

```bash
kb-mcp doctor
```

## データ配置

ノートはこのリポジトリには含まれない。`kb-mcp setup` で設定した Obsidian Vault に保存される。

```
<vault_path>/<kb_root>/
  projects/<project-name>/
    adr/            # 意思決定ログ
    gap/            # AI指摘 → 本当はどうしてほしかったか
    session-log/    # セッションログ
    knowledge/      # 開発中に得た知識
    draft/          # やりたいこと・アイデアメモ
    history.md      # プロジェクト変遷まとめ
  inbox/            # プロジェクトに紐づかないアイデア
  general/
    knowledge/      # プロジェクト横断の共通知見
    requirements/   # ユーザーがAIに求めることの集約
```

## MCP Tools

| tool | 役割 |
|---|---|
| `kb_init` | プロジェクト初期化 |
| `kb_adr` | 意思決定ログ保存 |
| `kb_gap` | 反省記録保存 |
| `kb_knowledge` | 知識保存 |
| `kb_session` | セッションログ保存 |
| `kb_draft` | アイデア・やりたいことメモ |
| `kb_search` | 検索 |
| `kb_read` | ノート読み込み |
| `kb_lint` | ルール整合性チェック |
| `kb_organize` | リンク候補の発見・提案 |
| `kb_graduate` | general/への昇格提案 |

## CLI コマンド

| コマンド | 役割 |
|---|---|
| `kb-mcp setup` | 初期設定（Vault パス、タイムゾーン） |
| `kb-mcp serve` | MCP サーバー起動 |
| `kb-mcp config get <key>` | 設定値取得 |
| `kb-mcp install skills` | スキルファイル配置 |
| `kb-mcp install hooks` | フック配置 |
| `kb-mcp doctor` | 環境診断 |

## ファイル命名

- adr / gap / knowledge / draft: `{slug}--{ULID}.md`
- session-log: `{yyyymmdd-hhmm}--{ULID}.md`

## frontmatter

```yaml
---
id: ULID
summary: 要約
ai_tool: claude | copilot | codex
ai_client: claude-code | copilot-cli | codex-cli  # optional
repo: github.com/owner/repo  # optional
tags: []                      # optional
related: []                   # optional
status: accepted              # ADR用 optional
created: YYYY-MM-DDTHH:MM+09:00
updated: YYYY-MM-DDTHH:MM+09:00
---
```

## ライセンス

MIT
