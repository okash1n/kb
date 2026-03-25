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

`~/.codex/config.toml`（`CODEX_HOME` 設定時はそちら）に追加:

```toml
[mcp_servers.kb]
command = "kb-mcp"
args = ["serve"]
```

#### GitHub Copilot CLI

`~/.copilot/mcp-config.json`（`COPILOT_HOME` 設定時はそちら）を作成（または追記）:

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


### 4. Hooks インストール（オプション）

```bash
kb-mcp install hooks --all                 # 全ツールの hook snippet / wrapper を用意
kb-mcp install hooks --claude --execute    # 可能なものは設定ファイルまで反映
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
| `kb-mcp install hooks` | lifecycle hook の wrapper / snippet 生成 |
| `kb-mcp hook dispatch` | raw hook payload を durable event として取り込む |
| `kb-mcp worker run-once` | due な sink を 1 回 drain する |
| `kb-mcp worker replay-dead-letter` | dead-letter 化した sink を ready に戻す |
| `kb-mcp worker cleanup-runtime` | 古い runtime artifact を削除する |
| `kb-mcp session run` | launcher 管理下で AI セッションを起動する |
| `kb-mcp doctor` | config, event DB, scheduler, hooks を診断する |

## Hooks / Events

hook は直接ノートを書き込むのではなく、`kb-mcp hook dispatch` で event pipeline に入る。

流れ:
1. client hook / launcher / middleware が raw event を送る
2. `dispatch` が normalize + redact + SQLite 永続化を行う
3. worker が checkpoint / candidate / promotion / finalizer 系 sink を処理する

旧 `hooks/on-session-end.sh` は互換 shim として残しており、内部では `dispatch` を呼ぶ。

memory promotion の考え方:
- 全 hook はまず checkpoint として保存する
- `gap` / `knowledge` / `adr` が anchor になった時だけ rich `session-log` を昇格する
- `final_hint` 付き checkpoint は thin `session-log` の区切り候補に使う

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

## 変更履歴

[CHANGELOG.md](CHANGELOG.md) を参照。
