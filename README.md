# kb

複数プロジェクト × 複数AIの共通コンテキスト基盤。ローカルMCPサーバーとして動作する。

kb は単なるノート置き場ではなく、AI がセッションをまたいで成長するための共有学習基盤を目指している。
Claude、Copilot、Codex のような複数の AI が、`gap`、`knowledge`、`adr` を通じて「ユーザーが AI に求めること」と「そのプロジェクトで積み上がった判断や知見」を学び、次のやり取りで自然に活かせる状態を作る。

目標は 2 つある。
- 単体 AI が、過去の失敗や判断を踏まえて継続的に賢くなること
- AI チーム全体が、同じユーザー・同じプロジェクト文脈を共有しながら揃って成長していくこと

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

保存系 / 検索系の入力互換:
- `kb_adr` / `kb_gap` / `kb_knowledge` / `kb_draft` の `slug` は省略可能。未指定時は `summary` から補完される
- `tags` / `related` / `kb_search.tags` は配列を優先するが、クライアント互換のためカンマ区切り文字列や JSON 風文字列も受け付ける
- schema 更新後もクライアントが古い定義を保持している場合は、MCP サーバーの再起動または再接続が必要になることがある

## CLI コマンド

| コマンド | 役割 |
|---|---|
| `kb-mcp setup` | 初期設定（Vault パス、タイムゾーン） |
| `kb-mcp serve` | MCP サーバー起動 |
| `kb-mcp version` | 現在の `kb-mcp` バージョン表示 |
| `kb-mcp config get <key>` | 設定値取得 |
| `kb-mcp install hooks` | lifecycle hook の wrapper / snippet 生成 |
| `kb-mcp hook dispatch` | raw hook payload を durable event として取り込む |
| `kb-mcp worker run-once` | due な sink を 1 回 drain する |
| `kb-mcp worker replay-dead-letter` | dead-letter 化した sink を ready に戻す |
| `kb-mcp worker cleanup-runtime` | 古い runtime artifact を削除する |
| `kb-mcp worker repair-learning-runtime` | learning packet / asset / application の runtime hygiene を補修する |
| `kb-mcp session run` | launcher 管理下で AI セッションを起動する |
| `kb-mcp doctor` | config, event DB, scheduler, hooks, judge/review runtime を診断する |
| `kb-mcp judge review-candidates` | checkpoint window を judge して review 候補を生成する |
| `kb-mcp judge accept <candidate-key>` | review 候補を accept する |
| `kb-mcp judge reject <candidate-key>` | review 候補を reject する |
| `kb-mcp judge relabel <candidate-key> --label <label>` | review 候補を別ラベルへ relabel する |
| `kb-mcp judge materialize [<candidate-key>]` | accepted / relabeled candidate を note materialize する |
| `kb-mcp judge learning-state` | learning asset の visibility と主要属性を確認する |
| `kb-mcp judge retract-learning <asset-key> --reason <reason>` | active learning asset を撤回する |
| `kb-mcp judge supersede-learning <asset-key> --replacement-asset-key <asset-key> --reason <reason>` | learning asset を後継 asset で supersede する |
| `kb-mcp judge expire-learning --before <timestamp> --reason <reason>` | stale learning asset を期限切れにする |
| `kb-mcp judge build-policy-snapshots` | active learning asset から runtime policy snapshot を生成する |
| `kb-mcp judge promote-scopes` | active project-local asset を wider scope へ昇格する |
| `kb-mcp judge retry-failed-materializations` | failed / repair_pending materialization を再投入する |

バージョン確認:
```bash
kb-mcp --version
kb-mcp version
```

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

judge / review の流れ:
1. `kb-mcp judge review-candidates` で checkpoint window を再読して候補を作る
2. `kb-mcp doctor` で pending backlog / judge failure を確認する
3. `kb-mcp judge accept` / `reject` / `relabel` で human verdict を review ledger に保存する
4. `kb-mcp judge materialize` / `retry-failed-materializations` で accepted candidate を note へ反映する

runtime hygiene:
- `kb-mcp doctor` は expired packet / orphan application / stale local asset を表示する
- `kb-mcp worker repair-learning-runtime` は doctor で見つかった learning runtime の補修を行う
- 詳細は [docs/learning-runtime-hygiene.md](docs/learning-runtime-hygiene.md) を参照

learning contract:
- governed runtime learning contract の要点と command map は [docs/governed-runtime-learning-contract.md](docs/governed-runtime-learning-contract.md) を参照
- client ごとの配布制約は [docs/learning-client-capabilities.md](docs/learning-client-capabilities.md) を参照

fast-path judge:
- `KB_JUDGE_FASTPATH_COMMAND` を設定した hook wrapper だけが `hook dispatch --judge-fastpath` を有効にする
- fast-path backend は contract version `1` と timeout `1.5s` を使う
- backend 未設定 / timeout / breaker open 時は hook 完了を優先し、fallback judge は後段 review を塞がない prompt version で記録する
- 通常経路は `hook -> dispatch -> worker` のままで、fast-path judge は optional な inline 分岐としてだけ動く

cross-client 前提:
- Claude / Copilot / Codex の hook はすべて checkpoint 入力として扱う
- vendor 固有 tool hook がなくても、server middleware event と checkpoint text から judge 入力を組み立てる

release 前の最小確認:
```bash
uv run python -m unittest tests.test_judge_cli tests.test_judge_review_cli tests.test_materialize_cli tests.test_fastpath_judge tests.test_install_and_doctor tests.test_event_pipeline tests.test_judge_inputs tests.test_cli_version -v
python -m compileall src tests
uv build
kb-mcp doctor --no-version-check
```

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
