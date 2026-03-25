# kb リポジトリ

複数プロジェクト × 複数AIの共通コンテキスト基盤。ローカルMCPサーバーとして動作する。

kb は単なるノート置き場ではなく、AI がセッションをまたいで成長するための共有学習基盤を目指している。
Claude、Copilot、Codex のような複数の AI が、`gap`、`knowledge`、`adr` を通じて「ユーザーが AI に求めること」と「そのプロジェクトで積み上がった判断や知見」を学び、次のやり取りで自然に活かせる状態を作る。

目標は 2 つある。
- 単体 AI が、過去の失敗や判断を踏まえて継続的に賢くなること
- AI チーム全体が、同じユーザー・同じプロジェクト文脈を共有しながら揃って成長していくこと

## アーキテクチャ

- kb_mcp は「Obsidian CLIのラッパー + kb固有ロジック」
- Obsidian CLI 1.12+ 起動済みを前提とする
- Obsidian CLI に委譲: ファイルI/O、frontmatter操作、検索、リンク構造取得
- kb_mcp 固有: ULID生成、slug生成、ファイル命名規則、ADR status管理、organize提案、graduate分析

## データ配置

ノートはこのリポジトリには含まれない。`kb-mcp setup` で設定した Obsidian Vault に保存される。

設定: `~/.config/kb/config.yml`
- `vault_path`: Obsidian Vault のルートパス
- `kb_root`: Vault 内の kb 専用スコープ（空 = Vault 直下、`kb` = サブディレクトリ）

実効パス = `vault_path / kb_root` の下に以下の構造が作られる:

- `projects/<project-name>/` — プロジェクトごとのコンテキスト
  - `adr/` — 意思決定ログ
  - `gap/` — AI指摘 → 本当はどうしてほしかったか
  - `session-log/` — セッションログ
  - `knowledge/` — 開発中に得た知識
  - `draft/` — やりたいこと・アイデアメモ
- `inbox/` — プロジェクトに紐づかないアイデア・雑多なメモ
- `general/` — プロジェクト横断の共通知見（kb_graduateで昇格）
  - `knowledge/` — 共通知識
  - `requirements/` — ユーザーがAIに求めることの集約

## ノートのルール

- frontmatter必須: `id`(ULID), `summary`, `ai_tool`, `created`, `updated`
- frontmatter任意: `ai_client`, `repo`, `tags`, `related`, `status`(ADR用)
- `ai_tool`: AI ベンダー（`claude | copilot | codex`）
- `ai_client`: 具体的なクライアント（`claude-code | copilot-cli | codex-cli` 等、optional）
- 配列フィールド（`tags`, `related`）はインライン形式 `[a, b]` で表現する
- ファイル命名: `{slug}--{ULID}.md`（session-logのみ `{yyyymmdd-hhmm}--{ULID}.md`）
- ノート間の関連付けは ULID（`id`）を正とする（`related` はULID配列）

## Project resolver

- 保存系 tool は `project` を自動解決する（cwd の git remote → `.kb-project.yml` でマッチング）
- 明示的に `project` を指定することもできる
- 各プロジェクトの `.kb-project.yml` に紐付くリポジトリを記録する

## MCP Tools

| tool | 役割 |
|---|---|
| `kb_init` | プロジェクト初期化 |
| `kb_adr` | 意思決定ログ保存 |
| `kb_gap` | 反省記録保存 |
| `kb_knowledge` | 知識保存 |
| `kb_session` | セッションログ保存 |
| `kb_draft` | アイデア・やりたいことメモ（project指定→draft/、なし→inbox/） |
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

## Hooks / Events

hook は直接ノートを書き込むのではなく、`kb-mcp hook dispatch` で event pipeline に入る。

流れ:
1. client hook / launcher / middleware が raw event を送る
2. `dispatch` が normalize + redact + SQLite 永続化を行う
3. worker が checkpoint / candidate / promotion / finalizer 系 sink を処理する

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

learning contract:
- governed runtime learning contract の要点と command map は `docs/governed-runtime-learning-contract.md` を参照
- client ごとの配布制約は `docs/learning-client-capabilities.md` を参照

cross-client 前提:
- Claude / Copilot / Codex の hook はすべて checkpoint 入力として扱う
- vendor 固有 tool hook がなくても、server middleware event と checkpoint text から judge 入力を組み立てる

## 変更履歴

`CHANGELOG.md` を参照。
