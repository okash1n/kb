# kb リポジトリ

複数プロジェクト × 複数AIの共通コンテキスト基盤。ローカルMCPサーバーとして動作する。

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
