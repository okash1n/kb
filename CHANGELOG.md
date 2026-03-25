# Changelog

このファイルはこのリポジトリの利用者向け変更履歴を管理する。

形式は [Keep a Changelog](https://keepachangelog.com/ja/1.1.0/) をベースにし、
バージョニングは [Semantic Versioning](https://semver.org/lang/ja/) に従う。

## [Unreleased]

## [0.6.1] - 2026-03-25

### Changed

- `doctor` の legacy path 表示を `Legacy path present check` に変更し、legacy file が無い通常運用を `✓`、legacy repo path 検出時を cleanup candidate として示すようにした

## [0.6.0] - 2026-03-25

### Added

- judge / candidate / human review 用の review ledger schema と store API を追加
- partition / ordinal から judge 用 window payload を再構成する `judge_inputs` と deterministic signal extractor を追加
- `kb-mcp judge review-candidates` と judge backend / runner の初期実装を追加
- `kb-mcp judge accept` / `reject` / `relabel` を追加し、human review verdict を CLI から記録できるようにした

### Changed

- event DB schema version を `3` に上げ、review ledger migration を有効化した
- `topic_shift_candidate` と `knowledge` 補助 signal を cross-client 共通ルールで抽出するようにした
- `doctor` に judge backlog / review ledger / runtime metric failure の診断を追加した
- review suggestion を pending backlog 基準に変更し、新規候補流入時の再提示を安定化した

## [0.5.1] - 2026-03-25

### Added

- `kb-mcp --version` と `kb-mcp version` で現在バージョンを確認できるようにした

### Changed

- package metadata が無い開発実行では `kb-mcp (dev)` を返すようにした

## [0.5.0] - 2026-03-25

### Added

- hook / tool / launcher の全入力をまず checkpoint と event store に集約し、その後段で memory promotion する基盤
- `gap` / `knowledge` / `adr` 保存を anchor に rich `session-log` を昇格する planner / applier
- `final_hint` / `checkpoint_kind=session_end` を使って thin `session-log` を切り出す planner / applier
- `kb-mcp worker replay-dead-letter` と `kb-mcp worker cleanup-runtime`

### Changed

- `session-log` を主役ではなく checkpoint 群の編集済みビューとして扱うよう再設計
- `doctor` に dead-letter と promotion runtime state の診断を追加
- runtime artifact を `checkpoints` / `candidates` / `promotions` / `promotion-records` に整理

## [0.4.2] - 2026-03-25

### Fixed

- Codex / Claude / Copilot の Stop 相当 hook を `session_ended` ではなく checkpoint 系イベントとして扱うように修正
- Stop ごとに session-log が増えていた問題を修正し、launcher 管理の本当の session 終了時だけ session-log を作るように変更

### Changed

- hook pipeline 内の event semantics を整理し、turn 単位の保存は checkpoint に寄せた

## [0.4.1] - 2026-03-25

### Fixed

- Codex `Stop` hook の manual install 出力を実 schema に合わせ、`doctor` でも `hooks.json` と `config.toml` を確認するよう修正
- Codex hook wrapper が `stdout` に dispatch 結果 JSON を流して `Stop running, failed` になる不具合を修正
- Codex transcript `.jsonl` から event log 全体ではなく会話メッセージだけを抜粋するように修正
- `client_hook` の `session_ended` で毎回 session-log を作っていた挙動をやめ、checkpoint のみに変更

### Changed

- session-log の本文生成を、人が読める会話抜粋ベースに寄せた

## [0.4.0] - 2026-03-25

### Added

- SQLite ベースの hook event pipeline と `kb-mcp hook dispatch` / `kb-mcp worker run-once` / `kb-mcp session run`
- kb-owned MCP tool 用の authoritative `tool_started` / `tool_succeeded` / `tool_failed` wrapper
- `CHANGELOG.md` 自体の導入と `Unreleased` 運用

### Changed

- `install hooks` を wrapper / snippet ベースの導線に整理
- `doctor` を event DB / scheduler / hook wrapper を見る診断へ拡張
- 旧 `hooks/on-session-end.sh` を直接ファイル書き込みから dispatch shim に変更

## [0.3.0]

### Added

- Obsidian Vault を保存先に使う `kb-mcp` のローカル MCP サーバー実装
- `kb_init`, `kb_adr`, `kb_gap`, `kb_session`, `kb_knowledge`, `kb_draft` などの保存系ツール
- `kb_search`, `kb_read`, `kb_lint`, `kb_organize`, `kb_graduate` などの検索・整合性確認ツール
- `kb-mcp setup`, `kb-mcp serve`, `kb-mcp install hooks`, `kb-mcp doctor` などの基本 CLI
- Claude Code / Codex CLI / GitHub Copilot CLI 向けの MCP 登録例と hooks 資産
