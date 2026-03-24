# Changelog

このファイルはこのリポジトリの利用者向け変更履歴を管理する。

形式は [Keep a Changelog](https://keepachangelog.com/ja/1.1.0/) をベースにし、
バージョニングは [Semantic Versioning](https://semver.org/lang/ja/) に従う。

## [Unreleased]

### Changed

- まだ未整理

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
