# Changelog

このファイルはこのリポジトリの利用者向け変更履歴を管理する。

形式は [Keep a Changelog](https://keepachangelog.com/ja/1.1.0/) をベースにし、
バージョニングは [Semantic Versioning](https://semver.org/lang/ja/) に従う。

## [Unreleased]

## [0.18.0] - 2026-03-26

### Fixed

- thin `session-log` 候補の window 上限を 10 checkpoint から 5 checkpoint に下げ、anchor なし会話でも 15 checkpoint で `session_thin` 候補が出るようにした
- `session_thin` の judge 回帰テストを 5 checkpoint window 前提に更新し、carry chain terminal 条件が実運用の会話粒度から外れにくいようにした
- README の release 前確認から削除済み `kb-mcp doctor --no-version-check` を取り除いた

### Changed

- memory promotion の説明に、anchor なし 3 window で thin `session-log` 候補を出す条件を追記した
- `0.18.0` リリースに向けて minor version を更新した

## [0.17.5] - 2026-03-26

### Fixed

- `promotion_applier` が note materialization / session promotion 成功後に対応する promotion plan JSON を削除するようにし、`Promotion plans` が未処理 backlog と一致するようにした
- promotion plan cleanup の回帰テストを追加し、適用済み plan が runtime に残り続ける状態を再発しにくくした

### Changed

- `0.17.5` リリースに向けて patch version を更新した

## [0.17.4] - 2026-03-26

### Fixed

- `kb-mcp doctor` が毎回現在バージョンと PyPI 上の最新版を表示し、更新要否を先頭で判断できるようにした
- `kb-mcp doctor` の未使用オプション `--no-version-check` を削除し、CLI surface と実装のずれを解消した

### Changed

- `AGENTS.md` を `CLAUDE.md` への symlink に反転し、Claude 系の learned rules を含む実体ファイルを正本として扱うようにした
- `0.17.4` リリースに向けて patch version を更新した

## [0.17.3] - 2026-03-26

### Fixed

- `tests.test_tool_input_compat` に `kb_mcp.server.adr/gap/knowledge/draft` の Python 直呼び出し互換 wrapper を直接叩く回帰テストを追加し、MCP schema が正しくても import 利用だけ壊れる状態を検知できるようにした
- `kb_session` の互換入力テストを拡張し、`tags` だけでなく `related` の JSON 風文字列入力も正規化されることを固定した

### Changed

- `0.17.3` リリースに向けて patch version を更新した

## [0.17.2] - 2026-03-26

### Fixed

- `kb_adr` / `kb_gap` / `kb_knowledge` / `kb_draft` の `slug` を任意化し、未指定時は `summary` から自動補完するようにして、クライアントから見た required 条件と保存実装の挙動を一致させた
- 保存系ツールの `tags` / `related` と `kb_search.tags` で、配列に加えてカンマ区切り文字列や JSON 風文字列も受け付けるようにし、クライアントごとの入力差異で保存・検索が失敗しにくいようにした
- `kb_search` はノート側の `tags` 取得結果も正規化して比較するようにし、文字列化された tags による誤判定を防ぐようにした
- MCP schema と互換入力の回帰テストを追加し、`slug` / `tags` / `related` の公開面が再び実装とずれにくいようにした

### Changed

- `0.17.2` リリースに向けて patch version を更新した
- 互換入力を使うクライアントでは、修正版 schema を反映するために MCP サーバーの再起動または再接続が必要になる場合がある

## [0.17.1] - 2026-03-26

### Fixed

- `kb_mcp.learning.scope_promotion` の `EventStore` import を遅延化し、`kb_mcp.events.store` との循環 import で hook dispatch が落ちる問題を修正した
- import graph の回帰テストを追加し、`.venv` 経由の実行でも circular import が再発しないことを確認できるようにした

## [0.17.0] - 2026-03-26

### Added

- `docs/governed-runtime-learning-contract.md` を追加し、governed runtime learning contract の中核概念、runtime semantics、serving / apply / governance の接続面を README から辿れるようにした
- `tests/test_cli_surface.py` を追加し、worker / judge の learning runtime surface が CLI parser から消えないことを回帰テストで固定した

### Changed

- README を更新し、governed runtime learning の現在地、judge governance command、runtime hygiene command、関連ドキュメントへの導線を実装 surface に合わせて統合した
- `0.8.0` から `0.16.0` まで追加した learning runtime / governance / repair surface を `0.17.0` の統合到達版として整理した

## [0.16.0] - 2026-03-26

### Added

- `docs/learning-runtime-hygiene.md` を追加し、doctor が表示する learning runtime hygiene 指標と `kb-mcp worker repair-learning-runtime` の使い方を整理した
- `kb-mcp worker repair-learning-runtime` を追加し、expired packet / stale local asset / orphan application / legacy wide-scope traceability fallback を runtime 上で補修できるようにした

### Changed

- `doctor` に learning runtime hygiene lines を追加し、packet asset mismatch、orphan application、stale local asset、legacy traceability fallback を診断できるようにした
- malformed な legacy `traceability_json` が混ざっていても hygiene 集計と repair が落ちないようにし、Codex MCP config も unreadable fallback で安全に扱うようにした

## [0.15.1] - 2026-03-26

### Added

- `docs/learning-client-capabilities.md` を追加し、Claude / Copilot / Codex / unknown client ごとの runtime learning capability rule を明文化した

### Changed

- resolver と packet builder に cross-client capability control を追加し、Copilot 系では `general` scope を omit、`user_global` force を段階的に downgrade するようにした
- unknown client は fail-close で local scope のみ許可し、`copilot-*` / `claude-*` / `codex-*` の variant は family alias で既知 capability に解決するようにした
- wide scope asset の `secrecy_boundary` は明示値を優先しつつ、legacy row では `memory_class` 由来 metadata から compatibility fallback できるようにした

## [0.15.0] - 2026-03-26

### Added

- `learning.metrics` と doctor outcome lines を追加し、same-gap recurrence / knowledge re-query / ADR re-discussion / cross-client consistency を runtime outcome として観測できるようにした

### Changed

- pipeline health 指標とは別に learning outcome 指標を集計し、doctor で分離して見えるようにした

## [0.14.0] - 2026-03-26

### Added

- `kb-mcp judge promote-scopes` を追加し、active project-local learning asset を wider scope へ deterministic に昇格できるようにした
- `scope_promotion` を追加し、`project_local -> user_global` / `project_local -> general` の narrow-first rule を first-class にした

### Changed

- resolver は `distribution_allowed` / `secrecy_boundary` metadata を見て wide scope asset の配布可否を強制するようにした

## [0.13.0] - 2026-03-26

### Added

- `kb-mcp judge build-policy-snapshots` を追加し、active learning asset から user/project policy snapshot を runtime 配下へ生成できるようにした
- `policy_projection` / `policy_snapshot` を追加し、project-local と user-global の governed learning policy を deterministic に投影できるようにした

### Changed

- `kb_graduate` は runtime policy snapshot がある場合にそれを参照する read-only surface として振る舞うようにした

## [0.12.0] - 2026-03-26

### Added

- `kb-mcp judge retract-learning` / `supersede-learning` / `expire-learning` を追加し、runtime learning asset の revoke 系操作を CLI から実行できるようにした
- `learning_revocations` ledger と packet TTL / invalidation metadata を追加し、trace と revocation を永続的に結び付けられるようにした

### Changed

- learning packet に TTL を持たせ、asset の retract / supersede / expire 時に関連 packet を invalidated に落とすようにした
- `doctor` に invalidated packet と revocation 件数を追加し、runtime safety rail の観測性を改善した

## [0.11.1] - 2026-03-26

### Added

- `kb-mcp judge learning-state` を追加し、governed runtime learning asset の visibility と主要属性を CLI から確認できるようにした

### Changed

- `doctor` に learning asset / packet / application と visibility 別件数の表示を追加し、runtime learning state の観測性を改善した

## [0.11.0] - 2026-03-26

### Added

- learning packet / packet asset / application trace の runtime table を追加した
- middleware request path で applicable learning asset から packet を作り、tool apply trace を永続化するようにした

## [0.10.0] - 2026-03-26

### Added

- `kb_mcp.learning` package と `resolve_learning_assets()` を追加し、scope / confidence / force に基づく deterministic resolver を導入した

### Changed

- project resolver と middleware runtime context の bridge として、learning asset の applicable 判定を `session_local > client_local > project_local > user_global > general` で固定した

## [0.9.0] - 2026-03-26

### Added

- judge candidate payload に governed runtime learning contract の初期 semantics を含めるようにした

### Changed

- learning contract の default semantics を helper に集約し、judge candidate と schema backfill が同じ初期規則を使うようにした
- `session_thin` を `session_local` / `session_summary_only` として扱う runtime semantics を固定した

## [0.8.0] - 2026-03-26

### Added

- `learning_assets` canonical table と store API を追加し、runtime learning contract の最小 10 項目を永続化できるようにした

### Changed

- event DB schema version を `5` に上げ、既存の accepted / relabeled / materialized candidate から canonical learning asset を idempotent に backfill するようにした
- schema migration 後も既存の judge / review / materialize フローが継続動作するように回帰テストを拡張した

## [0.7.0] - 2026-03-25

### Added

- `kb-mcp judge materialize` と `kb-mcp judge retry-failed-materializations` を追加し、review 済み candidate の note materialize と repair retry を CLI から実行できるようにした
- `kb-mcp hook dispatch --judge-fastpath` と fast-path backend contract を追加し、`KB_JUDGE_FASTPATH_COMMAND` 設定時だけ hook 同期 judge を試行できるようにした
- `doctor` に materialization runtime 指標と fast-path backend / breaker 指標を追加した

### Changed

- `review_materialization` の no-op / repair 判定を見直し、partial outbox loss や stale review_seq の recovery を安定化した
- bulk materialize と failed materialization retry が broken candidate / expired lease を巻き込まず継続できるようにした
- hook wrapper は fast-path backend 未設定時に inline judge を有効化しないようにした

## [0.6.1] - 2026-03-25

### Changed

- `doctor` の legacy path 表示で `not present ✓` / `present ✗` を明示し、legacy repo path 検出時を cleanup candidate として示すようにした

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
