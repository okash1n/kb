# Governed Runtime Learning Contract

## 目的

kb が扱う `gap` / `knowledge` / `adr` を、単なる保存カテゴリではなく runtime で効く learning asset として統一的に扱うための最小契約を整理する。

この文書は設計書の全文置き換えではなく、実装 surface と運用コマンドを README から辿りやすくするための短い実装ガイドである。

## 契約の中核

learning asset は少なくとも次の 10 項目を持つ。

1. `memory_class`
2. `update_target`
3. `scope`
4. `force`
5. `confidence`
6. `lifecycle`
7. `provenance`
8. `traceability`
9. `revocation_path`
10. `learning_state_visibility`

この契約により、capture / review / materialize / serve / revoke のどこでも同じ object を基準にできる。

## Runtime semantics

| memory_class | 主な update_target | runtime で更新するもの |
| --- | --- | --- |
| `gap` | `behavior_style`, `confirmation_policy`, `execution_policy` | 振る舞い、確認粒度、実行方針 |
| `knowledge` | `fact_model`, `constraint_model`, `execution_policy` | 事実認識、制約認識、再調査抑制 |
| `adr` | `decision_policy` | 意思決定拘束、既定路線、再議論回避 |
| `session_thin` | `session_summary_only` | 薄い文脈補助 |

## Serving / apply / trace

1. checkpoint / middleware event から candidate を作る
2. review と materialize を経て `learning_assets` に canonical row を作る
3. resolver が scope / confidence / force / client capability を見て applicable asset を選ぶ
4. packet builder が client ごとの配布制約を反映した memory packet を作る
5. middleware が packet と application trace を記録する

## Governance

- wide scope (`user_global`, `general`) は `distribution_allowed` と `secrecy_boundary` を必須にする
- revoke は `retract`, `supersede`, `expire` の 3 系統で扱う
- runtime hygiene で stale local asset、expired packet、orphan application、legacy traceability fallback を補修する

## 主な CLI surface

### review / materialize

- `kb-mcp judge review-candidates`
- `kb-mcp judge accept`
- `kb-mcp judge reject`
- `kb-mcp judge relabel`
- `kb-mcp judge materialize`
- `kb-mcp judge retry-failed-materializations`

### learning state / governance

- `kb-mcp judge learning-state`
- `kb-mcp judge retract-learning`
- `kb-mcp judge supersede-learning`
- `kb-mcp judge expire-learning`
- `kb-mcp judge build-policy-snapshots`
- `kb-mcp judge promote-scopes`

### runtime hygiene

- `kb-mcp doctor`
- `kb-mcp worker repair-learning-runtime`
- `kb-mcp worker cleanup-runtime`

## 関連ドキュメント

- `docs/learning-client-capabilities.md`
- `docs/learning-runtime-hygiene.md`
- `00-review/goal_0.17.0.md`
