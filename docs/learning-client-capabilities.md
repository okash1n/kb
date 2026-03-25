# Runtime Learning Client Capabilities

## 目的

cross-client consistency controls で導入した、クライアント別の governed runtime learning 配布制約を明文化する。

この文書の対象は runtime serving 時の `LearningAssetView` 解決と packet 生成であり、保存済み note 自体の distribution policy を置き換えるものではない。

## 基本原則

- `session_local` / `client_local` / `project_local` は narrow scope として扱う
- `user_global` / `general` は wide scope として扱う
- wide scope asset は `traceability.secrecy_boundary` を必須とする
- 未知の `source_client` は fail-close とし、local scope のみ許可する
- ただし既存 wide scope asset に `secrecy_boundary` が無い場合は、`memory_class` ごとの既定 metadata から互換 fallback を導出する

## クライアント別ルール

| client | allowed_scopes | allowed_secrecy_boundaries | force downgrade |
| --- | --- | --- | --- |
| `kb-mcp` | `session_local`, `client_local`, `project_local`, `user_global`, `general` | `project`, `user`, `general` | なし |
| `claude-code` | `session_local`, `client_local`, `project_local`, `user_global`, `general` | `project`, `user`, `general` | なし |
| `codex-cli` | `session_local`, `client_local`, `project_local`, `user_global`, `general` | `project`, `user`, `general` | なし |
| `copilot-cli` | `session_local`, `client_local`, `project_local`, `user_global` | `project`, `user` | `user_global/default -> preferred`, `user_global/preferred -> hint` |
| `copilot-vscode` | `session_local`, `client_local`, `project_local`, `user_global` | `project`, `user` | `user_global/default -> preferred`, `user_global/preferred -> hint` |
| unknown client | `session_local`, `client_local`, `project_local` | `project` | なし |

## 適用ルール

1. resolver は scope / provenance / lifecycle を見て candidate asset を選ぶ
2. wide scope asset は `distribution_allowed` と `secrecy_boundary` を満たした場合だけ通す
3. client capability table は allowed scope / secrecy boundary を追加で強制する
4. packet builder は client rule に従って force downgrade または omission を行う
5. omission により packet が空になった場合、packet は作らない
6. `copilot-*` / `claude-*` / `codex-*` の未知 variant は family alias で既知 capability へ解決する

## 互換性方針

- 既知 client の挙動差は capability table で明示する
- 新しい client を導入するときは、capability row と parity test を同時に追加する
- wide scope asset を導入する code path は、`traceability.secrecy_boundary` を必ず付与する
