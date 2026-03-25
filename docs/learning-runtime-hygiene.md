# Runtime Learning Hygiene

## 目的

learning contract 導入後の runtime artifact を、doctor と worker command で診断・修復できるようにする。

## Doctor で見る項目

- `Learning expired active packets`
- `Learning packet asset mismatches`
- `Learning orphan applications`
- `Learning legacy wide-scope fallbacks`
- `Learning packets using unknown-client fallback`
- `Learning stale session-local assets`
- `Learning stale client-local assets`

`legacy wide-scope fallbacks` は、`user_global` / `general` asset の `traceability` に明示 metadata が無く、互換 fallback に依存している件数を表す。

`unknown-client fallback` は異常ではなく、未知 client が安全側 capability で処理された件数を表す。

## Worker command

```bash
kb-mcp worker repair-learning-runtime
```

### 主な処理

1. TTL 切れ packet の invalidation
2. legacy wide-scope asset の `traceability` backfill
3. packet asset count mismatch の修復
4. orphan application の削除
5. stale `session_local` / `client_local` asset の expire と関連 packet invalidation

### オプション

```bash
kb-mcp worker repair-learning-runtime --session-local-days 1 --client-local-days 7
```

- `--session-local-days`
  - `session_local` asset を stale とみなす閾値
- `--client-local-days`
  - `client_local` asset を stale とみなす閾値

いずれも `0` 以上を受け付ける。
