# S4 Logs — DESIGN (P1 contract)

この文書は wave 並行実装の **契約**。ここに書かれた on-disk format / API 境界 /
クレート境界は orchestrator の承認なしに変更しない。背景は `S4_LOGS_PROPOSAL.md`。

## 1. クレート境界 (DO NOT cross)

| crate | 責務 | 依存 |
|---|---|---|
| `s4logs-core` | record schema / chunk encode・decode / S4LT sidecar / S3 layout / ObjectStore / ChunkSink | s4-codec (S4IX のみ。S4F2/multipart は使わない) |
| `s4logs-drain` | FilterLogEvents ページング、window 退避、manifest 冪等性、retention gate | core |
| `s4logs-gateway` | PutLogEvents 互換 HTTP API、routing、buffer/flush、CW passthrough、observability | core |
| `s4logs-cli` | clap binary: `drain` / `grep` / `restore` / `serve` | core + drain + gateway |

各 wave agent は自分の crate ディレクトリ以外を編集しない。root ファイル
(Cargo.toml / DESIGN.md / README) の変更が必要になったら最終レポートで要求する。
git 操作 (add/commit/push) は agent 側では一切行わない。

## 2. Record schema (on-disk JSONL — Athena 列名)

データオブジェクトの中身は JSONL。1 行 = 1 CW log event:

```json
{"timestamp":1717900000123,"stream":"app/i-0abc","message":"...","ingestion_time":1717900001000,"event_id":"3713..."}
```

- `timestamp` / `ingestion_time`: epoch **milliseconds** (i64)。
- `ingestion_time`, `event_id` は optional (省略時はフィールドごと出さない)。
- 実装は `s4logs_core::record::LogRecord`。フィールド名は format の一部。

## 3. S3 layout

```
{prefix}data/account={acct}/loggroup={g}/dt={YYYY-MM-DD}/{name}.jsonl.zst
{prefix}index/account={acct}/loggroup={g}/dt={YYYY-MM-DD}/{name}.jsonl.zst.s4index
{prefix}index/account={acct}/loggroup={g}/dt={YYYY-MM-DD}/{name}.jsonl.zst.s4lts
{prefix}manifest/account={acct}/loggroup={g}/window={start_ms}-{end_ms}.json
```

- `prefix` は "" か "...../"。`data/` と `index/` を分けるのは Athena/Spark が
  パーティション配下の全ファイルを読むため (sidecar 同居はクエリを壊す)。
- `{g}` = `sanitize_log_group()`: `[A-Za-z0-9_.-]` 以外の byte を `%XX`
  (大文字 hex) に percent-encode。可逆。`/aws/lambda/foo` → `%2Faws%2Flambda%2Ffoo`。
- `dt` はイベント `timestamp` (UTC) 由来。chunk は dt を跨がない (drain は
  window を日で切る、gateway は flush 時に日毎へ分配)。
- Drain の `{name}` = `{window_start_ms}-{seq:06}` (決定的 = 冪等性の基盤)。
  Gateway の `{name}` = `{first_event_ts_ms}-{uuid8}`。

## 4. Frame container — 標準 zstd マルチフレーム (決裁済み: 案1)

- データオブジェクト = **RFC 8878 standard zstd frame の連結**。`zstd -dc` /
  Athena (Hive zstd codec) でそのまま読める。S4F2 は使わない。
- 1 frame = 非圧縮 ~4 MiB (`ChunkConfig::frame_target_bytes`、レコード境界で切る)。
- zstd level 3、**content checksum (XXH64) 必須** (`include_checksum(true)`)。
- 読み出し側は decompression bomb 対策として sidecar の `original_size + 1024`
  bytes で出力を cap する (s4 の `cpu_zstd.rs` と同じ規律)。

## 5. Sidecars

### S4IX (`.s4index`) — s4-codec から無改変で流用

`s4_codec::index::{FrameIndex, FrameIndexEntry, encode_index, decode_index}`。
`compressed_offset/size` は各 zstd frame の byte 範囲、`original_offset/size` は
非圧縮 JSONL の byte 範囲。`source_etag` / `source_compressed_size` は PUT 後に
store が stamp してから encode (s4-server と同じ流儀)。`sse_v3` は常に None。

### S4LT (`.s4lts`) — 本プロジェクト新設 v1

```
magic "S4LT" (4) | version u32 LE = 1 | frame_count u64 LE
per frame: min_ts i64 LE | max_ts i64 LE        (16 B/frame)
```

- エントリは同一オブジェクトの S4IX entry 表と 1:1 同順。count 不一致は読み手が
  typed error で拒否。frame_count は S4IX と同じ 16M cap。
- 時刻範囲 grep: S4LT で frame を絞り → S4IX で byte 範囲に変換 → S3 Range GET。
- 実装は `s4logs_core::tsindex`。

## 6. 書き込み順序と検証 (退避が先、削除は検証後)

1. data object PUT (`ChecksumAlgorithm::CRC32C` 指定、SDK が end-to-end 検証)
2. `.s4index` PUT → `.s4lts` PUT (data 成功後のみ — write-after-data)
3. (drain のみ) window 完了時に manifest PUT
4. retention 変更 (`PutRetentionPolicy`) は「短縮後 retention より古い全 window に
   manifest が存在する」ことを確認できた場合のみ。確認できなければ **何もしない**
   (fail-closed)。`--apply-retention` opt-in flag、default は提案のみ表示。

## 7. Drain (`s4logs-drain`)

- 単位 = (log_group, window)。window は UTC 1h アライン (`--window 1h`、日境界も切る)。
- `FilterLogEvents` (stream 横断、interleaved) を nextToken でページング。
  ThrottlingException は exponential backoff + jitter。並列度は
  `--concurrency` (default 2) で window 並列、quota は適応的に譲る。
- 冪等性: window 処理前に manifest の存在を確認、あれば skip。再実行で同名
  object (`{window_start_ms}-{seq:06}`) を上書きしても内容は決定的なので安全。
- manifest JSON (version=1): account, log_group (raw), window_start_ms,
  window_end_ms, objects[{data_key, etag, crc32c, body_len, record_count,
  min_ts, max_ts, raw_bytes?}], record_count, completed_at_ms, drain_version。
  `raw_bytes` (非圧縮 byte 数) は wave 3G 追記の optional field — 旧 manifest は
  None として読め、`s4logs report` は欠損分を "[lower bound]" 表示で扱う。
- `--dry-run`: API 読み取りのみで書き込みゼロ、削減見積りを表示。
- CW client は trait (`CwSource`) で抽象化し、unit test は mock。

## 8. Gateway (`s4logs-gateway`)

### 8.1 API subset — AWS JSON 1.1, `X-Amz-Target: Logs_20140328.<Action>`

| Action | 挙動 |
|---|---|
| `PutLogEvents` | routing 評価 → buffer (s3) / passthrough (cloudwatch) / 両方 (both) / drop。response `{}` (sequence token は現行 CW 同様不要) |
| `CreateLogGroup` / `CreateLogStream` | gateway 内 registry に記録、`both`/`cloudwatch` 対象なら CW へも転送。重複は `ResourceAlreadyExistsException` |
| `DescribeLogGroups` / `DescribeLogStreams` | registry から最小応答 (agent 互換のため) |

- エラー形式: HTTP 400 + `{"__type":"<ExceptionName>","message":"..."}`。
- 未対応 Action は `UnrecognizedClientException` ではなく
  `InvalidAction` 系 400 を返し、access log に記録。
- SigV4 署名は **検証しない** (P1)。README に明記し、TLS + network 境界で守る
  前提。P3 で static credential 検証を追加予定。
- PutLogEvents 制約の再現: batch ≤ 10,000 events / ≤ 1 MiB、event ≤ 256 KiB は
  受理して通す (検証は緩め、拒否はしない方針 — 互換性優先)。

### 8.2 Routing (TOML, first-match-wins)

```toml
default_action = "s3"        # s3 | cloudwatch | both | drop

[[rule]]
log_group = "/aws/lambda/payments-*"   # globset glob
stream = "*"                            # optional, default "*"
action = "cloudwatch"
```

### 8.3 Buffer / flush

- key = log_group。`ChunkWriter` に直接 push。flush 条件: 非圧縮 ≥ 8 MiB
  (`--flush-bytes`) / 最古イベント到着から 60s (`--flush-interval`) / graceful
  shutdown (SIGTERM で全 buffer flush 完了後に exit)。
- 耐久性の正直な注記: P1 はプロセス内 buffer。クラッシュで直近 flush 未満を
  失い得る。README の Limitations に明記 (WAL は roadmap)。

### 8.4 Observability

`/health` (無条件 200) / `/ready` (S3 ListObjectsV2 1 回成功で 200) /
`/metrics` (Prometheus)。counter: `s4logs_events_total{action=}`,
`s4logs_flush_total`, `s4logs_flush_bytes_total{kind=raw|compressed}`,
`s4logs_cw_passthrough_errors_total`。tracing は `--log-format json|pretty`。

## 9. Restore / grep (`s4logs-cli`)

- `s4logs grep <regex> --log-group X --from <rfc3339|ms> --to ... [--account]`:
  manifest/dt で object 絞り → S4LT で frame 絞り → S4IX → Range GET → 復号 →
  regex 照合 → 行出力 (`--output jsonl|text`)。全量ダウンロード禁止。
- `s4logs restore --log-group X --from --to (--to-stdout | --to-file F | --to-log-group Y)`:
  - stdout/file: 生 JSONL。
  - `--to-log-group`: PutLogEvents は **14 日より古い timestamp を拒否**するため、
    event timestamp = now で ingest し、message を
    `{"original_timestamp":...,"original_stream":"...","message":"..."}` に wrap
    (`--raw` で wrap 無効 = 14 日以内のイベントのみ通る)。batch は
    ≤10,000 events / ≤1,048,576 bytes (event = len(message)+26) を厳守、
    ThrottlingException backoff。
- `s4logs serve`: gateway 起動。`s4logs drain`: drain 実行。

## 10. テスト規律

- 全 crate: unit + 主要 format/境界に proptest (roundtrip: records → chunk →
  body+sidecars → decode → records)。`#[allow(clippy::unwrap_used)]` は
  test module 単位で明示。
- E2E (wave 2E): LocalStack (S3 + CloudWatch Logs) を docker compose で。
  Mode A: CW へ投入 → drain → S3 検証 → grep。Mode B: gateway へ
  aws-sdk-cloudwatchlogs (endpoint override) で PutLogEvents → flush → S3 検証。
- 即値 (`Date.now` 系) を format に埋めない。manifest の completed_at_ms のみ
  wall clock 可。

## 11. Wave 3 amendments (製品化 — 2026-06-10 追記)

### 11.1 Gateway WAL (opt-in, `--wal-dir DIR`)

- buffer key (log_group, dt) ごとに append-only segment file: 1 行 = 1 受理イベントの
  JSONL `{"log_group":...,"timestamp":...,"stream":...,"message":...,"ingestion_time":...}`。
- PutLogEvents 応答を返す**前**に WAL append (group commit / 数 ms バッチの fsync は可、
  trade-off をコードに明記)。flush 成功でその segment を削除。
- 起動時 replay: 残存 segment を buffer に積み直してから listen 開始。壊れた末尾行
  (torn write) は warn して skip。WAL 無効時は従来挙動 (README の Limitations は維持)。

### 11.2 Gateway 認証 (opt-in, `--auth-mode none|sigv4`)

- sigv4: `--auth-access-key` / `--auth-secret` (または env) の static credential 1 組に
  対する受信 SigV4 検証 (Authorization header 分解 → 正規リクエスト再構築 → 署名比較、
  clock skew ±15 分、UNSIGNED-PAYLOAD と x-amz-content-sha256 両対応)。
- 失敗 → 403 `{"__type":"InvalidSignatureException"}`。/health /ready /metrics は検証外。
- default は none (P1 互換)。

### 11.3 grep / restore / report (CLI)

- grep: chunk 横断で **timestamp 昇順の k-way merge** 出力 (同 ts は stream, 入力順で安定)。
  per-dt prefix listing で LIST 範囲を絞る。
- restore --to-log-group: 全件メモリ sort をやめ、chunk を時刻順に stream しつつ
  batch を作る (上限メモリ一定)。
- 新コマンド `s4logs report --log-group X|--all`: manifest を集計し、退避済み
  records / raw / compressed / 推定月額削減 / window coverage を表示。

### 11.4 drain マルチ loggroup

- `--log-group` は glob (globset) を受け、`--all` で DescribeLogGroups 全列挙。
  group ごとに独立 DrainJob (失敗 group は skip + 集計報告、exit code 1)。

## 12. Wave 4 amendments (2026-06-10 追記)

- **Manifest schema (§7) v1 追加 optional fields**: `reconciled_at_ms: i64`
  (最後に追補が発生した reconcile の wall clock) と `reconciled_added: u64`
  (累計追補レコード数、`record_count` には算入済み)。serde skip-if-none、
  `raw_bytes` と同じ byte 互換規律。クリーンな reconcile は manifest を
  書き換えない。
- **Reconcile 追補オブジェクト命名**: `{window_start_ms}-r{attempt:02}{seq:04}`。
  base の `{seq:06}` (6 桁固定) と衝突しない。attempt は manifest 内の既存
  最大 +1 (1–99、超過は typed error → manifest 削除 + 再 drain を案内)。
- **決定性 (§7)**: `shard_streams > 1` では object 名は決定的のまま、
  **content は byte 決定的でなくなる** (shard ページの完了順 interleave。
  レコード集合は同一、S4LT は min/max なので順序不要)。manifest-skip の
  冪等性は不変。
- **CwSource trait**: `filter_log_events` に `streams: Option<&[String]>`
  (≤100 名、FilterLogEvents logStreamNames) を追加、`list_log_streams`
  (paginated DescribeLogStreams) を新設。
- **storage class**: data object のみ `--storage-class` を適用、sidecar /
  manifest は常に STANDARD (GIR の 128 KiB 最低課金 + hot path)。
- **`s4logs plan`**: read-only 診断 (DescribeLogGroups storedBytes = CW の
  gzip 課金実バイト + CW Metrics IncomingBytes)。bucket / account 不要。
