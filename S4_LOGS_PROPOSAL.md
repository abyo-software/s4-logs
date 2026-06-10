# 企画書: S4 Logs — CloudWatch Logs Offloader

**版**: v1.1 (2026-06-10、`../s4` リポジトリ実機照合済み)
**事業主体**: abyo software 合同会社
**位置づけ**: S4 ファミリー第2製品。OSS コア + 商用 AMI (AWS Marketplace)
**実装前提**: Claude Code 並行運用 (3–4 agent)。実測ベースライン 12h セッション = 281 commits / ~56K LOC

> v1.0 からの主な変更 (s4 実機照合の結果):
> 1. 「S4 Logs が消えても Athena + zstd で読める」は S4F2 フレームのままでは**成立しない**ため、出力形式を設計決裁事項として明示 (§4.3)
> 2. `s4logs restore` の CW 書き戻しは PutLogEvents の **14 日制約**により原タイムスタンプでは不可能。製品仕様を再定義 (§4.5, §10)
> 3. Mode B を「CloudWatch Logs API 互換エンドポイント」として明確化 — S4 の「S3 API 互換」と同型の差し替え体験 (§1)
> 4. s4 実測値 (nginx log 155×, 3.7 GB/s) をコスト試算の根拠として転記 (§1.1)
> 5. CreateExportTask の同時 1 タスク制約・API quota を Drain 設計に反映 (§5)

---

## 1. 製品コンセプト

**「CloudWatch Logs の請求を、アプリを変えずに 70–90% 削る」**

CloudWatch Logs (CW) のコスト構造は ingest $0.50/GB + 保管 $0.03/GB/月。ログ量が増えるほど線形に痛む一方、大半のログは「書いた後ほぼ読まれない」。S4 Logs は CW 上のログを zstd 圧縮して S3 に退避し、CW 側の保持期間を短縮することでコストを削減する。退避後も grep ライクな検索が可能で、「捨てる」のではなく「安い場所に動かす」。

S4 本体が「S3 の請求書」に刺さるのと同型の、「CloudWatch の請求書」プロダクト。s4-codec / S3 書き込み層 / observability スタックを最大限流用する。

**圧縮の実測根拠 (s4 README 公表値、2026-05-13 計測)**: nginx ログ 256 MiB → cpu-zstd-3 で **155×** (3.7 GB/s)。1 TiB のログが ~6.6 GiB に縮む。ログはテキスト反復が多く、zstd と最も相性の良いワークロードであり、S4 ファミリーの中で最も「効く」データ種。

### 動作モード (両対応必須)

- **Mode A: Drain (退避)** — 既存 LogGroup から過去ログを圧縮 S3 へ移動し、CW retention を短縮 (例: 90日→7日)。保管コスト削減。導入が最も簡単で、これが OSS の入口。
- **Mode B: Bypass (迂回)** — **PutLogEvents 互換 API を喋るエンドポイント**を提供し、Fluent Bit / CW Agent / 各言語 SDK は endpoint 差し替えだけで移行できる (S4 の「S3 API 互換」と同じ顧客体験)。ルーティング規則により重要ストリームのみ CW へ passthrough、残りは直接圧縮 S3 へ。**ingest $0.50/GB 自体を回避**するため削減幅が最大。商用版の主戦場。

### 1.1 コスト削減の構造 (per-GB 経済性)

| | CW のまま | Mode A (退避後) | Mode B (迂回) |
|---|---|---|---|
| ingest | $0.50/GB | $0.50/GB (支払済み、削減対象外) | **$0 (回避)** + S3 PUT 微小 |
| 保管 | $0.03/GB·月 × retention | S3 $0.023/GB·月 ÷ 圧縮率 10–155× ≈ **$0.0002–0.002/GB·月** | 同左 |
| 削減率 | — | 保管分の ~95% | **請求全体の ~90%+** (ingest が支配項のため) |

Mode A 単体は「保管が長い・量が多い」組織に効き、Mode B は ingest 比率が高い組織 (= ほぼ全員) に効く。マーケティングの「70–90%」は Mode B 前提の保守値で、根拠数値を README に明記する。

> **訂正 (2026-06-10, 実 AWS 統制実験で発覚)**: 上表の Mode A 保管削減 ~95% は誤り。
> **CW の保管課金は gzip level 6 圧縮後バイトに対して掛かる** (AWS 料金表脚注) ため、
> 正しい比較は CW-gzip ($0.006–0.010/GB-raw·月) vs S3-zstd ($0.002–0.004/GB-raw·月)
> = **保管行の削減は ~50–70%**。Mode A の本命は retention 短縮 (drain 済み範囲の CW
> 行を丸ごと消す) であり、Mode B の ingest 回避 (raw バイト課金、~90%+) は影響なし。
> README とドレイン推定ロジックは訂正済み。

## 2. 市場・競合

| 競合 | 性質 | S4 Logs の差別化 |
|---|---|---|
| CW Logs Infrequent Access class | AWS 純正、ingest $0.25/GB | 半額止まり。S3+zstd は 1 桁安い。IA は Live Tail / メトリクスフィルタ / サブスクリプションフィルタ / S3 export 等が制限される |
| CreateExportTask + 手組み | 無料だが運用が雑用化 | アカウント同時 1 タスク制約で大規模退避は実質回らない。自動化・圧縮・検索をパッケージ化 |
| サブスクリプションフィルタ + Firehose → S3 | AWS ネイティブのアーカイブ経路 | CW に ingest **した後**の複製なので $0.50/GB は払ったまま。Mode B は ingest 前に分岐する |
| Vector / Fluent Bit 単体 | ルーティングは出来るが「コスト製品」ではない | 退避+索引+検索+コスト可視化までの一気通貫 |
| Cribl | 商用テレメトリパイプライン、高価・多機能 | 単機能・低価格・セルフホスト。中小〜中堅の「Cribl は大げさ」層 |

ポジショニング: テレメトリパイプラインではなく**コスト特化アーカイバ**。機能を増やさないことが価値。

## 3. OSS / 商用の分割 (確定方針)

| 機能 | OSS (Apache-2.0) | 商用 AMI |
|---|---|---|
| Mode A Drain (単一アカウント) | ✅ | ✅ |
| Mode B Bypass エンドポイント | ✅ (単一アカウント) | ✅ |
| zstd 圧縮 + フレーム形式 + 索引 sidecar | ✅ | ✅ |
| CLI 検索 (`s4logs grep`) / 展開 (`s4logs restore`) | ✅ | ✅ |
| Prometheus / OTel / 構造化ログ | ✅ | ✅ |
| **AWS Organizations マルチアカウント横断** | ❌ | ✅ |
| **保持ポリシー / コンプライアンス設定 (WORM, Object Lock 連携)** | ❌ | ✅ |
| **コスト削減ダッシュボード (削減額の可視化 Web UI)** | ❌ | ✅ |
| **検索 Web UI** | ❌ | ✅ |
| ワンクリックデプロイ / CloudFormation 同梱 | ❌ | ✅ |

原則: 個人と 1 アカウント運用は OSS で完結させて拡散に使う。企業が金を払う理由 (複数アカウント・監査・UI・ワンクリック) を商用側に置く。**境界はビルドフラグではなくクレートで分離**し、商用クレートは private repo に置く (S4 ファミリー共通方針。公開 s4 repo は全体 Apache-2.0 を維持しており、S4 Logs も同じく OSS 側に商用コードを混ぜない)。

## 4. アーキテクチャ

```
[Mode A]
CW Logs ──FilterLogEvents (quota 内並列)──▶ s4logs-drain ──zstd──▶ S3 (frames + .s4index)
                                               │
                                               └─▶ retention 短縮 (PutRetentionPolicy)
                                                   ※ S3 書き込み + CRC 検証成功の後のみ

[Mode B]
Fluent Bit / CW Agent / SDK ──PutLogEvents 互換 API──▶ s4logs-gateway ──┬─ 重要 stream → CW Logs (passthrough)
                                                                         └─ それ以外 → zstd → S3
```

### 4.1 流用資産 (必ず再利用、再発明禁止 — s4 実機照合済み)

- `s4-codec` (`crates/s4-codec`): `CpuZstd` streaming (spawn_blocking、decompression bomb は `take(original_size + 1024)` で cap 済み、proptest 検証済み)、S4F2 フレーム形式、S4IX `.s4index` sidecar (32 bytes/frame、範囲読みで検索を成立させる中核)
- S4 の S3 書き込み層: multipart、CRC32C (crc32c 0.6)、sidecar の write-after-commit パターン
- observability 一式: `/health` `/ready` `/metrics` (metrics-exporter-prometheus)、`--log-format json`、OTLP traces (opentelemetry 0.31)
- TLS スタック (tokio-rustls 0.26 + ring、rustls-acme による自動証明書)、CI/fuzz テンプレート (proptest push 時 10K / nightly 1M、bolero 0.13 libfuzzer 5 target、nightly 失敗時 Issue 自動起票)

GPU コーデックは**初期スコープ外** (ログは CPU zstd で十分 — 実測 155× / 3.7 GB/s。GPU はクロスセルで S4 本体に誘導)。

### 4.2 S3 レイアウト

```
s3://bucket/s4logs/data/account=123456789012/loggroup=<sanitized>/dt=2026-06-10/
  └── 000001.jsonl.zst          # 標準 zstd マルチフレーム (推奨案、§4.3)
s3://bucket/s4logs/index/account=123456789012/loggroup=<sanitized>/dt=2026-06-10/
  └── 000001.jsonl.zst.s4index  # S4IX sidecar
```

- 中身は JSONL (timestamp, stream, message, 元の CW メタデータ保持)。LogGroup 名は `/` を含むため key への sanitize 規則を仕様化
- **index は data と別 prefix に置く** (v1.0 から変更)。Athena / Spark はパーティション配下の全ファイルを読むため、data と同居させると sidecar がクエリを壊す
- Hive パーティション形式により外部クエリエンジンから直接読める (§4.3 の形式選択に依存)

### 4.3 出力フレーム形式 — 設計決裁事項 ⚠

v1.0 は「Athena + zstd で読める」と「S4F2 をそのまま使う」を同時に掲げていたが、**この 2 つは両立しない**。S4F2 は 28-byte 独自ヘッダ (magic + codec_id + sizes + CRC32C) でペイロードを包むため、Athena / zstd CLI は直読できない。選択肢:

| | 案 1: 標準 zstd マルチフレーム (推奨) | 案 2: S4F2 維持 |
|---|---|---|
| 形式 | RFC 8878 zstd フレームの連結 (`zstd -d` でそのまま展開可能)、content checksum 有効化 | S4 と同一の S4F2 |
| Athena/Spark 直読 | ✅ | ❌ (s4-codec での変換 or 専用 SerDe が必要) |
| 範囲読み grep | ✅ S4IX sidecar はフレーム offset 表なので標準 zstd フレームでも機能する | ✅ |
| 完全性検証 | zstd content checksum (XXH64) | CRC32C in-band |
| S4 本体との互換 | sidecar (S4IX) 互換は維持。フレーム container のみ異なる | フル互換 |

**推奨は案 1**。ログ製品の lock-in 回避は「クエリエンジンで直接読める」が最強の形であり、S4 本体より一段強い story になる (S4 は「s4-codec ~1k LOC decoder で読める」止まり)。失うのは S4F2 の per-frame codec_id (ログは zstd 固定なので不要) と CRC32C (zstd checksum で代替)。**ただし v1.0 申し送りの「S4F2 固定」と矛盾するため、P1 着手前にどちらかを決裁すること。** 案 2 を選ぶ場合は README の lock-in 文言を「s4-codec (Apache-2.0, CLI/pip/WASM) で gateway なしに復号可能」に弱める。

### 4.4 検索

- `s4logs grep <pattern> --loggroup X --from --to`: sidecar で対象フレームのみ S3 Range GET し復号 (全量ダウンロード禁止)。時刻範囲 → フレームの絞り込みのため、sidecar に各フレームの min/max timestamp を載せる拡張を入れる (S4IX の将来 version として s4 側と調整)

### 4.5 復元 (`s4logs restore`) — v1.0 から仕様変更 ⚠

PutLogEvents は **14 日より古いイベント (または retention 超過分) を拒否する**ため、「90 日前のログを元のタイムスタンプで CW に書き戻す」は AWS API 上不可能。よって:

- **第一義の復元先はローカル / S3**: `s4logs restore --to-file / --to-stdout` で生 JSONL に展開。調査用途はこれと `s4logs grep` で 9 割賄える
- CW への書き戻し (`--to-loggroup`) は提供するが、**ingest 時刻 = 現在時刻、原タイムスタンプは JSON フィールドとして保持**する仕様と明記。Logs Insights では `original_timestamp` でクエリできる
- この制約は README の FAQ に正直に書く (隠すと「復元できると思ったのに」批判で信用を失う)

## 5. 技術仕様サマリ

- Rust / tokio / aws-sdk-rust。ワークスペースは `s4logs-core` / `s4logs-drain` / `s4logs-gateway` / `s4logs-cli`
- **Drain は FilterLogEvents ベース** (CreateExportTask はアカウント同時 1 タスク制約 + 変換不可のため不採用)。アカウント単位 API quota が律速のため、quota 消費を adaptive に制御し、必要なら Service Quotas 引き上げを README で案内
- IAM: 最小権限ポリシー JSON を同梱 (logs:FilterLogEvents, logs:PutRetentionPolicy, logs:PutLogEvents, s3:PutObject 等を明示)
- 設定: CLI flags + TOML。S4 と同じ流儀 (`--log-format json` 等)
- 失敗時安全: **削除より退避が常に先**。S3 書き込み + checksum 検証成功を確認してから retention 変更。Drain は冪等 (同一範囲の再実行で重複オブジェクトを作らない)
- テスト規律 (S4 準拠 + 追加): unit + proptest (push 時 10K cases / nightly 1M) + bolero fuzz。E2E は S3 側を s4 の MinIO testcontainers パターンで、CW API 側を LocalStack または moto で。**24h soak は s4 の既存 CI にない新規ワークフロー**として作り、Marketplace 出品前要件とする

## 6. マイルストーン (実測ベースライン基準)

| Phase | 内容 | 見積り |
|---|---|---|
| **P1: OSS コア** | drain + gateway + CLI 検索/復元 + テスト一式 + README (ベンチ・コスト試算表込み)。**API コスト実測と形式決裁 (§4.3) を含む** | **2–3日** (s4 流用前提、agent 並行) |
| **P2: OSS 公開 + GTM** | GitHub 公開、HN「Show HN」+ r/aws 同時投稿、This Week in Rust 推薦 | 0.5日 |
| **P3: 商用層** | Organizations 横断、ダッシュボード UI、保持ポリシー、CloudFormation | 3–4日 |
| **P4: AMI + Marketplace** | AMI ビルド、24h soak、セキュリティスキャン、出品審査提出 | 2日 + 審査待ち |

P1–P4 で実働 8–10 日。S4 本体の出品作業とは repo 独立のため並行可。分割・延長は依存循環/テスト不可能/push 上限が出た場合のみ検討。

## 7. 価格・課金

- OSS: 無料 (Apache-2.0)
- 商用 AMI: **$0.10–0.15/hr** (t3.medium 級で動く軽量さを訴求。S4 本体と違い GPU 不要なので「月 $75–110 で CW 請求が月数千ドル減る」の構図が GPU 月額 $725 の S4 より圧倒的に通しやすい) + 年間契約 (Private Offer) $900–1,200/年
- README / 出品ページに S4 同様の正直なコスト損益分岐表を必ず置く (「月 CW 請求 $500 未満なら OSS 版で十分」と書く。この正直さが S4 で信用を稼いだ)

## 8. GTM (ローンチ計画)

1. README は S4 品質を踏襲: 見出しに削減率の実測値、再現可能なベンチ、損益分岐、lock-in 回避の明記 (§4.3 で案 1 を採れば「Athena で直接読める」を見出しに使える)
2. **HN を主砲にする** (r/aws はスター転換しない実証済み。r/aws は見込み客向け、HN は開発者信用向けと役割分担)
3. ローンチ用の実測データ: 自社 AWS アカウントの CW 請求 before/after をスクショ付きで
4. S4 README / 出品ページと相互リンク (ファミリー化でセラーページを「ストレージ/ログコスト最適化専門」の顔にする)
5. 商用版リリース時に OSS ユーザーへ GitHub Release notes で告知

## 9. 成功指標と撤退条件

- **3ヶ月**: OSS スター 300+ / Marketplace 初回課金 3 アカウント → 継続
- **6ヶ月**: MRR $500+ → P5 (検索 UI 強化等) に投資
- 6ヶ月で課金 0 かつ OSS 反応も鈍い場合: メンテナンスモードに降格し、工数を SIEM ジェネレータへ回す

## 10. リスク

- **Sherlocking**: AWS が CW→S3 自動退避をネイティブ化する可能性は常時ある。対策は速度と、IA class 登場後も残った「S3 直置きの安さ」という構造的ギャップに張ること
- **Drain 自体の API コスト / quota**: FilterLogEvents の課金とアカウント quota が削減額・退避速度を食わないか、P1 でコスト実測を必須タスク化。大規模 LogGroup (TB 級) の初回退避所要時間も同時に実測し README に載せる
- **CW 復元の 14 日制約**: PutLogEvents は 14 日超の過去イベントを拒否 (§4.5)。「完全に元に戻せる」とは謳わない。grep + ローカル展開を主、CW 書き戻しは現在時刻 ingest と明記
- **データ喪失批判**: 「退避が先、削除は検証後」の設計原則と checksum 検証を README で前面に
- **商標**: "CloudWatch" を製品名に含めない。説明文での言及は nominative use の範囲 (s4 の NOTICE にある Amazon disclaimer と同形式の一文を NOTICE に置く)

---

## Claude Code への申し送り

- s4 リポジトリの `crates/s4-codec` (特に `cpu_zstd.rs` / `multipart.rs` / `index.rs`) と observability / CI 構成を最初に読み、流用可能部分の写経から始めること
- **着手前に §4.3 の形式決裁 (標準 zstd vs S4F2) をユーザーに確認すること**。推奨は標準 zstd マルチフレーム + S4IX sidecar (S4IX 形式自体は変更せず、S4 本体との索引互換を保つ)
- P1 完了の定義: `docker compose up` + MinIO/LocalStack で Mode A/B が E2E グリーン、README のコスト試算表が実測値で埋まっている状態
- `s4logs restore --to-loggroup` の 14 日制約ハンドリング (現在時刻 ingest + `original_timestamp` フィールド) をテストケースに含めること
