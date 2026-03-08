# Implementation Plan

## Replan (2026-03-08, Ingest Preprocess Pipeline)

- [x] `TagEngine` を前処理 API と forward API に分割し、前処理済み tensor を直接 forward できるようにする
- [x] ingest を download -> preprocess -> embed の3段パイプラインへ変更する
- [x] 前処理 worker 数とキュー深さを設定化し、README に反映する
- [x] 静的検証を実施し、レビュー欄に結果を記録する

### Review

- [x] 実装後に記入
- 実装:
  - `app/tag_engine.py` に `preprocess_image(s)` / `extract_feature_tensors(_with_stats)` を追加し、画像前処理と GPU forward を分離
  - `app/ingest_posts.py` に `PreparedPost` を追加し、前処理済み tensor とメタデータをまとめて保持するよう変更
  - `process_posts_with_stats()` を `download pool -> preprocess threads -> embed thread` の3段パイプラインへ変更
  - `build_rows_from_prepared_batch()` で前処理済み tensor を stack して forward し、前処理時間は各 worker 側の時間も含めて集計するよう調整
  - `config.json` / `app/config.py` / `README.md` に `ingest_preprocess_workers` と `ingest_preprocess_queue_factor` を追加
- 検証:
  - `python3 -m compileall app` 成功
  - `python3 -m py_compile app/ingest_posts.py app/tag_engine.py app/sync_posts.py app/config.py` 成功
- 未実施:
  - 実 GPU 環境での波形確認
  - 次ページ prefetch の追加

## Replan (2026-03-08, Ingest GPU Batch Throughput)

- [x] `TagEngine` にバッチ推論 API を追加し、単発推論の内部実装を共有化する
- [x] `sync_posts` ingest 経路を「ダウンロード並列 + GPUバッチ推論」に再編する
- [x] 無駄な前処理を削減し、埋め込み計測を preprocess / forward / transfer / batch size まで分解する
- [x] 設定と README を更新し、静的検証を実施してレビュー欄に記録する

### Review

- [x] 実装後に記入
- 実装:
  - `app/tag_engine.py` に `extract_image_features()` / `extract_image_features_with_stats()` を追加し、単画像 API も同じバッチ経路を通すよう整理
  - `app/ingest_posts.py` を「並列ダウンロード -> 推論キュー -> GPUバッチ埋め込み」に変更し、`ingest_embed_batch_size` または `ingest_embed_max_wait_ms` で flush するよう修正
  - `download_post_with_stats()` で RGB 変換済みの画像を保持し、`TagEngine._preprocess_image()` 側は非 RGB のときだけ変換するようにして二重 `convert("RGB")` を解消
  - `AdaptiveDownloadController` の「embed が重いと worker を減らす」分岐を除去し、GPU 律速時に供給を絞らないよう変更
  - `sync_posts.py` の perf ログに `pre/fwd/xfer/embed_batch_avg` を追加
  - `config.json` / `app/config.py` / `README.md` に ingest の推論バッチ設定を追加し、`ingest_batch_size` は parquet flush 用であることを明記
- 検証:
  - `python3 -m compileall app` 成功
- 未実施:
  - 実 GPU 環境での `sync_posts` ベンチ確認
  - WSL 側 `python3` では `httpx` が未導入のため import 実行確認は未完了

## Replan (2026-03-08, Ingest GPU Feed Smoothing)

- [x] ダウンロード完了回収と GPU バッチ推論を分離し、GPU 供給の間欠化を減らす
- [x] 推論ワーカで size/time ベース flush を維持しつつ、結果回収を並行化する
- [x] 静的検証を再実施し、レビュー欄に結果を記録する

### Review

- [x] 実装後に記入
- 実装:
  - `app/ingest_posts.py` の `process_posts_with_stats()` を producer/consumer 形へ変更
  - メインスレッドは `as_completed()` でダウンロード完了を回収し続け、成功した `DownloadedPost` を推論キューへ投入
  - 背景 `embed_worker` が `ingest_embed_batch_size` / `ingest_embed_max_wait_ms` に従ってバッチ flush し、結果を別キュー経由で返す形にした
  - これにより GPU バッチ実行中も次のダウンロード完了回収を止めず、供給の谷を減らす構造へ変更
- 検証:
  - `python3 -m compileall app` 成功
- 未実施:
  - 実 GPU 使用率波形の再確認

## Replan (2026-03-08, Download Autotune Stabilization)

- [x] download worker 自動調整を throughput ベースへ変更し、悪化時の増加を止める
- [x] ダウンロード遅化が続く場合は worker を戻すロジックを追加する
- [x] 静的検証を再実施し、レビュー欄に結果を記録する

### Review

- [x] 実装後に記入
- 実装:
  - `AdaptiveDownloadController` に前回ウィンドウの `download_avg` と `dl_img_s` を保持する状態を追加
  - block / 高失敗率で下げる既存制御は維持しつつ、通常時は「download_avg 悪化かつ dl_img_s 悪化なら worker 減」「両方改善した時だけ worker 増」に変更
  - autotune ログへ `dl_img_s` を追加し、増減判断の根拠を見えるようにした
- 検証:
  - `python3 -m compileall app` 成功
- 未実施:
  - 実運用ログでの worker 揺れ幅の再確認

## Replan (2026-03-08, Media Client Parallelism)

- [x] media download を共有 client から worker thread ごとの client へ分離する
- [x] page API と media download の HTTP 振る舞いを分け、media 側の並列性を上げる
- [x] 静的検証を再実施し、レビュー欄に結果を記録する

### Review

- [x] 実装後に記入
- 実装:
  - `process_posts_with_stats()` の download worker ごとに `make_media_client()` で専用 `httpx.Client` を作るよう変更
  - page API 取得は既存の `make_client()` を維持し、media download だけ `ingest_media_http2` 設定で HTTP/1.1 優先に切り替えられるよう分離
  - per-thread client は `ThreadPoolExecutor` の `initializer` で初期化し、ページ処理終了時に close するよう整理
  - `README.md` と `config.json` / `app/config.py` に `ingest_media_http2` を追加
- 検証:
  - `python3 -m compileall app` 成功
- 未実施:
  - 実 CDN スループット改善の確認

## Replan (2026-03-08, DB URL Recording Fix)

- [x] 現行 ingestion の URL 選定と DB 記録を分離し、記録側を `720x720` 実 URL 基準へ戻す
- [x] `post.file_ext` 優先の拡張子記録を廃止し、記録対象 URL の拡張子をそのまま保存する
- [x] 構文検証と実データ spot check を実施し、レビュー欄へ結果を記録する

### Review

- [x] 実装後に記入
- 原因:
  - 収集時のダウンロード URL 選定は `sample` 優先だった一方、DB 記録値 `c1..c5` は `post.file_ext` を優先して組み立てていた
  - このため `file_ext=png` でも実際の `720x720` variant が `webp` な投稿で、DB 復元 URL が誤っていた
- 実装:
  - `app/ingest_posts.py` でダウンロード用 URL と DB 記録用 URL を分離
  - DB 記録用は `media_asset.variants` から `720x720` variant の実 URL のみを採用
  - 拡張子は `post.file_ext` ではなく、記録対象 URL からそのまま符号化するよう変更
  - `720x720` variant がない投稿は `no_record_url` として記録対象から外す
- 検証:
  - `python3 -m compileall app` 成功
  - 実データ spot check で `10913348`, `10913347`, `10913346` の `720x720` variant がいずれも `webp` であることを確認
  - 同 spot check で、ダウンロード用 URL は `sample` のままでも、記録用 URL は `720x720` を選ぶべきケースを確認
- 未実施:
  - 依存不足のため、WSL 上で `app.ingest_posts` を直接 import した実行確認
  - 既存 DB の誤記録済み行に対する補正

## Replan (2026-03-08, Legacy Migration Batch Entry)

- [x] 旧 ILEMB cache を明示指定して移行できる専用 BAT を追加する
- [x] `database.bat` から自動移行を外し、継続収集専用の入口にする
- [x] README と運用手順を新しい入口構成に合わせて更新する
- [x] 構文とバッチ内容を確認し、レビュー欄へ結果を記録する

### Review (To be filled after implementation)

- [x] 実装後に記入
- 実装:
  - `migrate_legacy.bat` を追加し、ILEMB cache dir を明示指定して `app.migrate_legacy` を呼ぶ入口を追加
  - `database.bat` から自動 `migrate_legacy --if-needed` を削除し、`sync_posts -> build_index` の継続収集専用入口に変更
  - `README.md` を更新し、初回移行と継続収集の役割を分離して記載
- 検証:
  - `python3 -m compileall app` 成功
  - `migrate_legacy.bat` / `database.bat` / `README.md` の内容確認を実施
- 未実施:
  - Windows 実環境での `migrate_legacy.bat` 実行確認

## Replan (2026-03-08, Legacy ILEMB Compatibility)

- [x] 旧 ILEMB のインデックス構築方針と現行 `build_index.py` の差分を確認する
- [x] 現行構築処理を旧 ILEMB と同じ正規化・ID 空間へ合わせる
- [x] `migrate_legacy.py` の raw vector 変換を旧 `vectors_raw.npy` と互換な形に修正する
- [x] 構文検証を行い、互換性上の変更点をレビュー欄へ記録する

### Review (To be filled after implementation)

- [x] 実装後に記入
- 差分確認:
  - 旧 ILEMB は IVF-PQ 学習時も `index.add` 時もベクトルを正規化していなかった
  - 現行 `build_index.py` は train/add の両方で正規化しており、旧 index と混在すると検索順位が壊れる状態だった
- 実装:
  - `app/build_index.py` を旧方針に合わせ、train/add で未正規化ベクトルを使うよう修正
  - 新規構築時の既定 ID 空間を `post_id` ではなく `vec_idx` に変更
  - `vec_idx` 継続追加時は `index.ntotal` と `db.max_vec_idx()+1` の整合性を検証するよう追加
  - `app/migrate_legacy.py` で旧 `vectors_raw.npy` を `.f16` 生バイナリへチャンク変換して保存するよう修正
- 検証:
  - `python3 -m compileall app` 成功
- 未実施:
  - 実際の 400 万件 cache dir を使った移行実行
  - 移行後 index への追記と検索結果の実地確認

## Replan (2026-03-08, Image Search Failure + Startup DB Logging)

- [x] 画像検索APIの故障経路を特定し、最小修正方針を確定する
- [x] 起動時にDB/関連ファイルの診断ログを出す実装を追加する
- [x] 変更後の静的検証と動作確認可能な範囲の検証を行い、結果を記録する

### Review (To be filled after implementation)

- [x] 実装後に記入
- 原因:
  - `app/main.py` の `/API/search` で `Image.open(io.BytesIO(...))` を使っているのに、`io` と `PIL.Image` の import が欠けていた
  - このため、画像付き検索時だけ `NameError` で失敗する構造だった
- 実装:
  - `app/main.py` に `import io` と `from PIL import Image` を追加
  - 起動時 `lifespan` で DB 件数、ID 範囲、`vec_idx` 範囲、DB/FAISS/生ベクトル各ファイルの存在とサイズを `INFO` ログへ追加
  - `app/database.py` に起動ログ用の統計取得メソッドを追加
- 検証:
  - `python3 -m compileall app` 成功
- 未実施:
  - 実APIへの画像アップロード検証
  - WSL では Windows 側 `.venv` を使えないため、依存込みの実行確認は未実施

- [x] 要件整理: `docs/impl_request.md` と既存実装(PureILEMB/ILEMB)を確認
- [x] バックエンド骨格を実装 (FastAPI, /app, /API/search, /API/media, /API/tags)
- [x] 既存データ流用型のDB/インデックス構築機能を実装
- [x] PureILEMBフロントを`web_local/`へコピーし互換接続
- [x] `app.bat`/`database.bat`と設定ファイル・依存定義を整備
- [x] 検証実行 (静的コンパイル) とレビュー結果記録
- [x] 移行処理を専用スクリプトへ分離し、database実行フローを再設計

## Spec

- 目的: Danbooruビューアーをローカル起動し、既存ベクトル資産を流用して低メモリで検索可能にする。
- UI: `PureILEMB/web_local`をそのまま利用。
- サーバ: FastAPI (port 8002, `/app`配信)。
- 検索:
  - クエリなし: 新着ID順を返す。
  - クエリあり: `id:<post_id>` または数値IDを類似検索キーとして扱う。
- 画像表示:
  - `/API/media/{id}`でCDN画像をサーバ経由配信（CORS問題回避）。
- DB設計:
  - SQLiteに最小メタ情報のみ保持（`id`, `rating`, `url_c1..url_c5`, `vec_idx`）。
  - ベクトル本体はFaissインデックス + 必要時のみメモリマップ読み出し。
- 構築:
  - 既存キャッシュ (`search_ivfpq.index`, `metadata.npy`, `id_map.npy`, `vectors_raw.npy`) を優先流用。
  - 必要に応じてParquet群から新規構築。

## Review

- 実装結果:
  - `app/`にFastAPIバックエンドと検索サービスを新規実装
  - `app/build_index.py`で既存キャッシュ流用 + parquet再構築の2経路を実装
  - `web_local/`へPureILEMBフロントをコピーし、API互換パスで接続
  - `app.bat`/`database.bat`/`config.json`/`requirements.txt`/`README.md`を追加
  - `app/migrate_legacy.py` を追加し、既存データ移行を独立スクリプト化
  - `app/build_index.py` を「parquet増分同期専用」に再整理
- 検証:
  - `python3 -m compileall app` 成功
- 未実施:
  - 実行時検証（依存未インストール環境のため）

## Replan (2026-03-08)

### Problem Statement

- 現在の `database.bat` は「既存キャッシュ流用 + ローカル parquet 同期」しか行わず、方針にある「最新投稿の自動取り込み」と「未構築の古い投稿の穴埋め」が実行されない。
- `parquet_glob` に一致するファイルがないと同期が即終了し、次アクションがない。
- 取り込み進捗（どこまで最新追従したか / どこに欠損があるか）の状態管理がなく、継続運用不能。

### Spec (Database Build Policy Aligned)

- `database.bat` は毎回以下を自動実行する:
  - 既存資産移行（初回のみ）
  - 最新投稿の差分取り込み
  - 過去欠損レンジのバックフィル取り込み
  - ベクトル索引/DBへの反映
  - 実行結果サマリ出力（追加件数・欠損残数・失敗件数）
- メモリ方針:
  - 取得・埋め込み・書き込みはストリーミング/バッチ処理（大規模データを一括保持しない）
- 永続状態:
  - 「最後に追従済みの最新ID」「バックフィル対象レンジ」「失敗リトライキュー」を `data/build_state.json`（または専用テーブル）で管理

### Plan

- [ ] `app/sync_posts.py` を新規実装し、Danbooru API から最新差分を取得する（IDカーソル方式、レート制御、再開可能）
- [ ] `app/backfill_posts.py` を新規実装し、DB内IDギャップを検出して古い未構築投稿を段階的に補完する
- [ ] `app/embed_posts.py` を新規実装し、取得済み投稿の画像から埋め込みを生成して `faiss.index` / `vectors_raw.f16` / `images.db` に追記する
- [ ] `app/build_state.py` を新規実装し、latest cursor・backfill queue・retry queue を永続化する
- [ ] `database.bat` を再設計し、`migrate -> sync_latest -> backfill -> embed/index -> report` の順で必ず実行する
- [ ] `config.json` / `app/config.py` に運用パラメータ（batch size, API wait, retry, max backfill per run）を追加する
- [ ] `README.md` を更新し、初回構築と定期実行時の挙動を明記する
- [ ] 検証: 空データからの初回実行、2回目の差分実行、意図的欠損ID作成後のバックフィル実行を通しで確認する

### Review (To be filled after implementation)

- [ ] 実装後に記入

## Replan (2026-03-08, web_local D&D Regression)

### Problem Statement

- `web_local/index.html` で画像のドラッグ&ドロップが効かなくなった。
- 直近の検索バー予測変換修正で、サジェスト UI が入力外操作中も残る経路が増え、アップロード領域へのポインタ/ドロップを邪魔している可能性がある。
- 既存のタグ選択操作は壊さず、最小差分で D&D を復旧する必要がある。

### Plan

- [x] `web_local/index.html` のサジェスト表示制御と D&D イベントの干渉箇所を特定する
- [x] サジェスト UI を D&D の邪魔をしないよう最小修正し、アップロード挙動を復旧する
- [x] 差分確認と静的レビューを行い、結果を Review に記録する

### Review

- [x] 実装後に記入
- 原因:
  - 壊れていたのはアップロード受け側ではなく、一覧カードから外へドラッグする経路だった
  - 現行 `dragstart` は `image.url` を payload にしていたが、これは投稿ページ URL であり、従来の D&D が渡していた画像 URL ではなかった
  - 遅延読み込み対応後も、参照実装どおり `/API/media/{id}` を D&D payload に使うべきだった
- 実装:
  - 一覧カードの `dragstart` で渡す URL を `${API_BASE_URL}/API/media/${image.id}` に修正
  - 前回入れていた upload drop 側の誤修正は撤回し、D&D の修正対象をカード側だけに戻した
- 検証:
  - `node` で `web_local/index.html` 内の inline script を構文チェックし、`inline script syntax OK: 2` を確認
- 未実施:
  - ブラウザ実機でのカード D&D 手動確認

## Replan (2026-03-08, Interrupt Safety)

### Problem Statement

- 長時間実行中に `Ctrl+C` や強制終了が起きると、進捗状態の保存タイミングによっては大きく巻き戻る。
- `build_index` は途中で停止すると、`faiss.index` の永続化が末尾のみのため不整合リスクがある。

### Plan

- [x] `sync_posts.py` に中断ハンドリングを追加し、途中終了時でもカーソル/失敗キュー/未flush行を保存する
- [x] `backfill_posts.py` に同様の中断ハンドリングを追加する
- [x] `build_index.py` にチェックポイント保存（定期 `faiss.write_index`）を追加する
- [x] `build_index.py` でベクトル書き込みの `flush` を行い、DB commit前に耐障害性を上げる
- [x] 中断時ログ（どこまで保存されたか）を出して、再開時に確認可能にする
- [x] 構文検証を実行し、レビュー欄に結果を追記する

### Review

- `sync_posts.py` / `backfill_posts.py`:
  - `SIGINT`/`SIGTERM` 受信時に安全停止フラグへ遷移
  - 停止時に pending batch をparquetへflushし、`build_state` を保存して `exit 130`
- `build_index.py`:
  - `--checkpoint-every` を追加（デフォルト1バッチごと）
  - 各バッチで `vectors_raw` を `flush`（+ 可能なら `fsync`）
  - 中断時に最新 `faiss.index` を保存して `exit 130`
- 検証:
  - `python3 -m compileall app` 成功

## Replan (2026-03-08, Continuous Auto Collection)

### Problem Statement

- `database.bat` が単発実行で終了し、起動中の継続収集/継続インデクス化になっていない。
- 永続化の頻度と出力ファイル粒度が1000万件運用に不向き（小ファイル過多・IO過多）。
- 通信待ちとGPU処理待ちのバランスを取る自動調整が不足している。

### Plan

- [x] `sync_posts.py` / `backfill_posts.py` の state 永続化を「5分間隔」基準へ変更する
- [x] parquet出力をローリングwriter化し、1実行で少数の大きいファイルへ集約する
- [x] ダウンロード並列ワーカーを導入し、通信拒否率と処理時間から簡易自動調整する
- [x] `database.bat` を常駐ループ化し、実行中に `sync -> backfill -> build_index` を継続実行する
- [x] `config.json` / `app/config.py` / `README.md` を運用向け設定に更新する
- [x] 構文検証を実施し、レビュー欄に結果を追記する

### Review

- `sync_posts.py` / `backfill_posts.py`:
  - state保存のデフォルトを300秒へ変更
  - ページ内の未収集投稿を `process_posts_with_stats` で並列ダウンロードしてから埋め込み
  - parquetは `RollingParquetWriter` で大きいファイルへ集約
- `ingest_posts.py`:
  - `RollingParquetWriter` を追加
  - `AdaptiveDownloadController` を追加し、block率と download/embed 比率で worker 数を自動調整
  - `download_post_with_stats` と `build_row_from_downloaded` に処理を分離
- `database.bat`:
  - 初回 migration 後は常駐ループで collector cycle を継続実行
- `config.json` / `app/config.py` / `README.md`:
  - 5分永続化、rolling parquet、download worker 自動調整の設定を追加
- 検証:
  - `python3 -m compileall app` 成功

## Replan (2026-03-08, Throughput and Anti-Block)

### Problem Statement

- `sync_posts` / `backfill_posts` が投稿ごとに `sleep(ingest_sleep_sec)` を実行しており、通信帯域より先に固定ウェイトで律速されている。
- 一方で HTTP 429/5xx に対する再試行制御が弱く、速度を上げるとブロックリスクが高い。

### Plan

- [x] `app/ingest_posts.py` に HTTP リクエストの再試行と `Retry-After` 対応を実装する
- [x] 投稿単位の固定 `sleep` を除去し、必要時のみ（429/5xx時）待機する形へ変更する
- [x] `make_client` の接続設定を見直し、通信オーバーヘッドを下げる
- [x] 構文検証を実施し、レビュー欄に結果を記録する

### Review

- `sync_posts.py` / `backfill_posts.py`:
  - 投稿ごとの `sleep` を廃止
  - `build_state` 保存を `ingest_state_save_every`（デフォルト1000件）で間引き
  - `ingest_state_save_interval_sec`（デフォルト15秒）でも定期保存し、長時間運用の巻き戻りを抑制
- `build_state.py`:
  - state保存を tmp + replace の原子的書き込みに変更
- `ingest_posts.py`:
  - API/画像取得の共通リトライ (`429/5xx`, `Retry-After`, 指数バックオフ) を追加
  - HTTP/2 + connection limits を有効化
- `build_index.py`:
  - `vectors_raw` の `flush` を `vector_flush_every`（デフォルト16バッチ）へ間引き
  - `fsync` は明示オプトイン (`--vector-fsync`) 化
  - Faiss checkpoint は設定値（デフォルト20バッチ）で実行
- 設定:
  - `ingest_batch_size` を 512 に引き上げ
  - `ingest_sleep_sec` を 0.0 に変更
  - IO/リトライ関連の設定項目を追加
- 検証:
  - `python3 -m compileall app` 成功

## Replan (2026-03-08, impl_request verification)

### Problem Statement

- `docs/impl_request.md` 基準で見ると、`database.bat` は「最新差分」と「未構築の古い投稿」を自動で構築し続ける必要がある。
- 現在の実行ログだけでは、`sync_posts` が適切に終了して `backfill_posts` / `build_index` へ進むのか、またシンプルな運用設計を満たすのかが未確認。

### Plan

- [x] `docs/impl_request.md` を再読して検証基準を確定する
- [x] `database.bat` / `sync_posts.py` / `backfill_posts.py` / `build_index.py` の制御を要求に照らして確認する
- [x] 必要なら修正し、検証結果を Review に記録する

### Review

- 検証結果:
  - `page=a<ID>` は `ID` より新しい投稿、`page=b<ID>` は `ID` より古い投稿を返すことを実APIで確認
  - `data/images.db` は `max(id)=10204497`、`data/build_state.json` は `latest_cursor_id=10207747` まで進んでいた
  - 一方で Danbooru 最新投稿は `10912764` で、最新 backlog が大きく、従来実装では `sync_posts` が長時間戻らず `backfill_posts` / `build_index` が進まないことを確認
- 実装修正:
  - `sync_posts.py` / `backfill_posts.py` に `max-runtime-sec` を追加し、デフォルトで `collector_phase_budget_sec` 秒ごとに次段へ制御を返すよう変更
  - `config.json` / `app/config.py` / `README.md` に `collector_phase_budget_sec` を追加
- 検証:
  - `python3 -m compileall app` 成功

## Replan (2026-03-08, Single Collector Simplification)

### Problem Statement

- `sync_posts` と `backfill_posts` の分離は、要件の「シンプル」「メンテナンスしやすい」に反する。
- ユーザー意図は「最新から探索し、既取得を飛ばして未取得だけ埋める」単一 collector で十分。

### Plan

- [x] `backfill_posts` の責務を `sync_posts` に統合する
- [x] `build_state` を単一 collector 用の cursor / failure state に整理する
- [x] `database.bat` / `README.md` / 関連記録を単一 collector 前提へ更新する
- [x] 構文検証を実行する

### Review

- `app/sync_posts.py`:
  - 最新 head を取得して、そこから古い側へ1本で探索する collector に変更
  - DB に存在する ID は飛ばし、未取得だけを parquet へ出力
  - state は `sync_cursor_id` / `sync_failures` のみ保持
- `app/build_state.py`:
  - 旧 `latest_*` / `backfill_*` から新 `sync_*` への後方互換読み込みを追加
- `database.bat`:
  - `backfill_posts` 段を削除して 3 段構成へ簡素化
- 検証:
  - `python3 -m compileall app` 成功

## Replan (2026-03-08, Ctrl+C Drain)

### Problem Statement

- `sync_posts` は `Ctrl+C` 時に安全停止して parquet/state を flush するが、`database.bat` が終了コード `130` を失敗扱いして `build_index` を実行しない。
- そのため、途中停止時に DB/index へ反映されず「更新されていない」ように見える。

### Plan

- [x] `database.bat` で `sync_posts` の `130` を中断要求として扱う
- [x] 中断時でも `build_index` を1回実行してから終了するようにする
- [x] 変更内容を記録する

### Review

- `database.bat`:
  - `sync_posts` の戻り値を `SYNC_RC` に保持
  - `130` の場合は `STOP_AFTER_BUILD=1` として `build_index` 実行後に正常終了
  - それ以外の `errorlevel 1` は従来どおり失敗扱い

## Replan (2026-03-08, tags_from_id correctness)

### Problem Statement

- `/API/tags_from_id/{id}` が検索用ベクトルをそのままタグ分類ヘッドへ通しており、タグ復元が壊れている。
- `build_index.py` でも Faiss 用正規化後のベクトルを `vectors_raw` へ保存していたため、生特徴が失われる。

### Plan

- [x] `build_index.py` を修正し、`vectors_raw` には生特徴、Faiss には正規化コピーを保存する
- [x] `/API/tags_from_id/{id}` を保存済み生特徴ベクトル経路へ切り替える
- [x] 関連ドキュメントを更新し、構文検証する

### Review

- `app/build_index.py`:
  - `raw_embs` をそのまま `vectors_raw` に保存
  - `index_embs` のみ `faiss.normalize_L2` して index へ追加
- `app/main.py`:
  - `/API/tags_from_id/{id}` は保存済みベクトルを読み出してタグ確率を計算する
- 検証:
  - `python3 -m compileall app` 成功

## Replan (2026-03-08, web_local search UI + Danbooru link)

### Problem Statement

- `web_local/index.html` のタグ検索入力で、候補が表示されているだけで `Enter` 検索がブロックされ、検索確定が不安定。
- サジェスチョンは `blur` で閉じる実装と非同期 fetch の競合により、候補適用やクリック選択が取りこぼされる。
- モーダル内の Danbooru リンクは `image.url` を取得済みにもかかわらず `image.id` ベースで別 URL を組み直しており、表示とリンクの両方が破綻している。

### Plan

- [x] `web_local/index.html` の検索入力イベントを整理し、Enter/候補選択/外側クリックの責務を分離する
- [x] サジェスチョン fetch の古い応答を無視するようにし、候補適用の不安定さを解消する
- [x] Danbooru 情報描画を DOM ベースに置き換え、正しい post URL と崩れない見た目に修正する
- [x] 変更内容を静的確認し、レビュー欄へ結果を記録する

### Review

- [x] 実装後に記入
- `web_local/index.html`:
  - Enter は「候補選択中ならタグ確定、未選択なら検索実行」に整理し、候補が表示されているだけで検索不能になる条件を除去
  - `blur + setTimeout` を削除し、外側クリックで候補を閉じる形へ変更して、候補クリック取りこぼしを解消
  - サジェスチョン fetch に `tagFetchSeq` / `activeTagPrefix` を追加し、古い応答で候補一覧が巻き戻る競合を防止
  - Danbooru 情報は DOM 生成へ置換し、`image.url` をそのまま post URL として使うよう修正
- 検証:
  - `sed -n '256,1234p' web_local/index.html > /tmp/booruViewer_index_script.js && node --check /tmp/booruViewer_index_script.js` 成功

## Replan (2026-03-08, frontend media_url usage)

### Problem Statement

- `web_local/index.html` は API が返す `media_url` を使わず、各所で `/API/media/${image.id}` を手組みしている。
- そのため、一覧・詳細・ダウンロード・ドラッグが同じ URL ポリシーを共有できず、サムネイルと詳細の責務分離も曖昧になっている。

### Plan

- [x] `web_local/index.html` に画像URL解決ヘルパーを追加し、`image.media_url` を優先使用する
- [x] 一覧サムネイルと詳細/ダウンロード/ドラッグで使う URL をヘルパー経由へ統一する
- [x] 構文確認を行い、レビュー欄へ結果を記録する

### Review

- [x] 実装後に記入
- `web_local/index.html`:
  - `getMediaUrl(image, size)` を追加し、`image.media_url` を優先して一覧/詳細/ダウンロード/ドラッグの画像URLを解決するよう統一
  - 一覧は `getMediaUrl(image, '500x500')`、詳細・ダウンロード・ドラッグは `getMediaUrl(image)` を使う形へ整理
  - `media_url` がない場合のみ既存 `/API/media/${id}` をフォールバックする
- 検証:
  - `sed -n '256,1253p' web_local/index.html > /tmp/booruViewer_index_script.js && node --check /tmp/booruViewer_index_script.js` 成功

## Replan (2026-03-08, frontend direct booru image flow)

### Problem Statement

- `web_local/index.html` が画像取得を `/API/media` 前提で考え続けており、booru/CDN 直取得の要件から外れている。
- `docs/repo/ILEMB` 基準では、画像通信は検索結果の直 `media_url` と post URL から完結させるべきで、詳細ビューの高解像度もフロント側で解決する必要がある。

### Plan

- [x] `web_local/index.html` から `/API/media` 前提の画像URLフォールバックを外す
- [x] 一覧は直 `media_url`、詳細とダウンロードは post URL から解決した高解像度画像を使う
- [x] ドラッグは post URL を渡す形へ戻し、画像通信方法を repo 準拠へ揃える
- [x] 構文確認とレビュー記録を行う

### Review

- [x] 実装後に記入
- `web_local/index.html`:
  - `isDirectMediaUrl` / `getDirectMediaUrl` / `resolveDisplayImageUrl` を追加し、画像取得を booru/CDN の直URL優先に変更
  - 一覧カードは直 `media_url` だけを使い、カード生成時に HTML を取りに行かないよう変更
  - 詳細ビューは open 時にだけ post URL から 1 回 HTML を取得し、その結果を Danbooru 情報表示と高解像度画像解決に共用
  - ダウンロードは明示操作時のみ高解像度画像を解決する
  - ドラッグで渡す URL は画像APIではなく post URL に戻した
  - サムネイル読み込みは `IntersectionObserver` で lazy load するよう追加
- `tasks/lessons.md`:
  - `docs/repo` 先読みと「フロントだけ」の制約を外さないルールを追加
- 検証:
  - `sed -n '256,1294p' web_local/index.html > /tmp/booruViewer_index_script.js && node --check /tmp/booruViewer_index_script.js` 成功

## Replan (2026-03-08, search result thumb_url split)

### Problem Statement

- 一覧サムネと詳細画像の責務が検索結果に分離されておらず、フロントが `/API/media` や post HTML 解決を混在させている。
- バックエンドは CDN URL を復元できるのに、検索結果へサムネURLを載せていない。

### Plan

- [x] `ImageEntry` に一覧用 `thumb_url` を追加し、検索結果生成で CDN サムネURLを埋める
- [x] `media_url` を `/API/media` ではなく CDN 直URLへ戻し、一覧は `thumb_url`、詳細は post HTML 解決に整理する
- [x] `web_local/index.html` を `thumb_url` lazy load 前提へ更新する
- [x] Python/JS の構文確認を行い、レビューへ結果を記録する

### Review

- [x] 実装後に記入
- `app/schemas.py`:
  - 検索結果に一覧用 `thumb_url` を追加
- `app/url_utils.py` / `app/services/search_service.py`:
  - CDN URL 生成をサイズ指定対応へ変更
  - `media_url` は 720x720 の CDN 直URL、`thumb_url` は 360x360 の CDN URL を返すよう整理
- `web_local/index.html`:
  - 一覧カードは `thumb_url` を `IntersectionObserver` で lazy load
  - 詳細ビューとダウンロードは引き続き post HTML 解決で高解像度URLを使う
  - 一覧表示の主経路から `/API/media` 依存を外した
- 検証:
  - `python3 -m compileall app` 成功
  - `sed -n '256,1318p' web_local/index.html > /tmp/booruViewer_index_script.js && node --check /tmp/booruViewer_index_script.js` 成功

## Replan (2026-03-08, remove unsupported resolution URL derivation)

### Problem Statement

- `thumb_url` とサイズ可変 CDN URL を追加したが、保存データと repo 実装から保証できるのは `720x720` 復元だけだった。
- 解像度ごとの URL を同じ規則で派生させるのは根拠がなく、余計な一般化になっていた。

### Plan

- [x] `thumb_url` とサイズ可変 CDN URL 生成を削除する
- [x] 検索結果は `media_url=720x720 CDN URL` のみへ戻す
- [x] 一覧は既存 `media_url` を lazy load、詳細は HTML 解決のまま維持する
- [x] 構文確認と記録更新を行う

### Review

- [x] 実装後に記入
- `app/schemas.py` / `app/services/search_service.py`:
  - `thumb_url` を削除し、検索結果は再び `media_url` のみを返す形へ戻した
- `app/url_utils.py`:
  - `build_cdn_url(...)` を repo 準拠の `720x720` 固定へ戻し、サイズ引数と `build_thumb_url(...)` を削除
- `web_local/index.html`:
  - 一覧カードは `media_url` を lazy load するだけの形へ戻した
  - 詳細ビューとダウンロードの post HTML 解決は維持
- `tasks/lessons.md`:
  - 未確認の解像度別 URL パターンを勝手に生成しないルールを追加
- 検証:
  - `python3 -m compileall app` 成功
  - `sed -n '256,1315p' web_local/index.html > /tmp/booruViewer_index_script.js && node --check /tmp/booruViewer_index_script.js` 成功

## Replan (2026-03-08, sync_posts component_parse_failed)

### Problem Statement

- `database.bat` 実行時の `sync_posts` で `component_parse_failed` が継続発生している。
- 原因は `choose_record_url()` が `720x720` 記録URLの選定に `_pick_variant()` を流用しており、縦長画像では `sample` を拾ってしまうこと。
- `sample-<md5>.jpg` URL は現在の component 解析対象ではないため、記録前に失敗扱いになる。

### Plan

- [x] `choose_record_url()` を `720x720` variant 厳密選択へ変更し、`sample` へフォールバックしない
- [x] `component_parse_failed` の原因だった直近投稿パターンで再現確認する
- [x] `python3 -m compileall app` と実データ spot check の結果を Review に記録する

### Review

- [x] 実装後に記入
- 原因:
  - `choose_record_url()` が `_pick_variant()` の最短辺ペナルティを共有していたため、縦長画像では `720x720` より `sample` を選ぶことがあった
  - `sample-<md5>.jpg` URL は component 解析正規表現の対象外で、`component_parse_failed` として失敗計上されていた
- 実装:
  - `app/ingest_posts.py` の `choose_record_url()` を `media_asset.variants` から `type == "720x720"` の URL だけを返す実装へ変更
  - 埋め込み取得用の `choose_image_url()` には手を入れず、記録URL選定だけを分離した
- 検証:
  - `python3 -m compileall app` 成功
  - Danbooru 最新 200 件の spot check で、修正前に相当した `sample` 起因の `component_parse_failed` が 0 件になったことを確認
  - 同 spot check の結果は `ok=197`, `no_record_url=3`, `component_parse_failed=0`
- 未実施:
  - Windows 側の `database.bat` 実行で同ログ傾向になることの実機確認
