# booruViewer

Danbooru向けローカルビューアーです。`PureILEMB`のフロントを流用し、既存のベクトル資産を再利用できる構成にしています。
検索はローカルのベクトルインデックスのみを使用し、外部booru検索APIは使用しません。

## セットアップ

```bash
setup.bat
```

## 使い方

1. `setup.bat` を1回実行
2. 旧 ILEMB の cache dir を使う場合は `migrate_legacy.bat "<ILEMB_cache_dir>" --force` を1回実行
   - 必須ファイル: `search_ivfpq.index`, `metadata.npy`, `id_map.npy`
   - `vectors_raw.npy` がある場合は `vectors_raw.f16` に変換して取り込みます
3. `database.bat` を実行して常駐させる。起動中は以下を繰り返し自動実行
   - 単一 collector (`app.sync_posts`) が最新から古い側へ探索し、既取得IDを飛ばして未取得IDだけを収集
   - parquetからの増分同期 (`app.build_index`)
   - collector は `collector_phase_budget_sec` の範囲で進み、毎サイクル `build_index` まで到達する
4. `app.bat` を実行
5. ブラウザで `http://localhost:8002/app` を開く

## 設定

`config.json` で以下を変更できます。

- `legacy_cache_dirs`: `app.migrate_legacy` を直接実行する場合の既存キャッシュ探索先
- `parquet_glob`: 既存キャッシュがない場合に取り込むparquetのglob
- `db_path`, `faiss_index_path`, `vectors_raw_path`: 出力先
- `ingest_state_save_interval_sec`: state 永続化間隔（デフォルト300秒）。件数基準では保存しません
- `ingest_batch_size`: 収集済み行を parquet へ flush する単位。GPU 推論バッチサイズではありません
- `ingest_roll_max_rows`, `ingest_roll_max_mib`: 収集parquetのローテーション条件
- `ingest_download_workers*`: ダウンロード並列数と自動調整範囲
- `ingest_preprocess_workers`: ダウンロード後の画像前処理を並列実行する worker 数
- `ingest_preprocess_queue_factor`: download/preprocess/embed 間キューの深さ係数
- `ingest_embed_batch_size`: 画像特徴抽出を GPU へまとめて投げる推論バッチサイズ
- `ingest_embed_max_wait_ms`: 推論キューを時間で flush する上限待ち時間
- `ingest_embed_autocast`: CUDA 推論時に autocast(fp16) を使うか
- `ingest_media_http2`: 画像ダウンロードで HTTP/2 を使うか。既定は `false` で、worker ごとの複数接続を優先します
- `collector_phase_budget_sec`: 単一 collector が1サイクルで使う時間予算。未完了分は次サイクルで再開
- `docs/` 配下は参照しません。実データのパスを明示指定してください。

## API互換

- `POST /API/search`
- `GET /API/media/{id}`
- `GET /API/tags` (現在は空配列)
- `GET /API/tags_from_id/{id}`:
  - 保存済みの生特徴ベクトルからタグ確率を計算します
  - 検索用正規化ベクトルは使いません

## 旧 ILEMB 資産の移行

- 旧 ILEMB の 400万件 cache は parquet ではなく cache dir 単位で移行します
- 実行例:

```bat
migrate_legacy.bat "D:\ILEMB\data\10204497-5784553" --force
```

- 移行後は `app.bat` の起動ログで以下を確認してください
  - `posts=...`
  - `vec_idx_range=0..N`
  - `vectors_shape=(N+1, 1024)`
- `database.bat` は移行後の `data/` を前提に、以後の継続収集だけを担当します
