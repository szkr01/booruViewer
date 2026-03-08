from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass
class Settings:
    base_dir: Path
    data_dir: Path
    db_path: Path
    faiss_index_path: Path
    vectors_raw_path: Path
    embedding_dim: int
    model_repo: str
    app_host: str
    app_port: int
    recent_default_limit: int
    rating_threshold: int
    legacy_cache_dirs: list[Path]
    parquet_glob: str | None
    tag_csv_path: Path | None
    build_state_path: Path
    ingest_incoming_dir: Path
    ingest_parquet_glob: str
    ingest_page_size: int
    ingest_batch_size: int
    ingest_sleep_sec: float
    ingest_state_save_interval_sec: float
    ingest_roll_max_rows: int
    ingest_roll_max_mib: int
    ingest_download_workers: int
    ingest_download_workers_min: int
    ingest_download_workers_max: int
    ingest_download_autotune_interval: int
    ingest_http_max_retries: int
    ingest_http_retry_base_sec: float
    ingest_http_retry_backoff: float
    ingest_http_block_cooldown_sec: float
    collector_phase_budget_sec: float
    danbooru_api_base: str
    require_cuda_for_ingest: bool
    build_index_checkpoint_every: int
    build_index_vector_flush_every: int
    build_index_vector_fsync: bool


class SettingsLoader:
    def __init__(self) -> None:
        self.base_dir = Path(__file__).resolve().parent.parent
        self.config_path = self.base_dir / "config.json"

    def load(self) -> Settings:
        config = self._load_json()

        data_dir = self._resolve_path(config.get("data_dir", "data"))
        data_dir.mkdir(parents=True, exist_ok=True)

        db_path = self._resolve_path(config.get("db_path", str(data_dir / "images.db")))
        faiss_index_path = self._resolve_path(config.get("faiss_index_path", str(data_dir / "faiss.index")))
        vectors_raw_path = self._resolve_path(config.get("vectors_raw_path", str(data_dir / "vectors_raw.f16")))

        legacy_cache_dirs = [
            self._resolve_path(p)
            for p in config.get("legacy_cache_dirs", [])
            if isinstance(p, str) and p.strip()
        ]

        parquet_glob = config.get("parquet_glob")
        if parquet_glob is not None and not isinstance(parquet_glob, str):
            parquet_glob = None

        tag_csv_raw = config.get("tag_csv_path")
        tag_csv_path = None
        if isinstance(tag_csv_raw, str) and tag_csv_raw.strip():
            tag_csv_path = self._resolve_path(tag_csv_raw)

        build_state_path = self._resolve_path(config.get("build_state_path", str(data_dir / "build_state.json")))
        ingest_incoming_dir = self._resolve_path(config.get("ingest_incoming_dir", str(data_dir / "incoming")))
        ingest_parquet_glob = str(config.get("ingest_parquet_glob", "data/incoming/*.parquet"))
        ingest_page_size = int(config.get("ingest_page_size", 200))
        ingest_batch_size = int(config.get("ingest_batch_size", 512))
        ingest_sleep_sec = float(config.get("ingest_sleep_sec", 0.0))
        ingest_state_save_interval_sec = float(config.get("ingest_state_save_interval_sec", 300.0))
        ingest_roll_max_rows = int(config.get("ingest_roll_max_rows", 50000))
        ingest_roll_max_mib = int(config.get("ingest_roll_max_mib", 512))
        ingest_download_workers = int(config.get("ingest_download_workers", 6))
        ingest_download_workers_min = int(config.get("ingest_download_workers_min", 2))
        ingest_download_workers_max = int(config.get("ingest_download_workers_max", 16))
        ingest_download_autotune_interval = int(config.get("ingest_download_autotune_interval", 64))
        ingest_http_max_retries = int(config.get("ingest_http_max_retries", 4))
        ingest_http_retry_base_sec = float(config.get("ingest_http_retry_base_sec", 0.6))
        ingest_http_retry_backoff = float(config.get("ingest_http_retry_backoff", 1.8))
        ingest_http_block_cooldown_sec = float(config.get("ingest_http_block_cooldown_sec", 2.0))
        collector_phase_budget_sec = float(config.get("collector_phase_budget_sec", 300.0))
        danbooru_api_base = str(config.get("danbooru_api_base", "https://danbooru.donmai.us"))
        require_cuda_for_ingest = bool(config.get("require_cuda_for_ingest", True))
        build_index_checkpoint_every = int(config.get("build_index_checkpoint_every", 20))
        build_index_vector_flush_every = int(config.get("build_index_vector_flush_every", 16))
        build_index_vector_fsync = bool(config.get("build_index_vector_fsync", False))

        return Settings(
            base_dir=self.base_dir,
            data_dir=data_dir,
            db_path=db_path,
            faiss_index_path=faiss_index_path,
            vectors_raw_path=vectors_raw_path,
            embedding_dim=int(config.get("embedding_dim", os.environ.get("APP_EMBEDDING_DIM", 1024))),
            model_repo=str(config.get("model_repo", os.environ.get("APP_REPO_NAME", "SmilingWolf/wd-eva02-large-tagger-v3"))),
            app_host=str(config.get("app_host", os.environ.get("APP_HOST", "0.0.0.0"))),
            app_port=int(config.get("app_port", os.environ.get("APP_PORT", 8002))),
            recent_default_limit=int(config.get("recent_default_limit", 120)),
            rating_threshold=int(config.get("rating_threshold", 3)),
            legacy_cache_dirs=legacy_cache_dirs,
            parquet_glob=parquet_glob,
            tag_csv_path=tag_csv_path,
            build_state_path=build_state_path,
            ingest_incoming_dir=ingest_incoming_dir,
            ingest_parquet_glob=ingest_parquet_glob,
            ingest_page_size=ingest_page_size,
            ingest_batch_size=ingest_batch_size,
            ingest_sleep_sec=ingest_sleep_sec,
            ingest_state_save_interval_sec=ingest_state_save_interval_sec,
            ingest_roll_max_rows=ingest_roll_max_rows,
            ingest_roll_max_mib=ingest_roll_max_mib,
            ingest_download_workers=ingest_download_workers,
            ingest_download_workers_min=ingest_download_workers_min,
            ingest_download_workers_max=ingest_download_workers_max,
            ingest_download_autotune_interval=ingest_download_autotune_interval,
            ingest_http_max_retries=ingest_http_max_retries,
            ingest_http_retry_base_sec=ingest_http_retry_base_sec,
            ingest_http_retry_backoff=ingest_http_retry_backoff,
            ingest_http_block_cooldown_sec=ingest_http_block_cooldown_sec,
            collector_phase_budget_sec=collector_phase_budget_sec,
            danbooru_api_base=danbooru_api_base,
            require_cuda_for_ingest=require_cuda_for_ingest,
            build_index_checkpoint_every=build_index_checkpoint_every,
            build_index_vector_flush_every=build_index_vector_flush_every,
            build_index_vector_fsync=build_index_vector_fsync,
        )

    def _load_json(self) -> dict[str, Any]:
        if not self.config_path.exists():
            return {}
        try:
            with self.config_path.open("r", encoding="utf-8") as f:
                loaded = json.load(f)
                return loaded if isinstance(loaded, dict) else {}
        except Exception as exc:
            print(f"Failed to read config.json: {exc}")
            return {}

    def _resolve_path(self, path_value: str) -> Path:
        path = Path(path_value)
        if path.is_absolute():
            return path
        return self.base_dir / path


settings = SettingsLoader().load()
