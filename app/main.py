from __future__ import annotations

from contextlib import asynccontextmanager
import io
import logging
from pathlib import Path

import httpx
import numpy as np
import uvicorn
from fastapi import FastAPI, File, Form, HTTPException, Query, Request, Response, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from PIL import Image

from .config import settings
from .database import db
from .schemas import TagProbability
from .services.search_service import search_service
from .tag_engine import tag_engine
from .vector_store import vector_store

REQUEST_HEADERS = {
    "User-Agent": "booruViewer/0.1 (+https://danbooru.donmai.us/)",
}
logger = logging.getLogger("booruViewer.search")
if not logging.getLogger().handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


class _MediaAccessLogFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        return "/API/media/" not in msg


_uvicorn_access_logger = logging.getLogger("uvicorn.access")
for _handler in _uvicorn_access_logger.handlers:
    _handler.addFilter(_MediaAccessLogFilter())


def _file_log_meta(path: Path) -> str:
    try:
        resolved = path.resolve()
    except Exception:
        resolved = path

    exists = resolved.exists()
    size = resolved.stat().st_size if exists else -1
    return f"path={resolved} exists={exists} size={size}"


def _log_startup_diagnostics() -> None:
    stats = db.get_stats()
    logger.info(
        "startup db path=%s posts=%d id_range=%d..%d vec_idx_range=%d..%d",
        settings.db_path,
        stats["post_count"],
        stats["min_id"],
        stats["max_id"],
        stats["min_vec_idx"],
        stats["max_vec_idx"],
    )
    logger.info("startup db_file %s", _file_log_meta(settings.db_path))
    logger.info("startup faiss_file %s", _file_log_meta(settings.faiss_index_path))
    logger.info("startup vectors_file %s", _file_log_meta(settings.vectors_raw_path))


@asynccontextmanager
async def lifespan(app: FastAPI):
    _log_startup_diagnostics()
    vector_store.load()
    tag_engine.load()
    logger.info(
        "startup runtime index_ready=%s vectors_shape=%s model_loaded=%s",
        vector_store.is_ready(),
        None if vector_store.vectors_raw is None else tuple(vector_store.vectors_raw.shape),
        tag_engine.model is not None,
    )
    yield


app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/app")
@app.get("/app/")
async def serve_app(request: Request):
    user_agent = request.headers.get("user-agent", "").lower()
    if any(word in user_agent for word in ["android", "iphone", "ipad", "mobile"]):
        return FileResponse(str(settings.base_dir / "web_local" / "index_mobile.html"))
    return FileResponse(str(settings.base_dir / "web_local" / "index.html"))


app.mount("/assets", StaticFiles(directory=str(settings.base_dir / "web_local" / "assets")), name="assets")
app.mount("/app", StaticFiles(directory=str(settings.base_dir / "web_local"), html=False), name="frontend")


@app.get("/health")
async def health() -> dict[str, str | bool]:
    return {"ok": True, "index_loaded": vector_store.is_ready()}


@app.post("/API/search")
async def search_images(
    response: Response,
    q: str | None = Form(None),
    q_query: str | None = Query(None, alias="q"),
    image: list[UploadFile] | None = File(None),
    image_weight: list[float] | None = Form(None),
    image_wight: list[float] | None = Form(None),
    limit: int = Form(100),
    offset: int = Form(0),
):
    query_text = q if q is not None else q_query
    weights = image_weight if image_weight is not None else image_wight

    query_images: list[Image.Image] = []
    if image:
        for upload_file in image:
            content = await upload_file.read()
            try:
                img = Image.open(io.BytesIO(content)).convert("RGB")
                query_images.append(img)
            except Exception:
                continue

    limited = max(1, min(limit, 500))
    clamped_offset = max(0, offset)
    local_results, mode, reason = search_service.search_with_mode(
        query_text=query_text,
        query_images=query_images,
        image_weights=weights,
        limit=limited,
        offset=clamped_offset,
    )
    response.headers["X-Search-Mode"] = mode
    response.headers["X-Search-Reason"] = reason

    logger.info(
        "search mode=%s reason=%s q=%r images=%d weights=%d limit=%d offset=%d results=%d",
        mode,
        reason,
        (query_text or "")[:120],
        len(query_images),
        len(weights) if weights else 0,
        limited,
        clamped_offset,
        len(local_results),
    )

    # Web UI expectation: avoid [] for first-page searches.
    if local_results:
        return local_results

    if clamped_offset == 0 and db.count() > 0:
        response.headers["X-Search-Mode"] = "recent"
        response.headers["X-Search-Reason"] = "final_empty_fallback"
        logger.info(
            "search mode=recent reason=final_empty_fallback q=%r images=%d limit=%d offset=%d",
            (query_text or "")[:120],
            len(query_images),
            limited,
            clamped_offset,
        )
        return search_service.recent(limit=limited, offset=0)
    return local_results


@app.get("/API/media/{entry_id}")
async def get_media(entry_id: int, size: str | None = None):
    _ = size
    cdn_url = search_service.get_cdn_url_by_post_id(entry_id)
    if not cdn_url:
        raise HTTPException(status_code=404, detail="No media source found")

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        # Fast path: direct CDN URL from local metadata
        try:
            resp = await client.get(
                cdn_url,
                headers={**REQUEST_HEADERS, "Referer": f"https://danbooru.donmai.us/posts/{entry_id}"},
            )
            if resp.status_code < 400:
                return Response(content=resp.content, media_type=resp.headers.get("content-type", "image/jpeg"))
        except httpx.HTTPError:
            resp = None

        # Slow path fallback: only when direct URL fails
        post_json_url = f"https://danbooru.donmai.us/posts/{entry_id}.json"
        try:
            meta_resp = await client.get(post_json_url, headers=REQUEST_HEADERS)
            if meta_resp.status_code >= 400:
                raise HTTPException(status_code=meta_resp.status_code, detail="Failed to resolve media URL")
            post = meta_resp.json()
        except httpx.HTTPError as exc:
            raise HTTPException(status_code=502, detail=f"Failed to resolve media URL: {exc}") from exc

        fallback_urls: list[str] = []
        for key in ["large_file_url", "file_url", "preview_file_url"]:
            url = post.get(key)
            if isinstance(url, str) and url:
                if url.startswith("//"):
                    url = "https:" + url
                elif url.startswith("/"):
                    url = "https://danbooru.donmai.us" + url
                fallback_urls.append(url)

        for url in fallback_urls:
            try:
                fr = await client.get(
                    url,
                    headers={**REQUEST_HEADERS, "Referer": f"https://danbooru.donmai.us/posts/{entry_id}"},
                )
                if fr.status_code < 400:
                    return Response(content=fr.content, media_type=fr.headers.get("content-type", "image/jpeg"))
            except httpx.HTTPError:
                continue

    raise HTTPException(status_code=502, detail="Failed to fetch media from all sources")


@app.get("/API/tags")
async def get_tags(prefix: str = ""):
    return tag_engine.get_tags_prefix(prefix=prefix, limit=20)


@app.get("/API/tags_from_id/{entry_id}", response_model=list[TagProbability])
async def get_tags_from_id(entry_id: int, threshold: float = 0.1):
    vec = vector_store.query_vector_by_post_id(entry_id)
    if vec is None:
        raise HTTPException(status_code=404, detail="Image not found")

    probs = tag_engine.get_tag_probabilities(vec.reshape(1, -1).astype(np.float32, copy=False))
    if probs is None or probs.size == 0:
        return []

    p = probs[0]
    indices = np.where(p > threshold)[0]

    results: list[TagProbability] = []
    for idx in indices:
        if idx >= len(tag_engine.tag_names_in_model_order):
            continue
        results.append(
            TagProbability(
                tag_name=tag_engine.tag_names_in_model_order[int(idx)],
                probability=float(p[int(idx)]),
            )
        )

    results.sort(key=lambda x: x.probability, reverse=True)
    return results


if __name__ == "__main__":
    uvicorn.run("app.main:app", host=settings.app_host, port=settings.app_port, reload=False)
