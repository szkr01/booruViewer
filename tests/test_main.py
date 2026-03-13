from __future__ import annotations

import importlib
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch


def _install_main_import_stubs() -> None:
    if "fastapi" in sys.modules:
        return

    fastapi = types.ModuleType("fastapi")
    responses = types.ModuleType("fastapi.responses")
    staticfiles = types.ModuleType("fastapi.staticfiles")
    middleware = types.ModuleType("fastapi.middleware")
    cors = types.ModuleType("fastapi.middleware.cors")

    class FakeResponse:
        def __init__(self, content=None, media_type=None):
            self.content = content
            self.media_type = media_type
            self.headers: dict[str, str] = {}

    class FakeHTTPException(Exception):
        def __init__(self, status_code: int, detail: str):
            super().__init__(detail)
            self.status_code = status_code
            self.detail = detail

    class FakeFastAPI:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

        def add_middleware(self, *args, **kwargs):
            return None

        def mount(self, *args, **kwargs):
            return None

        def get(self, *args, **kwargs):
            def decorator(fn):
                return fn

            return decorator

        def post(self, *args, **kwargs):
            def decorator(fn):
                return fn

            return decorator

    def _identity(value=None, **_kwargs):
        return value

    fastapi.FastAPI = FakeFastAPI
    fastapi.File = _identity
    fastapi.Form = _identity
    fastapi.Query = _identity
    fastapi.HTTPException = FakeHTTPException
    fastapi.Request = type("Request", (), {})
    fastapi.Response = FakeResponse
    fastapi.UploadFile = type("UploadFile", (), {})
    responses.FileResponse = type("FileResponse", (), {})
    responses.Response = FakeResponse
    staticfiles.StaticFiles = type("StaticFiles", (), {"__init__": lambda self, *args, **kwargs: None})
    cors.CORSMiddleware = type("CORSMiddleware", (), {})

    sys.modules["fastapi"] = fastapi
    sys.modules["fastapi.responses"] = responses
    sys.modules["fastapi.staticfiles"] = staticfiles
    sys.modules["fastapi.middleware"] = middleware
    sys.modules["fastapi.middleware.cors"] = cors

    numpy = types.ModuleType("numpy")
    numpy.where = lambda *_args, **_kwargs: []
    sys.modules["numpy"] = numpy

    httpx = types.ModuleType("httpx")
    httpx.AsyncClient = type("AsyncClient", (), {})
    httpx.HTTPError = Exception
    sys.modules["httpx"] = httpx

    uvicorn = types.ModuleType("uvicorn")
    sys.modules["uvicorn"] = uvicorn

    pil = types.ModuleType("PIL")
    pil_image = types.ModuleType("PIL.Image")
    pil_image.Image = type("Image", (), {})
    pil_image.open = lambda *_args, **_kwargs: None
    pil.Image = pil_image
    sys.modules["PIL"] = pil
    sys.modules["PIL.Image"] = pil_image

    config = types.ModuleType("app.config")
    config.settings = types.SimpleNamespace(base_dir=Path("."), db_path=Path("db.sqlite"), faiss_index_path=Path("faiss.index"), vectors_raw_path=Path("vectors_raw.f16"))
    sys.modules["app.config"] = config

    database = types.ModuleType("app.database")
    database.db = types.SimpleNamespace(get_stats=lambda: {"post_count": 0, "min_id": -1, "max_id": -1, "min_vec_idx": -1, "max_vec_idx": -1}, count=lambda: 0)
    sys.modules["app.database"] = database

    schemas = types.ModuleType("app.schemas")
    schemas.TagProbability = type("TagProbability", (), {})
    sys.modules["app.schemas"] = schemas

    services = types.ModuleType("app.services.search_service")
    services.search_service = types.SimpleNamespace(search_with_mode=lambda **_kwargs: ([], "recent", "empty_query"), recent=lambda **_kwargs: [], get_cdn_url_by_post_id=lambda *_args, **_kwargs: None)
    sys.modules["app.services.search_service"] = services

    tag_engine = types.ModuleType("app.tag_engine")
    tag_engine.tag_engine = types.SimpleNamespace(load=lambda: None, model=None, get_tags_prefix=lambda **_kwargs: [], tag_names_in_model_order=[], get_tag_probabilities=lambda *_args, **_kwargs: None)
    sys.modules["app.tag_engine"] = tag_engine

    vector_store = types.ModuleType("app.vector_store")
    vector_store.vector_store = types.SimpleNamespace(load=lambda: None, is_ready=lambda: False, search_id_mode="post_id", vectors_raw=None, query_vector_by_post_id=lambda *_args, **_kwargs: None)
    sys.modules["app.vector_store"] = vector_store


_install_main_import_stubs()
main = importlib.import_module("app.main")


class SearchApiTests(unittest.IsolatedAsyncioTestCase):
    async def test_search_images_passes_requested_limit_to_service(self) -> None:
        response = main.Response()

        with patch.object(
            main.search_service,
            "search_with_mode",
            return_value=([], "recent", "empty_query"),
        ) as search_mock, patch.object(main.db, "count", return_value=0):
            await main.search_images(
                response=response,
                q="tag",
                q_query=None,
                image=None,
                image_weight=None,
                image_wight=None,
                limit=1234,
                offset=5,
            )

        search_mock.assert_called_once_with(
            query_text="tag",
            query_images=[],
            image_weights=None,
            limit=1234,
            offset=5,
        )

    async def test_search_images_clamps_limit_to_100000(self) -> None:
        response = main.Response()

        with patch.object(
            main.search_service,
            "search_with_mode",
            return_value=([], "recent", "empty_query"),
        ) as search_mock, patch.object(main.db, "count", return_value=0):
            await main.search_images(
                response=response,
                q="tag",
                q_query=None,
                image=None,
                image_weight=None,
                image_wight=None,
                limit=main.SEARCH_LIMIT_MAX + 1,
                offset=0,
            )

        search_mock.assert_called_once_with(
            query_text="tag",
            query_images=[],
            image_weights=None,
            limit=main.SEARCH_LIMIT_MAX,
            offset=0,
        )


if __name__ == "__main__":
    unittest.main()
