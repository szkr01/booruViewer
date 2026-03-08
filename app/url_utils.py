from __future__ import annotations

from typing import Mapping

EXTENSION_MAPPING = {
    0: "jpg",
    1: "png",
    2: "jpeg",
    3: "bmp",
    4: "webp",
}


def build_post_url(post_id: int) -> str:
    return f"https://danbooru.donmai.us/posts/{post_id}"


def build_cdn_url(row: Mapping[str, int]) -> str:
    c1 = int(row["c1"])
    c2 = int(row["c2"])
    # c3/c4 are stored in signed 64-bit columns; restore raw 64-bit bits for hex filename.
    c3 = int(row["c3"]) & 0xFFFFFFFFFFFFFFFF
    c4 = int(row["c4"]) & 0xFFFFFFFFFFFFFFFF
    c5 = int(row["c5"])

    filename = f"{c3:016x}{c4:016x}"
    ext = EXTENSION_MAPPING.get(c5, "jpg")
    return f"https://cdn.donmai.us/720x720/{c1:02x}/{c2:02x}/{filename}.{ext}"
