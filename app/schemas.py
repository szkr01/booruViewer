from __future__ import annotations

from pydantic import BaseModel


class ImageEntry(BaseModel):
    id: str
    url: str
    media_url: str
    rating: int
    score: float = 0.0


class TagProbability(BaseModel):
    tag_name: str
    probability: float
