from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from pathlib import Path

import httpx
import numpy as np
import timm
import torch
import torch.nn as nn
from PIL import Image
import torchvision.transforms.functional as TF

from .config import settings


@dataclass
class TagRow:
    name: str
    category: int
    count: int


class TagEngine:
    TARGET_SIZE = 448

    def __init__(self) -> None:
        self.rows: list[TagRow] = []
        self.tag_names_in_model_order: list[str] = []
        self.name_to_index: dict[str, int] = {}

        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.model = None
        self.tag_weight: np.ndarray | None = None
        self.tag_bias: np.ndarray | None = None

    def load(self) -> None:
        self._load_tag_csv()
        self._load_model_head_and_encoder()

    def _load_tag_csv(self) -> None:
        # 1) explicit local file
        if settings.tag_csv_path and settings.tag_csv_path.exists():
            raw = settings.tag_csv_path.read_text(encoding="utf-8")
            self._parse_tag_csv(raw)
            return

        # 2) cached local copy
        cached = settings.data_dir / "selected_tags.csv"
        if cached.exists():
            self._parse_tag_csv(cached.read_text(encoding="utf-8"))
            return

        # 3) remote model asset
        url = f"https://huggingface.co/{settings.model_repo}/raw/main/selected_tags.csv"
        try:
            with httpx.Client(timeout=20.0, follow_redirects=True) as client:
                resp = client.get(url)
                if resp.status_code < 400 and resp.text.strip():
                    self._parse_tag_csv(resp.text)
                    try:
                        cached.write_text(resp.text, encoding="utf-8")
                    except Exception:
                        pass
                    return
        except httpx.HTTPError:
            pass

        # last resort: empty dictionary
        self.rows = []
        self.tag_names_in_model_order = []
        self.name_to_index = {}

    def _parse_tag_csv(self, raw: str) -> None:
        reader = csv.DictReader(io.StringIO(raw))
        rows: list[TagRow] = []
        for r in reader:
            name = str(r.get("name", "")).strip()
            if not name:
                continue
            category = int(r.get("category", 0) or 0)
            count = int(r.get("count", 0) or 0)
            rows.append(TagRow(name=name, category=category, count=count))

        self.rows = rows
        self.tag_names_in_model_order = [x.name for x in rows]
        self.name_to_index = {name: i for i, name in enumerate(self.tag_names_in_model_order)}

    def _load_model_head_and_encoder(self) -> None:
        try:
            model = timm.create_model(f"hf-hub:{settings.model_repo}", pretrained=True).eval()
            model = model.to(self.device)

            head_state = model.head.state_dict()
            self.tag_weight = head_state["weight"].cpu().numpy().astype(np.float32)
            self.tag_bias = head_state["bias"].cpu().numpy().astype(np.float32)

            model.head = nn.Identity()
            self.model = model
        except Exception as exc:
            print(f"Warning: failed to load model ({settings.model_repo}): {exc}")
            self.model = None
            self.tag_weight = None
            self.tag_bias = None

    def get_tags_prefix(self, prefix: str, limit: int = 20) -> list[tuple[str, int, int]]:
        p = prefix.strip()
        if not p:
            return []

        matched = [r for r in self.rows if r.name.startswith(p)]
        matched.sort(key=lambda x: x.count, reverse=True)
        if matched:
            return [(r.name, r.category, r.count) for r in matched[:limit]]

        # When dictionary is unavailable or no match, return typed prefix as fallback candidate
        return [(p, 0, 0)]

    def str_to_tags(self, query: str) -> list[tuple[int, float]]:
        tags_with_weights: list[tuple[int, float]] = []
        for part in query.split():
            token = part.strip()
            if not token:
                continue

            tag = token
            weight = 1.0
            if token.startswith("(") and token.endswith(")") and ":" in token:
                try:
                    inside = token[1:-1]
                    t, w = inside.rsplit(":", 1)
                    tag = t.strip()
                    weight = float(w)
                except ValueError:
                    tag = token
                    weight = 1.0

            idx = self.name_to_index.get(tag)
            if idx is not None:
                tags_with_weights.append((idx, weight))
        return tags_with_weights

    def extract_tag_feature(self, index: int) -> np.ndarray | None:
        if self.tag_weight is None or self.tag_bias is None:
            return None
        if index < 0 or index >= self.tag_weight.shape[0]:
            return None

        weight_vec = self.tag_weight[index]
        bias_val = self.tag_bias[index]
        norm = float(np.linalg.norm(weight_vec))
        if norm <= 1e-8:
            norm = 1e-8
        normalized = weight_vec / norm
        bias_mult = 1.0 + (np.arctan(bias_val) + np.pi / 2.0) / np.pi
        return (normalized * bias_mult).astype(np.float32)

    def extract_image_feature(self, img: Image.Image) -> np.ndarray | None:
        if self.model is None:
            return None

        tensor = self._preprocess_image(img).unsqueeze(0).to(self.device)
        with torch.no_grad():
            feat = self.model(tensor)
        return feat.detach().cpu().numpy().astype(np.float32)

    def _preprocess_image(self, img: Image.Image) -> torch.Tensor:
        img = img.convert("RGB")
        w, h = img.size
        aspect = w / max(h, 1)

        if aspect > 1:
            new_w = self.TARGET_SIZE
            new_h = int(self.TARGET_SIZE / aspect)
        else:
            new_h = self.TARGET_SIZE
            new_w = int(self.TARGET_SIZE * aspect)

        img = img.resize((max(1, new_w), max(1, new_h)), Image.Resampling.BICUBIC)
        t = TF.to_tensor(img)

        pad_l = (self.TARGET_SIZE - new_w) // 2
        pad_t = (self.TARGET_SIZE - new_h) // 2
        pad_r = self.TARGET_SIZE - new_w - pad_l
        pad_b = self.TARGET_SIZE - new_h - pad_t
        t = TF.pad(t, (pad_l, pad_t, pad_r, pad_b), fill=1.0)

        # RGB->BGR, and normalize [-1, 1]
        t = t[[2, 1, 0], :, :]
        t = TF.normalize(t, mean=[0.5, 0.5, 0.5], std=[0.5, 0.5, 0.5])
        return t

    def get_tag_probabilities(self, feature: np.ndarray) -> np.ndarray | None:
        if self.tag_weight is None or self.tag_bias is None:
            return None
        logits = feature @ self.tag_weight.T + self.tag_bias.reshape(1, -1)
        probs = 1.0 / (1.0 + np.exp(-logits))
        return probs.astype(np.float32, copy=False)


tag_engine = TagEngine()
