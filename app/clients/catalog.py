from __future__ import annotations
import os
import re
from typing import Tuple, Dict, List, Optional

import pandas as pd
import numpy as np

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

_XLSX_ENV = "ARMOULE_CATALOG_XLSX"


_cache_map: Dict[str, str] | None = None
_cache_titles: List[str] | None = None
_cache_titles_norm: List[str] | None = None


_df_cache: pd.DataFrame | None = None
_vec_cache: TfidfVectorizer | None = None
_matrix_cache: Optional[np.ndarray] = None

SYS_TITLE = "__title__"
SYS_DESC = "__desc__"
SYS_ID = "__id__"


def _read_xlsx() -> pd.DataFrame:
    path = os.getenv(_XLSX_ENV)
    if not path:
        raise FileNotFoundError(
            f"Env {_XLSX_ENV} is not set. Set it to full path of your Excel file."
        )
    df = pd.read_excel(path, sheet_name=0)
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _norm_colname(s: str) -> str:
    return (
        s.lower()
        .replace("\n", " ")
        .replace("\r", " ")
        .replace("  ", " ")
        .strip()
    )


def _pick_cols_by_alias(df: pd.DataFrame) -> tuple[Optional[str], Optional[str], Optional[str]]:
    norm2orig = {_norm_colname(c): c for c in df.columns}

    id_aliases = [
        "nm_id", "nm id", "nmid", "nm", "nm id wb",
        "артикул wb", "артикул", "код wb", "код",
        "wb id", "wb артикул", "id", "ид", "номер товара", "номер карточки",
        "n mid", "nm- id", "nm-id",
    ]
    title_aliases = [
        "название wb", "наименование", "название", "product name",
        "title", "name", "card name", "товар", "наименование товара",
    ]
    desc_aliases = [
        "описание", "description", "desc", "описание товара", "описание wb",
        "описание продукта", "product description"
    ]

    id_col = next((norm2orig[a] for a in id_aliases if a in norm2orig), None)
    title_col = next((norm2orig[a] for a in title_aliases if a in norm2orig), None)
    desc_col = next((norm2orig[a] for a in desc_aliases if a in norm2orig), None)

    return id_col, title_col, desc_col


def _looks_like_nmid_series(s: pd.Series) -> bool:
    vals = s.dropna().astype(str).str.strip()
    if len(vals) < 10:
        return False
    only_digits = vals.str.fullmatch(r"\d{6,12}")
    score = only_digits.mean()
    uniq_ratio = vals.nunique() / max(len(vals), 1)
    return (score >= 0.7) and (uniq_ratio >= 0.5)


def _score_as_title_series(s: pd.Series) -> float:
    vals = s.dropna().astype(str).str.strip()
    if len(vals) == 0:
        return 0.0
    is_texty = vals.str.contains(r"[A-Za-zА-Яа-яЁё]", regex=True)
    only_digits = vals.str.fullmatch(r"\d+")
    mean_len = vals.str.len().mean()
    score = is_texty.mean() * 2.0 + (mean_len / 50.0) - only_digits.mean()
    return float(score)


def _infer_cols(df: pd.DataFrame) -> tuple[Optional[str], Optional[str], Optional[str]]:
    id_candidates = []
    for c in df.columns:
        try:
            if _looks_like_nmid_series(df[c]):
                id_candidates.append(c)
        except Exception:
            continue
    id_col = id_candidates[0] if id_candidates else None

    title_scores = []
    for c in df.columns:
        if df[c].dtype == object or pd.api.types.is_string_dtype(df[c]):
            title_scores.append((c, _score_as_title_series(df[c])))
    title_scores.sort(key=lambda x: x[1], reverse=True)
    title_col = title_scores[0][0] if title_scores and title_scores[0][1] > 0 else None

    desc_col = None
    if title_scores and len(title_scores) > 1:
        for c, sc in title_scores[1:]:
            if sc > 0 and c != title_col:
                desc_col = c
                break

    return id_col, title_col, desc_col


def load_available() -> Tuple[Dict[str, str], List[str]]:
    global _cache_map, _cache_titles, _cache_titles_norm
    global _df_cache, _vec_cache, _matrix_cache

    if _cache_map is not None and _cache_titles is not None:
        return _cache_map, _cache_titles

    df = _read_xlsx()
    id_col, title_col, desc_col = _pick_cols_by_alias(df)
    if id_col is None or title_col is None:
        inf_id, inf_title, inf_desc = _infer_cols(df)
        id_col = id_col or inf_id
        title_col = title_col or inf_title
        desc_col = desc_col or inf_desc

    if id_col is None:
        raise KeyError("Не найдена колонка с nm_id/артикулом в каталоге")
    if title_col is None:
        raise KeyError("Не найдена колонка с названием товара в каталоге")

    use_df = df[[id_col, title_col] + ([desc_col] if desc_col else [])].dropna(subset=[id_col, title_col])
    use_df[id_col] = use_df[id_col].astype(str).str.strip()
    use_df[title_col] = use_df[title_col].astype(str).str.strip()
    if desc_col:
        use_df[desc_col] = use_df[desc_col].astype(str).fillna("").str.strip()
    else:
        use_df[desc_col] = ""

    use_df = use_df[(use_df[id_col] != "") & (use_df[title_col] != "")]

    mapping = dict(zip(use_df[id_col].tolist(), use_df[title_col].tolist()))
    titles = sorted(list(set(use_df[title_col].tolist())), key=lambda s: s.lower())

    _cache_map = mapping
    _cache_titles = titles
    _cache_titles_norm = [_normalize_title(t) for t in titles]

    _df_cache = pd.DataFrame({
        SYS_ID: use_df[id_col].tolist(),
        SYS_TITLE: use_df[title_col].tolist(),
        SYS_DESC: use_df[desc_col].astype(str).tolist()
    })

    _vec_cache = TfidfVectorizer(max_features=6000)
    _matrix_cache = _vec_cache.fit_transform(_clean_series(_df_cache[SYS_DESC]))

    return _cache_map, _cache_titles


def name_by_nm_id(nm_id: int | str | None, mapping: Dict[str, str] | None = None) -> str | None:
    if nm_id is None:
        return None
    if mapping is None:
        mapping, _ = load_available()
    return mapping.get(str(nm_id))


def titles_pool(mapping: Dict[str, str] | None = None) -> List[str]:
    if mapping is None:
        mapping, titles = load_available()
        return titles
    return sorted(list(set(mapping.values())), key=lambda s: s.lower())


def names_only() -> List[str]:
    _, titles = load_available()
    return titles

_STOP_WORDS = {
    "духи", "парфюм", "парфюмерная", "вода", "масляные", "масляный", "аромат",
    "edp", "edt", "cologne", "парф", "ml", "мл"
}
_BRAND_WORDS = {
    "armoule","tom","ford","zara","chanel","dior","hermes","ysl","jo","malone",
    "mancera","montale","kenzo","gucci","burberry","versace","lancome","creed",
    "kilian","valentino","givenchy","prada","loewe","byredo","zadig","voltaire"
}
_WORD_RE = re.compile(r"[a-zA-Zа-яА-ЯёЁ']+", re.UNICODE)


def _normalize_title(s: str) -> str:
    s = s.lower()
    s = re.sub(r"\b\d+\s*(ml|мл)\b", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _tokens(s: str) -> set[str]:
    toks: list[str] = []
    for w in _WORD_RE.findall(s):
        w = w.strip().lower()
        if not w or w in _STOP_WORDS:
            continue
        toks.append(w)
    toks = [t for t in toks if t not in _BRAND_WORDS]
    return set(toks)


def _score_titles(a: str, b: str) -> float:
    from difflib import SequenceMatcher
    an, bn = _normalize_title(a), _normalize_title(b)
    ta, tb = _tokens(an), _tokens(bn)
    if not ta or not tb:
        j = 0.0
    else:
        inter = len(ta & tb)
        union = len(ta | tb)
        j = inter / union if union else 0.0
    ratio = SequenceMatcher(None, an, bn).ratio()  # 0..1
    bonus = 0.1 if (ta & tb) else 0.0
    return 0.6 * j + 0.4 * ratio + bonus


def _clean_series(s: pd.Series) -> pd.Series:
    return (
        s.astype(str)
         .str.replace(r"[^A-Za-zА-Яа-яЁё0-9\s]", " ", regex=True)
         .str.replace(r"\s+", " ", regex=True)
         .str.lower()
         .str.strip()
    )


def similar_titles(query_title: str, catalog_titles: List[str], k: int = 3) -> List[str]:
    if not query_title or not catalog_titles:
        return []

    load_available()

    assert _df_cache is not None
    assert _vec_cache is not None
    assert _matrix_cache is not None

    q_mask = _df_cache[SYS_TITLE].astype(str).str.lower().str.strip() == query_title.lower().strip()
    if q_mask.any():
        q_desc = _df_cache.loc[q_mask, SYS_DESC].astype(str)
        if not q_desc.empty and q_desc.iloc[0].strip():
            vec_q = _vec_cache.transform(_clean_series(q_desc))
            sims = cosine_similarity(vec_q, _matrix_cache)[0]

            idx_all = np.argsort(sims)[::-1]
            qn = _normalize_title(query_title)
            out: list[str] = []
            seen = set()
            for i in idx_all:
                title_i = str(_df_cache.iloc[i][SYS_TITLE]).strip()
                if _normalize_title(title_i) == qn:
                    continue
                key = title_i.lower()
                if key in seen:
                    continue
                seen.add(key)
                out.append(title_i)
                if len(out) >= k:
                    break
            return out

    qn = _normalize_title(query_title)
    candidates = [t for t in catalog_titles if _normalize_title(t) != qn]
    scored = [(t, _score_titles(query_title, t)) for t in candidates]
    scored.sort(key=lambda x: x[1], reverse=True)

    result: list[str] = []
    seen = set()
    for t, _ in scored:
        key = t.strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(t)
        if len(result) >= k:
            break
    return result


def similar_by_nm_id(nm_id: int | str | None, k: int = 3) -> List[str]:
    mapping, titles = load_available()
    title = name_by_nm_id(nm_id, mapping)
    if not title:
        return []
    return similar_titles(title, titles, k=k)
