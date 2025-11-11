from __future__ import annotations

import time
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Tuple, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db import Feedback, Status
from app.core.config import settings
from app.clients import WBClient, make_answer, AnswerInput
from app.clients.catalog import load_available, name_by_nm_id, titles_pool, similar_titles

log = logging.getLogger("app.pipeline")

client = WBClient()

_ISO_RE = re.compile(
    r"^(?P<date>\d{4}-\d{2}-\d{2})T"
    r"(?P<hms>\d{2}:\d{2}:\d{2})"
    r"(?:\.(?P<frac>\d+))?"
    r"(?P<tz>Z|[+\-]\d{2}:\d{2})?$"
)

def _iso_to_dt(val: str) -> datetime:
    if not val:
        raise ValueError("empty datetime string")
    m = _ISO_RE.match(val)
    if not m:
        return datetime.fromisoformat(val.replace("Z", "+00:00"))
    date = m.group("date")
    hms = m.group("hms")
    frac = m.group("frac") or ""
    tz = m.group("tz") or ""
    if tz == "Z":
        tz = "+00:00"
    if frac:
        frac = (frac[:6]).ljust(6, "0")
        iso = f"{date}T{hms}.{frac}{tz}"
    else:
        iso = f"{date}T{hms}{tz}"
    return datetime.fromisoformat(iso)

def _surrogate_feedback_text(text: str | None, rating: int | None, username: str | None) -> str:
    if text and text.strip():
        return text.strip()
    parts = ["Отзыв без текста."]
    if rating is not None:
        parts.append(f"Оценка: {rating}/5.")
    if username:
        parts.append(f"Покупатель: {username}.")
    return " ".join(parts)

def ingest_feedbacks(session: Session) -> int:
    skip = 0
    inserted = 0
    take = settings.api_keys.TAKE
    while True:
        data = client.list_feedbacks(is_answered=False, take=take, skip=skip) or {}
        items = (data.get("data") or {}).get("feedbacks") or []
        if not items:
            break
        for f in items:
            wb_id = f.get("id")
            if not wb_id:
                continue
            exists = session.scalar(select(Feedback).where(Feedback.wb_id == wb_id))
            if exists:
                continue
            pd = f.get("productDetails") or {}
            product_name = (
                pd.get("productName")
                or pd.get("name")
                or ""
            )
            created_raw = f.get("createdDate")
            fb = Feedback(
                wb_id=wb_id,
                nm_id=pd.get("nmId"),
                product_name=product_name,
                text=f.get("text") or "",
                created_at_wb=_iso_to_dt(created_raw) if created_raw else datetime.utcnow(),
                username=f.get("userName"),
                product_valuation=f.get("productValuation"),
                status=Status.loaded,
            )
            session.add(fb)
            inserted += 1
        session.commit()
        skip += take

    log.info("Inserted feedbacks: %s", inserted, extra={"event": "ingest_feedbacks", "feedbacks": inserted})
    return inserted

def generate_answers(session: Session, model) -> Tuple[int, Optional[float]]:

    made_fb = 0
    retry_after_max: Optional[float] = None

    available_map, available_titles = load_available()
    available_titles_list = titles_pool(available_map)

    for fb in session.scalars(select(Feedback).where(Feedback.status == Status.loaded)):
        if (fb.product_valuation is None) or (int(fb.product_valuation) != 5):
            continue

        start_time = time.time()

        product_title = fb.product_name or ""
        if not product_title and fb.nm_id is not None:
            product_title = name_by_nm_id(fb.nm_id, available_map) or ""

        preferred = similar_titles(product_title, available_titles_list, k=3) if product_title else []

        text, retry_after = make_answer(
            model,
            AnswerInput(
                kind="feedback",
                text=_surrogate_feedback_text(fb.text, fb.product_valuation, fb.username),
                rating=fb.product_valuation,
                product_name=product_title,
            ),
            available_titles=available_titles_list,
            preferred_titles=preferred,
            exclude_titles=[product_title] if product_title else None,
        )

        duration = round(time.time() - start_time, 2)

        if retry_after:
            retry_after_max = max(retry_after_max or 0.0, retry_after)
            log.warning("Quota hit on feedback", extra={"retry_after": retry_after})
            break

        if not text:
            continue

        fb.answer_text = text
        fb.status = Status.generated
        session.add(fb)
        session.commit()
        made_fb += 1

        log.info(
            "Generated answer for feedback %s",
            fb.wb_id,
            extra={"event": "feedback_generated", "elapsed": duration}
        )

    return made_fb, retry_after_max

def send_to_wb(session: Session) -> tuple[int, int]:
    sent_fb = 0
    for fb in session.scalars(select(Feedback).where(Feedback.status == Status.generated)):
        if not (fb.answer_text and fb.answer_text.strip()):
            fb.status = Status.failed
            session.commit()
            continue
        try:
            client.send_feedback_answer(fb.wb_id, fb.answer_text)
            fb.status = Status.sent
            session.commit()
            sent_fb += 1
        except Exception:
            fb.status = Status.failed
            session.commit()

    return sent_fb, 0

def ingest_feedbacks_archive(
    session: Session,
    *,
    order: str | None = "dateDesc",
    client: WBClient | None = None,
) -> int:
    client = client or WBClient()
    inserted = 0
    skip = 0
    take_cfg = getattr(settings.api_keys, "TAKE", 1000) or 1000
    take = min(int(take_cfg), 5000)
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)

    while True:
        data = client.list_feedbacks_archive(take=take, skip=skip, order=order) or {}
        items = (data.get("data") or {}).get("feedbacks") or []
        if not items:
            break

        page_all_older = True

        for f in items:
            ans_text = ((f.get("answer") or {}).get("text") or "").strip()
            if ans_text:
                continue

            created_raw = f.get("createdDate")
            if not created_raw:
                continue

            try:
                created_dt = _iso_to_dt(created_raw)
            except Exception:
                continue

            if created_dt.tzinfo is None:
                created_dt = created_dt.replace(tzinfo=timezone.utc)
            else:
                created_dt = created_dt.astimezone(timezone.utc)

            if created_dt < cutoff:
                continue
            else:
                page_all_older = False

            wb_id = f.get("id")
            if not wb_id:
                continue
            exists = session.scalar(select(Feedback).where(Feedback.wb_id == wb_id))
            if exists:
                continue

            pd = f.get("productDetails") or {}
            product_name = pd.get("productName") or pd.get("name") or ""
            fb = Feedback(
                wb_id=str(wb_id),
                nm_id=pd.get("nmId"),
                product_name=product_name,
                text=f.get("text") or "",
                created_at_wb=created_dt,
                username=f.get("userName"),
                product_valuation=f.get("productValuation"),
                status=Status.loaded,
            )
            session.add(fb)
            inserted += 1

        session.commit()
        skip += len(items)

        if order == "dateDesc" and page_all_older:
            break

    log.info(
        "Inserted archive feedbacks (unanswered, last 7d): %s",
        inserted,
        extra={"event": "ingest_feedbacks_archive", "feedbacks": inserted, "order": order, "window": "7d"},
    )
    return inserted
