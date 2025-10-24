from __future__ import annotations

import time
import logging
import re
from datetime import datetime, timedelta, timezone

from typing import Tuple, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db import Feedback, Question, Status
from app.core.config import settings
from app.clients import WBClient, get_model, make_answer, AnswerInput
from app.clients.catalog import load_available, name_by_nm_id, titles_pool, similar_titles

log = logging.getLogger("app.pipeline")

client = WBClient()
_model = None


def ensure_model():
    global _model
    if _model is None:
        _model = get_model()


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

def _surrogate_question_text(text: str | None) -> str:
    if text and text.strip():
        return text.strip()
    return "Вопрос без текста."


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

def ingest_questions(session: Session) -> int:
    skip = 0
    inserted = 0
    take = settings.api_keys.TAKE
    while True:
        data = client.list_questions(is_answered=False, take=take, skip=skip) or {}
        items = (data.get("data") or {}).get("questions") or []
        if not items:
            break
        for q in items:
            wb_id = q.get("id")
            if not wb_id:
                continue
            exists = session.scalar(select(Question).where(Question.wb_id == wb_id))
            if exists:
                continue
            pd = q.get("productDetails") or {}
            created_raw = q.get("createdDate")
            qo = Question(
                wb_id=wb_id,
                nm_id=pd.get("nmId"),
                text=q.get("text") or "",
                created_at_wb=_iso_to_dt(created_raw) if created_raw else datetime.utcnow(),
                status=Status.loaded,
            )
            session.add(qo)
            inserted += 1
        session.commit()
        skip += take

    log.info("Ingested questions: %s", inserted, extra={"event": "ingest_questions", "questions": inserted})
    return inserted


def generate_answers(session: Session) -> Tuple[int, int, Optional[float]]:
    ensure_model()
    made_fb = 0
    made_q = 0
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
            _model,
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

    if retry_after_max:
        return made_fb, made_q, retry_after_max

    TRIGGERS = ("альтернатива", "похож", "заменить", "совет", "рекоменд", "что взять", "какой")
    def _need_recommendation(txt: str) -> bool:
        t = (txt or "").lower()
        return any(w in t for w in TRIGGERS)

    for q in session.scalars(select(Question).where(Question.status == Status.loaded)):
        start_time = time.time()
        text_in = _surrogate_question_text(q.text)

        preferred = []
        product_title = ""
        exclude: list[str] = []

        if q.nm_id:
            product_title = (name_by_nm_id(q.nm_id, available_map) or "").strip()
            if product_title:
                exclude = [product_title]
                if _need_recommendation(text_in):
                    preferred = similar_titles(product_title, available_titles_list, k=3)

        text, retry_after = make_answer(
            _model,
            AnswerInput(kind="question", text=text_in, product_name=product_title),
            available_titles=available_titles_list,
            preferred_titles=preferred,
            exclude_titles=exclude,
        )

        duration = round(time.time() - start_time, 2)
        if retry_after:
            retry_after_max = max(retry_after_max or 0.0, retry_after)
            break

        if not text:
            continue

        q.answer_text = text
        q.status = Status.generated
        session.add(q)
        session.commit()
        made_q += 1

        log.info(
            "Generated answer for question %s",
            q.wb_id,
            extra={"event": "question_generated", "elapsed": duration}
        )

    return made_fb, made_q, retry_after_max


# --- Отправка в WB ---
def send_to_wb(session: Session) -> tuple[int, int]:
    sent_fb = 0
    # sent_q = 0

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
        except Exception as e:
            log.warning("Failed to send feedback %s: %s", fb.wb_id, e)
            fb.status = Status.failed
            session.commit()

    # for q in session.scalars(select(Question).where(Question.status == Status.generated)):
    #     if not (q.answer_text and q.answer_text.strip()):
    #         q.status = Status.failed
    #         session.commit()
    #         continue
    #     try:
    #         client.send_question_answer(q.wb_id, q.answer_text)
    #         q.status = Status.sent
    #         session.commit()
    #         sent_q += 1
    #     except Exception as e:
    #         log.warning("Failed to send question %s: %s", q.wb_id, e)
    #         q.status = Status.failed
    #         session.commit()

    return sent_fb #sent_q


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
