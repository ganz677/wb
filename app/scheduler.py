# app/cron/scheduler.py

from __future__ import annotations
import logging
import time
from datetime import datetime
from typing import List, Tuple

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from pytz import timezone

from app.core.logger import setup_logging
from app.core.config import settings
from app.db import get_session
from app.pipeline import ingest_feedbacks, generate_answers, send_to_wb, ingest_feedbacks_archive
from app.clients.genai_client import get_model

log = logging.getLogger("app.scheduler")
MOSCOW = timezone("Europe/Moscow")


def get_gemini_tokens() -> List[str]:
    raw = getattr(settings.api_keys, "GEMINI_TOKENS", None)
    single = getattr(settings.api_keys, "GEMINI_TOKEN", None)
    tokens = [t.strip() for t in (raw or "").split(",") if t and t.strip()]
    if not tokens and single:
        tokens = [single]
    return tokens


def _run_ingest() -> Tuple[int, int]:
    with get_session() as s:
        c1 = ingest_feedbacks(s)
        c2 = ingest_feedbacks_archive(s)
    print(f"✅ Ingest regular, archive: {c1}, {c2}")
    return c1, c2


def _run_send() -> int:
    with get_session() as s:
        fb, _ = send_to_wb(s)
    print(f"📤 Sent to WB: feedbacks={fb}")
    return fb


def _generate_with_token(token: str) -> Tuple[int, float | None]:
    model = get_model(token_override=token)
    with get_session() as s:
        fb, retry_after = generate_answers(s, model)
    print(f"🧠 Generate (token=...{token[-4:]}): fb={fb}{' | quota' if retry_after else ''}")
    return fb, retry_after


def _run_generate_auto_switch(tokens: List[str], start_index: int = 0):
    if not tokens:
        print("❌ No Gemini tokens configured")
        return

    n = len(tokens)
    i = start_index % n
    no_progress_streak = 0

    while True:
        tok = tokens[i]
        fb, retry_after = _generate_with_token(tok)

        if retry_after:
            i = (i + 1) % n
            no_progress_streak = 0
            continue

        if fb > 0:
            no_progress_streak = 0
            time.sleep(0.5)
            continue

        no_progress_streak += 1
        if no_progress_streak >= n:
            break
        i = (i + 1) % n


def _pipeline_once(token_index_for_slot: int):
    tokens = get_gemini_tokens()
    if not tokens:
        print("❌ No Gemini tokens configured")
        return
    if token_index_for_slot >= len(tokens):
        print(f"⚠️ Not enough tokens in .env (need at least {token_index_for_slot+1})")
        return

    start = datetime.now(MOSCOW)
    masked = [f"...{t[-4:]}" for t in tokens]
    print(f"[{start:%Y-%m-%d %H:%M:%S %Z}] ▶️ pipeline start | tokens={masked} | start_index={token_index_for_slot}")

    _run_ingest()

    _run_generate_auto_switch(tokens, start_index=token_index_for_slot)

    _run_send()

    end = datetime.now(MOSCOW)
    print(f"[{end:%Y-%m-%d %H:%M:%S %Z}] ✅ pipeline done")


def job_pipeline_slot1():
    _pipeline_once(token_index_for_slot=0)


def job_pipeline_slot2():
    _pipeline_once(token_index_for_slot=1)


def job_pipeline_slot3():
    _pipeline_once(token_index_for_slot=2)


def main():
    setup_logging()
    sched = BlockingScheduler(timezone=MOSCOW)

    sched.add_job(job_pipeline_slot1, CronTrigger(hour=14, minute=22))
    sched.add_job(job_pipeline_slot2, CronTrigger(hour=15, minute=5))
    sched.add_job(job_pipeline_slot3, CronTrigger(hour=21, minute=5))

    print("⏱  APScheduler started (Europe/Moscow). Jobs:")
    print("   14:17 — start with token #1, auto-switch on quota")
    print("   15:05 — start with token #2, auto-switch on quota")
    print("   21:05 — start with token #3, auto-switch on quota")

    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        print("🛑 Scheduler stopped")


if __name__ == "__main__":
    main()
