from __future__ import annotations

import logging
import time
from datetime import datetime

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from pytz import timezone

from app.core.logger import setup_logging
from app.core.config import settings
from app.db import get_session
from app.pipeline import ingest_feedbacks, ingest_feedbacks_archive, generate_answers, send_to_wb
from app.clients.genai_client import get_model

log = logging.getLogger("app.scheduler")
MOSCOW = timezone("Europe/Moscow")

# спим поменьше, чем раньше
MAX_SLEEP_ONCE = 15.0      # максимум 15 секунд за один раз


# --------- токены ---------

def get_gemini_tokens() -> list[str]:
    """
    Берём список токенов из .env:

      APP__API_KEYS__GEMINI_TOKENS=key1,key2,key3

    Если он не задан – fallback на один APP__API_KEYS__GEMINI_TOKEN.
    """
    raw = getattr(settings.api_keys, "GEMINI_TOKENS", None)
    single = getattr(settings.api_keys, "GEMINI_TOKEN", None)

    tokens = [t.strip() for t in (raw or "").split(",") if t and t.strip()]
    if not tokens and single:
        tokens = [single]

    return tokens


def _pick_token_for_slot(slot: str, tokens: list[str]) -> str:
    """
    slot1 → токен #1
    slot2 → токен #2 (или #1, если только один)
    slot3 → токен #3 (или последний, если <3)
    """
    if not tokens:
        raise RuntimeError("No Gemini tokens configured")

    if slot == "slot1":
        return tokens[0]

    if slot == "slot2":
        if len(tokens) >= 2:
            return tokens[1]
        return tokens[0]

    if slot == "slot3":
        if len(tokens) >= 3:
            return tokens[2]
        return tokens[-1]

    # на всякий случай
    return tokens[0]


# --------- шаги пайплайна ---------

def _run_ingest() -> tuple[int, int]:
    with get_session() as s:
        c1 = ingest_feedbacks(s)
        c2 = ingest_feedbacks_archive(s)
    print(f"✅ ingest regular={c1}, archive={c2}")
    return c1, c2


def _run_generate_loop(model) -> None:
    """
    Старое поведение, как ты показал:

    while True:
        - вызываем generate_answers(...)
        - если что-то сгенерили → продолжаем сразу
        - если квота → немного спим и пробуем ещё
        - выходим, когда made_fb == 0 и retry_after нет

    ВАЖНО: здесь НЕТ ограничения по общему времени сна — будет крутиться,
    пока либо:
      • всё не сгенерит,
      • либо Gemini весь день честно даёт 429 (тогда это уже реальный лимит).
    """
    while True:
        with get_session() as s:
            made_fb, retry_after = generate_answers(s, model)

        # если что-то сгенерили — чуть подышим, чтобы не долбить базу
        if made_fb > 0:
            time.sleep(0.5)

        # если Gemini вернул retry_after — чуть подождём и повторим
        if retry_after:
            try:
                delay = float(retry_after)
            except Exception:
                delay = MAX_SLEEP_ONCE

            delay = min(delay, MAX_SLEEP_ONCE)

            # логируем только ощутимые паузы
            if delay >= 5:
                log.warning("Gemini quota → sleeping %.0fs", delay)
                print(f"⏳ Quota → sleeping {int(delay)}s")

            time.sleep(delay)
            continue

        # нет квоты и ничего не сгенерили → всё готово
        if made_fb == 0 and not retry_after:
            break


def _run_send() -> int:
    with get_session() as s:
        fb, _ = send_to_wb(s)
    print(f"📤 Sent to WB: feedbacks={fb}")
    return fb


def _pipeline_once(slot: str):
    tokens = get_gemini_tokens()
    if not tokens:
        print("❌ No Gemini tokens configured")
        return

    try:
        token = _pick_token_for_slot(slot, tokens)
    except RuntimeError as e:
        print(f"❌ {e}")
        return

    start = datetime.now(MOSCOW)
    print(f"[{start:%Y-%m-%d %H:%M:%S %Z}] ▶ pipeline start | slot={slot} | token=...{token[-4:]}")

    # 1) тянем отзывы
    _run_ingest()

    # 2) цикл генерации до победного (или пока Gemini реально не станет в бетон)
    model = get_model(token_override=token)
    _run_generate_loop(model)

    # 3) отправляем ответы в WB
    _run_send()

    end = datetime.now(MOSCOW)
    print(f"[{end:%Y-%m-%d %H:%M:%S %Z}] ✅ pipeline done")


# --------- джобы ---------

def job_pipeline_slot1():
    # утренний слот – токен #1
    _pipeline_once("slot1")


def job_pipeline_slot2():
    # дневной слот – токен #2
    _pipeline_once("slot2")


def job_pipeline_slot3():
    # вечерний/ночной слот – токен #3
    _pipeline_once("slot3")


def main():
    setup_logging()
    sched = BlockingScheduler(timezone=MOSCOW)

    # Поставь любые часы/минуты, как тебе удобно:
    sched.add_job(job_pipeline_slot1, CronTrigger(hour=9, minute=5))
    sched.add_job(job_pipeline_slot2, CronTrigger(hour=15, minute=5))
    sched.add_job(job_pipeline_slot3, CronTrigger(hour=21, minute=3))

    print("⏱ APScheduler started (Europe/Moscow). Jobs:")
    print("   09:05 — token #1")
    print("   15:05 — token #2")
    print("   21:05 — token #3")

    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        print("🛑 Scheduler stopped")


if __name__ == "__main__":
    main()
