from __future__ import annotations

import argparse
import logging
import time
from typing import Optional

from app.core.logger import setup_logging
from app.db import get_session
from app.pipeline import (
    ingest_feedbacks,
    ingest_feedbacks_archive,
    generate_answers,
    send_to_wb,
)
from app.clients.genai_client import get_model  # get_model(token_override=...)

log = logging.getLogger("app.run_pipeline")

# --- Параметры ожидания при квоте (если захочешь — поменяешь здесь) ---
MAX_SLEEP_ONCE = 20.0   # максимум спим за раз (сек)
BASE_SLEEP = 5.0        # если retry_after нет — спим BASE_SLEEP


def cmd_ingest() -> None:
    """
    Только загрузка отзывов из WB в БД (новые, +архив за 7 дней).
    """
    with get_session() as s:
        c1 = ingest_feedbacks(s)
        c2 = ingest_feedbacks_archive(s)
    print(f"✅ ingest done: regular={c1}, archive={c2}")


def cmd_generate(token: Optional[str]) -> None:
    """
    Генерация ответов для всех Feedback со статусом loaded и оценкой 5.
    Работает в цикле: пока есть, что генерировать.
    """
    if token:
        model = get_model(token_override=token)
        print(f"🧠 Using explicit Gemini token ...{token[-4:]}")
    else:
        model = get_model()
        print("🧠 Using GEMINI_TOKEN from .env")

    total_made = 0
    round_no = 0

    while True:
        round_no += 1
        with get_session() as s:
            made_fb, retry_after = generate_answers(s, model)

        total_made += made_fb

        if made_fb > 0:
            print(f"🧠 Round {round_no}: generated {made_fb} answers (total={total_made})")
            # маленькая пауза, чтобы не долбить базу
            time.sleep(0.5)

        # если квота — ждём и пробуем ещё раз
        if retry_after:
            delay = float(retry_after) if retry_after > 0 else BASE_SLEEP
            delay = min(delay, MAX_SLEEP_ONCE)
            log.warning("Gemini quota → sleeping %.1fs", delay)
            print(f"⏳ Quota → sleeping {delay:.1f}s")
            time.sleep(delay)
            # после сна продолжаем while True
            continue

        # если нет квоты и ничего не сгенерили — выходим
        if made_fb == 0 and not retry_after:
            break

    print(f"✅ generation done: total answers={total_made}")


def cmd_send() -> None:
    """
    Отправка всех сгенерированных ответов (status=generated) в WB.
    """
    with get_session() as s:
        sent_fb, _ = send_to_wb(s)
    print(f"📤 send done: sent={sent_fb}")


def cmd_all(token: Optional[str]) -> None:
    """
    Полный цикл: ingest → generate → send.
    """
    print("▶ Step 1: ingest feedbacks")
    cmd_ingest()

    print("\n▶ Step 2: generate answers")
    cmd_generate(token)

    print("\n▶ Step 3: send to WB")
    cmd_send()

    print("\n🎉 Pipeline finished")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Armoule WB pipeline (без scheduler, ручной запуск)."
    )

    sub = parser.add_subparsers(dest="command", required=True)

    # ingest
    p_ingest = sub.add_parser("ingest", help="Загрузить новые отзывы из WB в БД")
    p_ingest.set_defaults(func=lambda args: cmd_ingest())

    # generate
    p_gen = sub.add_parser("generate", help="Сгенерировать ответы для отзывов")
    p_gen.add_argument(
        "--token",
        dest="token",
        help="Gemini API ключ (если не указать — возьмётся GEMINI_TOKEN из .env)",
    )
    p_gen.set_defaults(func=lambda args: cmd_generate(args.token))

    # send
    p_send = sub.add_parser("send", help="Отправить сгенерированные ответы в WB")
    p_send.set_defaults(func=lambda args: cmd_send())

    # all
    p_all = sub.add_parser("all", help="Полный цикл: ingest → generate → send")
    p_all.add_argument(
        "--token",
        dest="token",
        help="Gemini API ключ (если не указать — возьмётся GEMINI_TOKEN из .env)",
    )
    p_all.set_defaults(func=lambda args: cmd_all(args.token))

    return parser


def main() -> None:
    setup_logging()
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
