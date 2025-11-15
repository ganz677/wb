from __future__ import annotations

import time
from datetime import datetime

from app.core.logger import setup_logging
from app.db import get_session
from app.pipeline import generate_answers


setup_logging()


def main():
    print(f"[{datetime.now()}] Generating answers...")

    while True:
        with get_session() as s:
            fb, q, retry_after = generate_answers(s)

        print(f"Generated answers: feedbacks={fb}, questions={q}")

        if retry_after:
            sleep_s = int(retry_after) + 2
            print(f"Quota hit. Sleeping {sleep_s}s...")
            time.sleep(sleep_s)
            continue

        if fb == 0 and q == 0:
            break

        time.sleep(1.0)


if __name__ == "__main__":
    main()
