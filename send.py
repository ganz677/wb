from __future__ import annotations

from datetime import datetime

from app.core.logger import setup_logging
from app.db import get_session
from app.pipeline import send_to_wb


setup_logging()


def main():
    print(f"[{datetime.now()}] Sending to WB...")
    with get_session() as s:
        fb, q = send_to_wb(s)
    print(f"✅ Sent to WB: feedbacks={fb}, questions={q}")


if __name__ == "__main__":
    main()
