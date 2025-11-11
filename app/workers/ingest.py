from __future__ import annotations

from datetime import datetime
import sys
import traceback

from app.core.logger import setup_logging
from app.db import get_session
from app.pipeline import ingest_feedbacks, ingest_feedbacks_archive


def main() -> int:
    setup_logging()

    started = datetime.now()
    print(f"[{started}] Ingesting archived + regular feedbacks...")

    arch_count = 0
    reg_count = 0

    with get_session() as session:
        try:
            arch_count = ingest_feedbacks_archive(session)
            print(f"Archived ingested: {arch_count}")
        except Exception as e:
            print("Archived ingest failed:", e, file=sys.stderr)
            traceback.print_exc()
        try:
            reg_count = ingest_feedbacks(session)
            print(f"✅ Regular ingested:  {reg_count}")
        except Exception as e:
            print("❌ Regular ingest failed:", e, file=sys.stderr)
            traceback.print_exc()

    total = arch_count + reg_count
    finished = datetime.now()
    print(f"[{finished}] Done. Total ingested: {total} (archived={arch_count}, regular={reg_count})")

    return 0 if (arch_count or reg_count) else 1


if __name__ == "__main__":
    sys.exit(main())
