from __future__ import annotations
import argparse
import logging

from app.core.logger import setup_logging
from app.db import get_session
from sqlalchemy import text

log = logging.getLogger("tools.requeue_failed")

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Requeue failed items: move to generated (resend) or loaded (regen)."
    )
    parser.add_argument("--mode", choices=["resend", "regen"], default="resend",
                        help="resend=failed→generated; regen=failed→loaded+answer_text=NULL")
    parser.add_argument("--kind", choices=["all", "feedbacks", "questions"], default="all")
    args = parser.parse_args()

    setup_logging()
    log.info("Requeue failed start", extra={"mode": args.mode, "kind": args.kind})

    with get_session() as s:
        if args.kind in ("all", "feedbacks"):
            if args.mode == "resend":
                s.execute(text("UPDATE feedbacks SET status='generated' WHERE status='failed'"))
            else:
                s.execute(text("UPDATE feedbacks SET status='loaded', answer_text=NULL WHERE status='failed'"))

        if args.kind in ("all", "questions"):
            if args.mode == "resend":
                s.execute(text("UPDATE questions SET status='generated' WHERE status='failed'"))
            else:
                s.execute(text("UPDATE questions SET status='loaded', answer_text=NULL WHERE status='failed'"))

        s.commit()

    log.info("Requeue failed done")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
