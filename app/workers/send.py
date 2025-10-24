from app.core.logger import setup_logging
from app.db import get_session
from app.pipeline import send_to_wb

setup_logging()

def main():
    from datetime import datetime
    print(f"[{datetime.now()}] Sending to WB...")
    with get_session() as s:
        fb = send_to_wb(s)
    print(f"✅ Sent to WB: feedbacks={fb}, questions=0")

if __name__ == "__main__":
    main()
