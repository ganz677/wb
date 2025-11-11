from __future__ import annotations
import logging
from typing import Any

import time
import requests


from app.core.config import settings
from app.utils import rate

log = logging.getLogger("app.clients.wb_client")


class WBClient:
    def __init__(self, token: str | None = None, base: str | None = None) -> None:
        self.base = (base or settings.api_keys.WB_BASE_URL).rstrip("/")
        self.s = requests.Session()
        self.s.headers.update({"Authorization": f"{token or settings.api_keys.WB_TOKEN}"})

    def list_feedbacks_archive(
            self,
            *,
            take: int,
            skip: int,
            order: str | None = None,
            nm_id: int | None = None,
            timeout: int = 30,
    ) -> dict[str, Any]:
        rate.wait()
        take = max(1, min(int(take), 5000))
        skip = max(0, int(skip))

        params: dict[str, Any] = {"take": take, "skip": skip}
        if order is not None:
            if order not in {"dateAsc", "dateDesc"}:
                raise ValueError("order must be 'dateAsc' or 'dateDesc'")
            params["order"] = order
        if nm_id is not None:
            params["nmId"] = int(nm_id)

        url = f"{self.base}/feedbacks/archive"

        backoff = 1.0
        for attempt in range(5):
            resp = self.s.get(url, params=params, timeout=timeout)
            if resp.status_code == 204:
                return {}
            if resp.status_code in (429, 500, 502, 503, 504):
                log.warning(
                    "WB archive rate/5xx, retrying",
                    extra={"url": url, "status": resp.status_code, "params": params, "attempt": attempt + 1},
                )
                time.sleep(backoff)
                backoff = min(backoff * 2, 16)
                continue
            try:
                resp.raise_for_status()
            except requests.HTTPError as e:
                self._log_http_error("WB list_feedbacks_archive failed", url, params, resp, e)
                raise
            return resp.json() or {}

        resp.raise_for_status()
        return {}


    def list_feedbacks(self, *, is_answered: bool, take: int, skip: int) -> dict[str, Any]:
        rate.wait()
        params = {"isAnswered": str(is_answered).lower(), "take": take, "skip": skip}
        resp = self.s.get(f"{self.base}/feedbacks", params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def send_feedback_answer(self, feedback_id: int | str, text: str) -> dict[str, Any]:
        rate.wait()
        payload = {"id": str(feedback_id), "text": text}
        url = f"{self.base}/feedbacks/answer"
        resp = self.s.post(url, json=payload, timeout=30)
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            self._log_http_error("WB feedback answer failed", url, payload, resp, e)
            raise
        return resp.json() if resp.content else {"ok": True}

    def _log_http_error(
        self,
        msg: str,
        url: str,
        payload: dict[str, Any],
        resp: requests.Response,
        exc: requests.HTTPError,
    ) -> None:
        try:
            body = resp.json()
        except Exception:
            body = {"raw": resp.text[:1000]}
        log.warning(
            msg,
            extra={
                "url": url,
                "status": resp.status_code,
                "payload": payload,
                "response": body,
            },
        )
