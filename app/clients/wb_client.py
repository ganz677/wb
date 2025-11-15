from __future__ import annotations

import logging
import time
from typing import Any

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
                    extra={
                        "url": url,
                        "status": resp.status_code,
                        "params": params,
                        "attempt": attempt + 1,
                    },
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

    def list_questions(self, *, is_answered: bool, take: int, skip: int) -> dict[str, Any]:
        rate.wait()
        params = {"isAnswered": str(is_answered).lower(), "take": take, "skip": skip}
        resp = self.s.get(f"{self.base}/questions", params=params, timeout=30)
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

    def send_question_answer(self, question_id: int | str, text: str) -> dict[str, Any]:
        rate.wait()
        url = f"{self.base}/questions"

        payload_a = {"id": str(question_id), "state": "wbRu", "answer": {"text": text}}
        resp = self.s.patch(url, json=payload_a, timeout=30)
        if 200 <= resp.status_code < 300:
            return resp.json() if resp.content else {"ok": True}

        try:
            body = resp.json()
            err_text = (body or {}).get("errorText", "") or ""
        except Exception:
            err_text = resp.text or ""

        if resp.status_code == 400 and (
            "Empty state" in err_text or "Неправильный текст ответа" in err_text
        ):
            payload_b = {"id": str(question_id), "state": "wbRu", "text": text}
            resp_b = self.s.patch(url, json=payload_b, timeout=30)
            if 200 <= resp_b.status_code < 300:
                return resp_b.json() if resp_b.content else {"ok": True}
            try:
                resp_b.raise_for_status()
            except requests.HTTPError as e2:
                self._log_http_error("WB question answer failed (fallback B)", url, payload_b, resp_b, e2)
                raise

        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            self._log_http_error("WB question answer failed", url, payload_a, resp, e)
            raise

        return resp.json() if resp.content else {"ok": True}

    def reject_question(self, question_id: int | str) -> dict[str, Any]:
        rate.wait()
        url = f"{self.base}/questions"
        payload = {"id": str(question_id), "state": "none"}

        resp = self.s.patch(url, json=payload, timeout=30)
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            self._log_http_error("WB question reject failed", url, payload, resp, e)
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
