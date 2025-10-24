from __future__ import annotations

import logging
import re
import time
from types import SimpleNamespace
from typing import Optional, Tuple, Sequence, List

import google.generativeai as genai
from google.api_core.exceptions import (
    ResourceExhausted,
    FailedPrecondition,
    GoogleAPICallError,
    PermissionDenied,
    NotFound,
)

from app.schemas.gemini_schemas import AnswerInput
from app.core.config import settings

log = logging.getLogger("app.clients.gemini")

PROMPT = """
Ты — голос парфюмерного бренда Armoule.  
Отвечай только на отзывы с 5 звёздами.  
Пиши спокойно, с лёгким теплом и естественной элегантностью.  
Главное — хорошо поблагодари клиента, коротко передай атмосферу аромата и мягко вдохнови его посмотреть другие ароматы бренда.  

💎 Правила:
1. Для всех отзывов — создай плавный, живой ответ в 3–4 фразы:  
   - начни с искренней благодарности с упоминанием имени покупателя;
   - коротко опиши купленный аромат как ощущение (не технически);  
   - мягко пригласи заглянуть в профиль Armoule;  
   - добавь 2–3 рекомендации из “ДОСТУПНЫЕ АРОМАТЫ” (в первую очередь — из “ПРИОРИТЕТНЫЕ”).  
   Заверши фразой бренда, например:  
   «Armoule — вдох, который остаётся» или «Armoule — пусть аромат говорит первым».


🪶 Стиль:
- Искренний, лёгкий, профессиональный.  
- Не используй технические термины (“амбровый”, “мускусный”).  
- Не упоминай «наш бренд» или «наш продукт».  
- Избегай штампов вроде “мы ценим ваш отзыв”.  
- Не будь излишне эмоциональным — речь должна звучать естественно.  
- Длина:  
  • отзыв → 3–4 фразы + 2–3 рекомендации + слоган;  

📘 Пример:
Благодарим <ИМЯ> вас за тёплый отзыв — такие слова вдохновляют нас создавать ещё больше красоты.  
Intense Cafe — аромат, в котором уют встречает уверенность: сладкий шлейф кофе и мягкое сияние тепла.  
Если этот характер вам близок, загляните к Armoule — там живут ароматы с настроением.  
🔹 Climat — белые цветы и чистый холодный свет  
🔹 Euphoria — бархат радости, шлейф летнего утра  
🔹 Angel’s Share — густое тепло, будто вечер в янтаре  
Armoule — пусть аромат говорит первым.
""".strip()


UNICODE_DASHES_RE = re.compile(r"[\u2010-\u2015\u2212\uFE58\uFE63\uFF0D]")
def _normalize_model_name(name: str) -> str:
    return UNICODE_DASHES_RE.sub("-", name or "")


FALLBACK_MODELS: List[str] = [
    "gemini-2.5-flash",
]


class _GeminiAdapter:
    def __init__(self, model_name: str, api_key: str, max_retries: int = 6, base_sleep: float = 0.6):
        self.model_name = _normalize_model_name(model_name)
        self.max_retries = max_retries
        self.base_sleep = base_sleep

        genai.configure(
            api_key=api_key,
            transport="rest",
        )

        self.model = genai.GenerativeModel(
            model_name=self.model_name,
            system_instruction= PROMPT,
        )

    def _swap_model(self, new_name: str) -> None:
        self.model_name = _normalize_model_name(new_name)
        self.model = genai.GenerativeModel(
            model_name=self.model_name,
            system_instruction=PROMPT,
        )

    def generate_content(self, prompt_text: str) -> SimpleNamespace:
        attempt = 0
        while True:
            try:
                out = self.model.generate_content(prompt_text)
                text = getattr(out, "text", None)
                if not text:
                    try:
                        text = out.candidates[0].content.parts[0].text
                    except Exception:
                        text = None
                return SimpleNamespace(text=(text or "").strip())

            except (ResourceExhausted, FailedPrecondition) as e:
                msg = str(e)
                if "User location is not supported" in msg:
                    log.warning("Gemini region block: %s (model=%s)", msg, self.model_name)
                    return SimpleNamespace(text="")
                attempt += 1
                delay = _extract_retry_after(msg) or (self.base_sleep * (2 ** attempt))
                if attempt > self.max_retries:
                    log.warning("Gemini precondition/exhausted: %s (model=%s)", msg, self.model_name)
                    return SimpleNamespace(text="")
                log.warning("Gemini rate/precondition; retry #%s in %.2fs; err=%s",
                            attempt, min(delay, 15.0), msg)
                time.sleep(min(delay, 15.0))

            except (PermissionDenied, NotFound) as e:
                msg = str(e)
                log.warning("Gemini permission/model error: %s (model=%s)", msg, self.model_name)
                if FALLBACK_MODELS:
                    alt = _normalize_model_name(FALLBACK_MODELS.pop(0))
                    log.warning("Switching Gemini model → %s", alt)
                    self._swap_model(alt)
                    continue
                return SimpleNamespace(text="")

            except GoogleAPICallError as e:
                msg = f"{type(e).__name__}: {e}"
                if "User location is not supported" in str(e):
                    log.warning("Gemini region block (GAE): %s (model=%s)", e, self.model_name)
                    return SimpleNamespace(text="")
                attempt += 1
                delay = _extract_retry_after(str(e)) or (self.base_sleep * (2 ** attempt))
                if attempt > self.max_retries:
                    log.warning("Gemini API error (exhausted): %s (model=%s)", msg, self.model_name)
                    return SimpleNamespace(text="")
                log.warning("Gemini API error; retry #%s in %.2fs; err=%s",
                            attempt, min(delay, 15.0), msg)
                time.sleep(min(delay, 15.0))

_RETRY_IN_RE = re.compile(r"(retry in|retry_after|retry-after)\s*:?[\s=]*([0-9]+(?:\.[0-9]+)?)", re.IGNORECASE)
_RETRY_SECONDS_BLOCK_RE = re.compile(r"retry_delay\s*\{\s*seconds:\s*([0-9]+)", re.IGNORECASE)

def _extract_retry_after(err_msg: str) -> Optional[float]:
    m = _RETRY_IN_RE.search(err_msg or "")
    if m:
        try:
            return float(m.group(2))
        except Exception:
            pass
    m = _RETRY_SECONDS_BLOCK_RE.search(err_msg or "")
    if m:
        try:
            return float(m.group(1))
        except Exception:
            pass
    return None


def _dedup_keep_order(items: Sequence[str], limit: int | None = None) -> List[str]:
    seen = set()
    out: List[str] = []
    for x in items or []:
        if not isinstance(x, str):
            continue
        t = x.strip()
        if not t:
            continue
        key = t.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(t)
        if limit is not None and len(out) >= limit:
            break
    return out

def _join_block(title: str, lines: Sequence[str]) -> str:
    data = _dedup_keep_order(lines, limit=80)
    if not data:
        return f"{title}:\n—"
    return f"{title}:\n" + "\n".join(f"- {t}" for t in data)

def _extract_title_from_bullet(line: str) -> Optional[str]:
    s = line.strip()
    if not s.startswith(("🔹", "•", "-")):
        return None
    s = s.lstrip("🔹•- ").strip()
    if not s:
        return None
    parts = re.split(r"\s+—\s+| - ", s, maxsplit=1)
    title = (parts[0] if parts else s).strip().strip("*")
    return title or None

_NO_TEXT_MARKERS = {
    "отзыв без текста.",
    "вопрос без текста.",
}

def _is_no_text_feedback(inp: AnswerInput) -> bool:
    if getattr(inp, "kind", None) != "feedback":
        return False
    t = (getattr(inp, "text", "") or "").strip().lower()
    if not t:
        return True
    if t in _NO_TEXT_MARKERS:
        return True
    return len(t) <= 2

def _pick_recos(
    preferred: Sequence[str] | None,
    available: Sequence[str] | None,
    exclude: Sequence[str] | None,
    k: int = 3,
) -> List[str]:
    excl = {e.strip().lower() for e in (exclude or []) if isinstance(e, str)}
    out: List[str] = []

    def push_many(pool: Sequence[str] | None):
        if not pool:
            return
        for t in pool:
            if not isinstance(t, str):
                continue
            s = t.strip()
            if not s:
                continue
            key = s.lower()
            if key in excl:
                continue
            if any(key == x.lower() for x in out):
                continue
            out.append(s)
            if len(out) >= k:
                break

    push_many(preferred)
    if len(out) < k:
        push_many(available)

    return out[:k]

_NO_TEXT_VARIANTS = [
    "Спасибо за доверие! Если вам близок характер «{product}», загляните в профиль Armoule — там ждут новые истории ароматов.",
    "Благодарим за высокую оценку! Если настроение «{product}» вам откликнулось, посмотрите ещё ароматы Armoule.",
    "Спасибо за 5★! Если вы полюбили «{product}», в профиле Armoule найдёте ещё несколько настроений.",
    "Радуемся вашей оценке! Если «{product}» пришёлся по душе, загляните к Armoule — там есть чем вдохновиться.",
    "Спасибо! Если понравился характер «{product}», в профиле Armoule есть и другие истории ароматов.",
    "Благодарим! Если «{product}» стал вашим настроением, присмотритесь к другим ароматам Armoule.",
    "Спасибо за вашу оценку! Если «{product}» вам близок, загляните к Armoule за новыми открытиями.",
    "Признательны за 5★! Если «{product}» понравился, в профиле Armoule вас ждут родственные настроения.",
]

def _render_no_text_reply(product: str | None, recos: Sequence[str]) -> str:
    p = (product or "").strip()
    idx = (abs(hash(p.lower())) % len(_NO_TEXT_VARIANTS)) if p else 0
    lead = _NO_TEXT_VARIANTS[idx].format(product=p or "аромат")
    lines = [lead]
    if recos:
        lines.append("🔹 " + "\n🔹 ".join(recos))
    return "\n".join(lines).strip()



def get_model():
    api = settings.api_keys
    token = getattr(api, "GEMINI_TOKEN", None)
    if not token:
        raise RuntimeError("GEMINI_TOKEN is not set")

    raw_name = getattr(api, "GEMINI_MODEL", None) or "gemini-2.5-flash"
    model_name = _normalize_model_name(raw_name)
    return _GeminiAdapter(model_name=model_name, api_key=token)


def make_answer(
    model: _GeminiAdapter,
    inp: AnswerInput,
    *,
    available_titles: Optional[Sequence[str]] = None,
    preferred_titles: Optional[Sequence[str]] = None,
    exclude_titles: Optional[Sequence[str]] = None,
) -> Tuple[Optional[str], Optional[float]]:

    try:
        if getattr(inp, "kind", None) == "feedback":
            r = getattr(inp, "rating", None)
            if r is None or int(r) != 5:
                return None, None
    except Exception:
        pass

    if _is_no_text_feedback(inp):
        product = (getattr(inp, "product_name", None) or "").strip()
        exclude = list(exclude_titles or [])
        if product:
            exclude.append(product)
        recos = _pick_recos(preferred_titles, available_titles, exclude, k=3)
        text = _render_no_text_reply(product, recos)
        return (text or None), None

    def _fmt(val):
        if not val:
            return "—"
        return str(val).strip()

    available_block = _join_block("ДОСТУПНЫЕ АРОМАТЫ", available_titles or [])
    pref = _dedup_keep_order(preferred_titles or [], limit=5)
    preferred_block = "\n\n" + _join_block("ПРИОРИТЕТНЫЕ АЛЬТЕРНАТИВЫ (используй в первую очередь)", pref) if pref else ""
    excl = _dedup_keep_order(exclude_titles or [], limit=10)
    exclude_block = "\n\n" + _join_block("НЕ РЕКОМЕНДОВАТЬ", excl) if excl else ""

    prompt_text = f"""
ВХОД:
- Тип: {inp.kind}
- Купленный аромат: { _fmt(getattr(inp, 'product_name', None)) }
- Текст клиента: { _fmt(inp.text) }
- Оценка: { inp.rating if inp.rating is not None else "—" }

ТВОЯ ЗАДАЧА:
- В ответе учитывай купленный аромат (если указан).
- При рекомендациях в первую очередь используй позиции из блока "ПРИОРИТЕТНЫЕ АЛЬТЕРНАТИВЫ".
- Бери рекомендации только из "ДОСТУПНЫЕ АРОМАТЫ".
- Не предлагай позиции из "НЕ РЕКОМЕНДОВАТЬ".
- Не повторяй одну и ту же формулировку для разных клиентов.

{available_block}{preferred_block}{exclude_block}
""".strip()

    try:
        out = model.generate_content(prompt_text)
        text = (getattr(out, "text", "") or "").strip()
        if not text:
            log.warning("Gemini вернул пустой ответ", extra={"input": str(getattr(inp, 'text', ''))[:160]})
            return None, None

        raw_lines = [line for line in text.splitlines() if not re.fullmatch(r"[🔹•\-\s]*\d+[\.]*\s*", line.strip())]
        exclude_lc = {t.lower() for t in (exclude_titles or []) if isinstance(t, str)}
        seen_titles = set()
        filtered_lines: List[str] = []
        for line in raw_lines:
            stripped = line.strip()
            title = _extract_title_from_bullet(stripped)
            if title:
                tl = title.lower()
                if tl in exclude_lc or tl in seen_titles:
                    continue
                seen_titles.add(tl)
            filtered_lines.append(line)

        cleaned = "\n".join(filtered_lines).strip()
        return (cleaned or None), None

    except (ResourceExhausted, FailedPrecondition) as e:
        ra = _extract_retry_after(str(e))
        log.warning("Gemini quota/rate", extra={"error": str(e), "retry_after": ra})
        return None, ra
    except (PermissionDenied, NotFound) as e:
        log.warning("Gemini permission/model error", extra={"error": str(e)})
        return None, None
    except GoogleAPICallError as e:
        log.warning("Gemini API error", extra={"error": str(e)})
        return None, None
    except Exception as e:
        log.warning("Gemini generation failed", extra={"error": str(e)})
        return None, None
