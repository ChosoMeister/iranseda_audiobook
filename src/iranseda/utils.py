
from __future__ import annotations
import logging, time, random, functools, hashlib
from pathlib import Path
from typing import Callable, Any, Optional

def setup_logging(logfile: Path | None, level: str = "INFO") -> None:
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    # console
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.handlers = [ch]
    # file
    if logfile:
        fh = logging.FileHandler(logfile, encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)

def throttle(min_s: float, max_s: float) -> None:
    time.sleep(random.uniform(min_s, max_s))

def retry(max_attempts: int = 3, base_delay: float = 0.5, factor: float = 2.0, jitter: float = 0.2):
    """Decorator: retry with exponential backoff + jitter."""
    def deco(fn: Callable[..., Any]):
        @functools.wraps(fn)
        def wrapped(*args, **kwargs):
            attempt = 0
            delay = base_delay
            while True:
                try:
                    return fn(*args, **kwargs)
                except Exception as e:
                    attempt += 1
                    if attempt >= max_attempts:
                        raise
                    sleep_for = delay + random.uniform(0, jitter)
                    logging.getLogger().warning(f"{fn.__name__}: attempt {attempt} failed ({e}); retrying in {sleep_for:.2f}s")
                    time.sleep(sleep_for)
                    delay *= factor
        return wrapped
    return deco

class DiskCache:
    def __init__(self, base: Path):
        self.base = Path(base)
        self.base.mkdir(parents=True, exist_ok=True)

    def key_for(self, url: str) -> Path:
        h = hashlib.sha1(url.encode("utf-8")).hexdigest()
        return self.base / f"{h}.html"

    def get(self, url: str) -> Optional[str]:
        p = self.key_for(url)
        if p.exists():
            try:
                return p.read_text(encoding="utf-8")
            except Exception:
                return None
        return None

    def set(self, url: str, html: str) -> None:
        p = self.key_for(url)
        p.write_text(html, encoding="utf-8")
