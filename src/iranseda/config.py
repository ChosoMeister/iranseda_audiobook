
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import yaml

@dataclass
class Outputs:
    ids_csv: str = "audiobooks.csv"
    books_csv: str = "books_with_attid.csv"
    errors_csv: str = "errors.csv"
    jsonl: str = "books_with_attid.jsonl"

@dataclass
class Throttle:
    min: float = 0.1
    max: float = 0.3

@dataclass
class Filters:
    min_mp3_size_bytes: int = 0
    require_full_mp3: bool = False

@dataclass
class Parallel:
    workers: int = 2

@dataclass
class Logging:
    level: str = "INFO"
    file: str | None = "iranseda.log"

@dataclass
class CacheCfg:
    enabled: bool = True
    dir: str = ".cache/details"

@dataclass
class Settings:
    start_url_template: str
    pages: int
    outputs: Outputs
    throttle: Throttle
    filters: Filters
    parallel: Parallel
    logging: Logging
    cache: CacheCfg

def load_config(path: str | Path) -> Settings:
    data = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    outputs = Outputs(**(data.get("outputs") or {}))
    throttle = Throttle(**(data.get("throttle") or {}))
    filters = Filters(**(data.get("filters") or {}))
    parallel = Parallel(**(data.get("parallel") or {}))
    logging = Logging(**(data.get("logging") or {}))
    cache = CacheCfg(**(data.get("cache") or {}))
    return Settings(
        start_url_template=data["start_url_template"],
        pages=int(data["pages"]),
        outputs=outputs,
        throttle=throttle,
        filters=filters,
        parallel=parallel,
        logging=logging,
        cache=cache,
    )
