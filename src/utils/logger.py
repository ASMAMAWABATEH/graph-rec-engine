import logging
import os
from pathlib import Path
from typing import Optional


DEFAULT_FORMAT = "[%(levelname)s] %(asctime)s - %(name)s - %(message)s"
_CONFIGURED = False


def _resolve_level(level: str | int | None) -> int:
    if isinstance(level, int):
        return level
    candidate = (level or os.getenv("LOG_LEVEL") or "INFO").upper()
    return getattr(logging, candidate, logging.INFO)


def configure_logging(level: str | int | None = None, log_file: str | Path | None = None) -> None:
    global _CONFIGURED

    resolved_level = _resolve_level(level)
    if _CONFIGURED:
        root = logging.getLogger()
        root.setLevel(resolved_level)
        return

    handlers: list[logging.Handler] = [logging.StreamHandler()]
    target_log = log_file or os.getenv("LOG_FILE")
    if target_log:
        log_path = Path(target_log)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_path))

    logging.basicConfig(level=resolved_level, format=DEFAULT_FORMAT, handlers=handlers)
    _CONFIGURED = True


def get_logger(name: Optional[str] = None) -> logging.Logger:
    if not _CONFIGURED:
        configure_logging()
    return logging.getLogger(name)
