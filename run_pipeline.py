"""Compatibility wrapper for the canonical pipeline CLI at src.cli.pipeline."""

from src.cli.pipeline import *  # noqa: F401,F403
from src.cli.pipeline import main


if __name__ == "__main__":
    main()

