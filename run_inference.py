"""Compatibility wrapper for the canonical inference CLI at src.cli.inference."""

from src.cli.inference import *  # noqa: F401,F403
from src.cli.inference import main


if __name__ == "__main__":
    main()

