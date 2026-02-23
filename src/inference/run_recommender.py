import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.utils.logger import get_logger


logger = get_logger(__name__)


def main() -> None:
    logger.warning(
        "Deprecated entrypoint: use run_inference.py instead. Forwarding arguments for compatibility."
    )
    cmd = [sys.executable, "-m", "src.cli.inference", *sys.argv[1:]]
    result = subprocess.run(cmd, check=False)
    raise SystemExit(result.returncode)


if __name__ == "__main__":
    main()
