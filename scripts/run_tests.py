"""Use a new workspace-local temporary directory on every test run."""
from pathlib import Path
import subprocess
import sys
import uuid

root = Path(__file__).resolve().parents[1]
temp_root = root / ".test-tmp"
temp_root.mkdir(exist_ok=True)
raise SystemExit(subprocess.call([sys.executable, "-m", "pytest", "--basetemp",
                                 str(temp_root / uuid.uuid4().hex), *sys.argv[1:]], cwd=root))
