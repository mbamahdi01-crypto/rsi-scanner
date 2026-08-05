"""Fail CI when tracked files contain credentials or private keys."""

from pathlib import Path
import re
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
PATTERNS = {
    "GitHub classic token": re.compile(r"gh" + r"[pousr]_[A-Za-z0-9]{20,}"),
    "GitHub fine-grained token": re.compile(r"github" + r"_pat_[A-Za-z0-9_]{20,}"),
    "Render deploy hook": re.compile(r"api\.render\.com/deploy/[^\s]+\?key="),
    "Telegram bot token": re.compile(r"\b\d{7,12}:[A-Za-z0-9_-]{30,}\b"),
    "Private key": re.compile(r"-----BEGIN " + r"(?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
}
SENSITIVE_PATHS = {
    "data/secret.json",
    "data/config.json",
    "data/alerts.db",
    "alerts.db",
    ".env",
    ".env.local",
    ".env.production",
}


def tracked_files():
    try:
        output = subprocess.check_output(
            ["git", "ls-files", "-z"], cwd=ROOT, stderr=subprocess.DEVNULL
        )
        return [ROOT / p.decode("utf-8") for p in output.split(b"\0") if p]
    except (OSError, subprocess.CalledProcessError):
        skipped = {".git", ".venv", "__pycache__", "yf_cache"}
        return [p for p in ROOT.rglob("*") if p.is_file() and not skipped.intersection(p.parts)]


def main():
    failures = []
    for path in tracked_files():
        relative = path.relative_to(ROOT).as_posix()
        if relative in SENSITIVE_PATHS:
            failures.append(f"sensitive runtime file is tracked: {relative}")
            continue
        try:
            if path.stat().st_size > 2_000_000:
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        for label, pattern in PATTERNS.items():
            if pattern.search(text):
                failures.append(f"{label} found in {relative}")
    if failures:
        print("Secret check failed:")
        for failure in failures:
            print(f"- {failure}")
        return 1
    print("Secret check passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
