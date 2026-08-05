# Project Instructions

- Keep AI-generated changes in reviewed pull requests. Never push directly to `main`.
- Never print, commit, request, or modify credentials and runtime files under `data/`.
- Preserve exact scanner mathematics unless a focused regression test documents the change.
- Run `python tools/check_secrets.py` and `python -m unittest discover -s tests -v` before proposing a change.
- Treat Yahoo and market-list responses as untrusted data. Validate schemas, counts, symbols, and timestamps.
- Keep the Render deployment at one Gunicorn worker unless scan state is moved to durable shared storage.
